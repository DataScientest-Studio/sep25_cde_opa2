"""
predict_model.py

Génère des prédictions à partir d'un modèle entraîné et les stocke en base.

Étapes :
    1. Chargement du modèle et du scaler sauvegardés
    2. Chargement des dernières données (candles + features) depuis PostgreSQL
    3. Génération des prédictions
    4. Insertion des prédictions dans la table predictions
"""

import argparse
import pickle
import time
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger
from src.config import PROJECT_ROOT
from src.models.train_model import (
    CANDLE_COLS,
    FEATURE_COLS,
    MODELS_DIR,
    get_symbol_id,
    load_candles,
    load_features,
)

# ---------------------------------------------------------------------------
# Chargement du modèle
# ---------------------------------------------------------------------------

def load_model(symbol: str, interval: str, horizon: int, threshold: float) -> dict:
    """Charge le modèle et le scaler depuis le répertoire models/."""
    model_name = f"rf_{symbol}_{interval}_h{horizon}_t{threshold}.pkl"
    model_path = MODELS_DIR / model_name

    if not model_path.exists():
        raise FileNotFoundError(
            f"Modèle introuvable : {model_path}. "
            "Lancez d'abord src.models.train_model pour entraîner le modèle."
        )

    with open(model_path, "rb") as f:
        bundle = pickle.load(f)

    logger.info(f"Modèle chargé : {model_path}")
    return bundle


# ---------------------------------------------------------------------------
# Préparation des données pour la prédiction
# ---------------------------------------------------------------------------

def build_prediction_dataset(
    pg_conn, id_symbol: int, interval: str, n_candles: int
) -> pd.DataFrame:
    """
    Charge et assemble les n_candles les plus récentes avec leurs features.

    Retourne un DataFrame avec les mêmes colonnes que lors de l'entraînement.
    """
    df_candles = load_candles(pg_conn, id_symbol, interval)
    df_features = load_features(pg_conn, id_symbol, interval)

    if df_candles.empty or df_features.empty:
        raise ValueError("Données insuffisantes pour générer des prédictions.")

    df = df_candles.merge(
        df_features[["id_candle"] + FEATURE_COLS],
        on="id_candle",
        how="inner",
    )
    df = df.sort_values("open_time").reset_index(drop=True)

    all_feature_cols = (
        CANDLE_COLS
        + FEATURE_COLS
    )

    df = df.dropna(subset=all_feature_cols).reset_index(drop=True)

    # On ne garde que les n_candles les plus récentes
    df = df.tail(n_candles).reset_index(drop=True)

    logger.info(f"{len(df)} candles prêtes pour la prédiction.")
    return df, all_feature_cols


# ---------------------------------------------------------------------------
# Sauvegarde des prédictions en base
# ---------------------------------------------------------------------------

def save_predictions(
    pg_conn, id_symbol: int, interval: str, horizon: int, threshold: float,
    df: pd.DataFrame,
) -> None:
    """Insère ou met à jour les prédictions dans la table predictions."""
    records = [
        (id_symbol, interval, horizon, float(threshold), row["open_time"], int(row["predicted_up_down"]))
        for _, row in df.iterrows()
    ]

    with pg_conn.cursor() as cur:
        # Upsert batch : ON CONFLICT s'appuie sur UNIQUE(id_symbol, interval, horizon, threshold, timestamp)
        cur.executemany(
            """
            INSERT INTO predictions (id_symbol, interval, horizon, threshold, timestamp, predicted_up_down)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id_symbol, interval, horizon, threshold, timestamp)
            DO UPDATE SET predicted_up_down = EXCLUDED.predicted_up_down;
            """,
            records,
        )

    pg_conn.commit()
    logger.info(f"{len(records)} prédictions insérées/mises à jour dans la table predictions.")


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def run(
    symbol: str, interval: str, horizon: int, threshold: float, n_candles: int = 100
) -> pd.DataFrame:
    """
    Génère des prédictions et les stocke en base.

    Retourne le DataFrame des prédictions (open_time, predicted_value, predicted_label).
    """
    bundle = load_model(symbol, interval, horizon, threshold)
    model = bundle["model"]
    scaler = bundle["scaler"]

    pg = PostgreSQLConnector().connect()

    try:
        id_symbol = get_symbol_id(pg.conn, symbol)
        if id_symbol is None:
            return pd.DataFrame()

        df, feature_cols = build_prediction_dataset(pg.conn, id_symbol, interval, n_candles)

        X = scaler.transform(df[feature_cols])
        predictions = model.predict(X)

        df["predicted_up_down"] = predictions.astype(int)  # -1 SELL | 0 HOLD | 1 BUY

        logger.info(
            f"Prédictions générées — distribution : "
            f"{pd.Series(predictions).value_counts().to_dict()}"
        )

        save_predictions(pg.conn, id_symbol, interval, horizon, threshold, df[["open_time", "predicted_up_down"]])

    finally:
        pg.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Génère des prédictions de marché.")
    parser.add_argument("--symbol", required=True, help="Ex: BTCUSDT")
    parser.add_argument("--interval", required=True, help="Ex: 1h, 5m, 1d")
    parser.add_argument("--horizon", type=int, required=True, help="Horizon utilisé à l'entraînement")
    parser.add_argument("--threshold", type=float, required=True, help="Threshold utilisé à l'entraînement")
    parser.add_argument(
        "--n_candles", type=int, default=100,
        help="Nombre de candles récentes sur lesquelles prédire (défaut: 100)"
    )
    parser.add_argument("--loop", action="store_true", help="Exécuter en boucle toutes les 60 secondes (mode production)")
    args = parser.parse_args()

    if args.loop:
        delay_seconds = 60
        logger.info(
            f"Démarrage des prédictions pour {args.symbol} ({args.interval}), "
            f"horizon={args.horizon}, seuil={args.threshold}..."
        )
        logger.info(f"Exécution toutes les {delay_seconds} secondes.")
        try:
            while True:
                logger.info("Début du calcul des prédictions...")
                start_time = time.time()
                run(
                    symbol=args.symbol,
                    interval=args.interval,
                    horizon=args.horizon,
                    threshold=args.threshold,
                    n_candles=args.n_candles,
                )
                duration = round(time.time() - start_time, 2)
                logger.info(f"Prédictions terminées en {duration} secondes.")
                logger.info(f"Attente de {delay_seconds} secondes...")
                time.sleep(delay_seconds)
        except KeyboardInterrupt:
            logger.info("Arrêt demandé par l'utilisateur.")
        except Exception as e:
            logger.error(f"Erreur inattendue dans la boucle principale: {e}")
        finally:
            logger.info("Processus de prédiction arrêté.")
    else:
        result = run(
            symbol=args.symbol,
            interval=args.interval,
            horizon=args.horizon,
            threshold=args.threshold,
            n_candles=args.n_candles,
        )

        if not result.empty:
            print(result.to_string(index=False))
