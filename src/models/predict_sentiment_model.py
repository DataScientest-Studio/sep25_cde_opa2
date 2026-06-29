"""
predict_model.py

Génère des prédictions basées sur la stratégie "Sniper SELL"
et les stocke en base de données.
"""

import argparse
import pickle
import numpy as np
import pandas as pd
from sklearn.metrics import classification_report

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger
from src.models.train_sentiment_model import (
    CANDLE_COLS,
    FEATURE_COLS,
    SENTIMENT_COLS,
    MODELS_DIR,
    get_symbol_id,
    load_candles,
    load_features,
    load_sentiments_daily,
)

# ---------------------------------------------------------------------------
# Logique de prédiction personnalisée
# ---------------------------------------------------------------------------

def predict_custom(model, X_scaled, proba_sell=0.50, proba_buy=0.80):
    """
    Logique unifiée :
    - Si proba achat > seuil : 1
    - Sinon si proba vente > seuil : -1
    - Sinon : 0
    """
    y_probs = model.predict_proba(X_scaled)
    classes = list(model.classes_)
    
    idx_sell = classes.index(-1) if -1 in classes else None
    idx_buy = classes.index(1) if 1 in classes else None
    
    y_pred_custom = []

    for probs in y_probs:
        # Priorité aux signaux d'achat si détectés
        if idx_buy is not None and probs[idx_buy] > proba_buy:
            y_pred_custom.append(1)
        # Sinon, priorité aux signaux de vente
        elif idx_sell is not None and probs[idx_sell] > proba_sell:
            y_pred_custom.append(-1)
        # Par défaut, neutre
        else:
            y_pred_custom.append(0)
            
    return np.array(y_pred_custom)

# ---------------------------------------------------------------------------
# Chargement du modèle
# ---------------------------------------------------------------------------

def load_model(symbol: str, interval: str, horizon: int, threshold: float) -> dict:
    """Charge le modèle et le scaler depuis le répertoire models/."""
    model_name = f"hgb_{symbol}_{interval}_h{horizon}_t{threshold}.pkl"
    model_path = MODELS_DIR / model_name

    if not model_path.exists():
        raise FileNotFoundError(
            f"Modèle introuvable : {model_path}. "
            "Lancez d'abord le script d'entraînement pour générer ce fichier."
        )

    with open(model_path, "rb") as f:
        bundle = pickle.load(f)

    logger.info(f"Modèle chargé : {model_path}")
    return bundle

# ---------------------------------------------------------------------------
# Préparation des données (Corrigée : fusion des colonnes complètes)
# ---------------------------------------------------------------------------

def build_prediction_dataset(pg_conn, id_symbol: int, symbol: str, interval: str, n_candles: int):
    """Adaptation de build_dataset pour la prédiction (sans labels)."""
    base_asset = symbol.replace("USDT", "")

    # 1. Chargement des données (On enlève les arguments train_from/until pour prendre tout)
    df_candles = load_candles(pg_conn, id_symbol, interval)
    df_features = load_features(pg_conn, id_symbol, interval)
    df_sentiments = load_sentiments_daily(pg_conn, base_asset)

    if df_candles.empty or df_features.empty:
        raise ValueError(f"Données insuffisantes pour la prédiction de {symbol}.")

    # 2. Alignement et formats (Exactement comme dans ton train_model)
    df_candles["open_time"] = pd.to_datetime(df_candles["open_time"])
    df_sentiments["open_time"] = pd.to_datetime(df_sentiments["date_dg"])

    # Merge Candles + Features
    df = df_candles.merge(df_features[["id_candle"] + FEATURE_COLS], on="id_candle", how="inner")

    # Merge Sentiments
    df = df.merge(df_sentiments[["open_time"] + SENTIMENT_COLS], on="open_time", how="left")

    # Propager les sentiments (ffill)
    df[SENTIMENT_COLS] = df[SENTIMENT_COLS].ffill().fillna(0.0)

    # 3. Finalisation (On garde la même logique de tri et colonnes)
    df = df.sort_values("open_time").reset_index(drop=True)
    
    all_feature_cols = CANDLE_COLS + FEATURE_COLS + SENTIMENT_COLS
    
    # Nettoyage des NaN sur les features uniquement
    df = df.dropna(subset=all_feature_cols).tail(n_candles).reset_index(drop=True)

    # On ne garde que les colonnes nécessaires au modèle
    cols_to_keep = all_feature_cols + ["open_time"]
    df = df[cols_to_keep]    
    X = df[cols_to_keep]
    
    logger.info(f"Dataset de prédiction prêt : {len(X)} lignes.")
    return X, all_feature_cols

# ---------------------------------------------------------------------------
# Sauvegarde
# ---------------------------------------------------------------------------

def save_predictions(pg_conn, id_symbol: int, interval: str, horizon: int, threshold: float, df: pd.DataFrame):
    records = [
        (id_symbol, interval, horizon, float(threshold), row["open_time"], int(row["predicted_up_down"]))
        for _, row in df.iterrows()
    ]

    with pg_conn.cursor() as cur:
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
    logger.info(f"{len(records)} prédictions insérées dans la table.")

# ---------------------------------------------------------------------------
# Exécution principale
# ---------------------------------------------------------------------------

def run(symbol: str, interval: str, horizon: int, threshold: float, n_candles: int = 100):
    bundle = load_model(symbol, interval, horizon, threshold)
    model = bundle["model"]
    scaler = bundle["scaler"]

    pg = PostgreSQLConnector().connect()
    try:
        id_symbol = get_symbol_id(pg.conn, symbol)
        if id_symbol is None:
            logger.error(f"Symbole introuvable : {symbol}")
            return

        # Construction du dataset avec les bonnes colonnes
        df, feature_cols = build_prediction_dataset(pg.conn, id_symbol, symbol, interval, n_candles)

        # Transformation via scaler (avec les bonnes colonnes)
        X_scaled = scaler.transform(df[feature_cols])
        
        # Prédiction personnalisée (Sniper SELL)
        predictions = predict_custom(model, X_scaled)

        df["predicted_up_down"] = predictions
        
        logger.info(f"Distribution : {pd.Series(predictions).value_counts().to_dict()}")

        save_predictions(pg.conn, id_symbol, interval, horizon, threshold, df[["open_time", "predicted_up_down"]])

    finally:
        pg.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--interval", required=True)
    parser.add_argument("--horizon", type=int, required=True)
    parser.add_argument("--threshold", type=float, required=True)
    parser.add_argument("--n_candles", type=int, default=100)
    args = parser.parse_args()

    try:
        logger.info(f"--- Démarrage de la prédiction pour {args.symbol} ---")
        run(
            symbol=args.symbol, 
            interval=args.interval, 
            horizon=args.horizon, 
            threshold=args.threshold, 
            n_candles=args.n_candles
        )
        logger.info("--- Travail terminé avec succès ---")
    except Exception as e:
        logger.error(f"--- Erreur critique lors de l'exécution : {e} ---")
        exit(1)