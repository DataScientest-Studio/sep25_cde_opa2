"""
train_model.py

Pipeline d'entraînement supervisé pour les données de marché crypto.

Étapes :
    1. Chargement des données depuis PostgreSQL (candles, features_candles, labels)
    2. Fusion et préparation des features/labels
    3. Entraînement d'un RandomForestClassifier
    4. Évaluation et sauvegarde du modèle
"""

import argparse
import pickle
from pathlib import Path

import pandas as pd
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.utils.class_weight import compute_sample_weight

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger
from src.config import PROJECT_ROOT

# Dossier de sauvegarde des modèles
MODELS_DIR = PROJECT_ROOT / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# Colonnes de features issues de features_candles
FEATURE_COLS = ["rsi_14", "macd", "macd_signal", "ema_20", "ema_50", "ema_100"]

# Colonnes de features issues des candles brutes
CANDLE_COLS = ["open", "high", "low", "close", "volume"]


# ---------------------------------------------------------------------------
# Chargement des données
# ---------------------------------------------------------------------------

def get_symbol_id(pg_conn, symbol: str) -> int | None:
    """Récupère l'id du symbole dans la table symbols."""
    with pg_conn.cursor() as cur:
        cur.execute("SELECT id FROM symbols WHERE symbol = %s", (symbol,))
        row = cur.fetchone()
    if row is None:
        logger.error(f"Symbole '{symbol}' introuvable dans la table symbols.")
    return row[0] if row else None


def load_candles(
    pg_conn, id_symbol: int, interval: str,
    train_from: str | None = None, train_until: str | None = None,
) -> pd.DataFrame:
    """Charge les candles depuis PostgreSQL."""
    query = """
        SELECT id, open_time, open, high, low, close, volume
        FROM candles
        WHERE id_symbol = %s AND interval = %s
    """
    params: list = [id_symbol, interval]
    if train_from:
        query += " AND open_time >= %s"
        params.append(train_from)
    if train_until:
        query += " AND open_time <= %s"
        params.append(train_until)
    query += " ORDER BY open_time ASC"

    with pg_conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    if not rows:
        logger.warning("Aucune candle trouvée.")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["id_candle", "open_time"] + CANDLE_COLS)
    for col in CANDLE_COLS:
        df[col] = df[col].astype(float)
    logger.info(f"{len(df)} candles chargées.")
    return df


def load_features(
    pg_conn, id_symbol: int, interval: str,
    train_from: str | None = None, train_until: str | None = None,
) -> pd.DataFrame:
    """Charge les features techniques depuis features_candles."""
    query = """
        SELECT id_candle, timestamp_candle, rsi_14, macd, macd_signal,
               ema_20, ema_50, ema_100
        FROM features_candles
        WHERE id_symbol = %s AND interval = %s
    """
    params: list = [id_symbol, interval]
    if train_from:
        query += " AND timestamp_candle >= %s"
        params.append(train_from)
    if train_until:
        query += " AND timestamp_candle <= %s"
        params.append(train_until)
    query += " ORDER BY timestamp_candle ASC"

    with pg_conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    if not rows:
        logger.warning("Aucune feature trouvée.")
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["id_candle", "timestamp_candle"] + FEATURE_COLS)
    for col in FEATURE_COLS:
        df[col] = df[col].astype(float)
    logger.info(f"{len(df)} lignes de features chargées.")
    return df


def load_labels(
    pg_conn, id_symbol: int, interval: str, horizon: int, threshold: float,
    train_from: str | None = None, train_until: str | None = None,
) -> pd.DataFrame:
    """Charge les labels supervisés depuis la table labels."""
    query = """
        SELECT timestamp, label_up_down
        FROM labels
        WHERE id_symbol = %s AND interval = %s
          AND horizon = %s AND threshold = %s
    """
    params: list = [id_symbol, interval, horizon, threshold]
    if train_from:
        query += " AND timestamp >= %s"
        params.append(train_from)
    if train_until:
        query += " AND timestamp <= %s"
        params.append(train_until)
    query += " ORDER BY timestamp ASC"

    with pg_conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    if not rows:
        logger.warning(
            f"Aucun label trouvé pour {interval} | horizon={horizon} | threshold={threshold}."
        )
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["open_time", "label"])
    logger.info(f"{len(df)} labels chargés.")
    return df


# ---------------------------------------------------------------------------
# Préparation du dataset
# ---------------------------------------------------------------------------

def build_dataset(
    pg_conn, id_symbol: int, symbol: str, interval: str, horizon: int, threshold: float,
    train_from: str | None = None, train_until: str | None = None,
) -> tuple[pd.DataFrame, pd.Series]:
    """
    Assemble candles, features et labels en un DataFrame unique.

    Retourne (X, y) prêts pour l'entraînement.
    """
    logger.info(f"Période d'entraînement : {train_from} - {train_until}")
    df_candles = load_candles(pg_conn, id_symbol, interval, train_from, train_until)
    df_features = load_features(pg_conn, id_symbol, interval, train_from, train_until)
    df_labels = load_labels(pg_conn, id_symbol, interval, horizon, threshold, train_from, train_until)

    if df_candles.empty:
        logger.error("Impossible de construire le dataset : aucune candle disponible.")
    if df_features.empty:
        logger.error("Impossible de construire le dataset : aucune feature disponible.")
    if df_labels.empty:
        logger.error("Impossible de construire le dataset : aucun label disponible.")

    if df_candles.empty or df_features.empty or df_labels.empty:
        raise ValueError(
            f"Données insuffisantes pour {symbol} | {interval} | "
            f"horizon={horizon} | threshold={threshold}"
        )

    # Jointure candles ↔ features sur id_candle
    df = df_candles.merge(
        df_features[["id_candle"] + FEATURE_COLS],
        on="id_candle",
        how="inner",
    )

    # Jointure avec les labels sur open_time (= timestamp du label)
    df = df.merge(df_labels, on="open_time", how="inner")

    df = df.sort_values("open_time").reset_index(drop=True)

    all_feature_cols = (
        CANDLE_COLS
        + FEATURE_COLS
    )

    df = df.dropna(subset=all_feature_cols + ["label"]).reset_index(drop=True)

    X = df[all_feature_cols]
    y = df["label"].astype(int)

    logger.info(
        f"Dataset assemblé : {len(X)} échantillons, {X.shape[1]} features. "
        f"Distribution des labels : {y.value_counts().to_dict()}"
    )
    return X, y


# ---------------------------------------------------------------------------
# Entraînement et évaluation
# ---------------------------------------------------------------------------

def train_and_evaluate(X: pd.DataFrame, y: pd.Series) -> tuple:
    """
    Entraîne un RandomForestClassifier

    - Les 80% les plus anciens servent à l'entraînement du modèle final.
    - Les 20% les plus récents constituent le test final.

    Retourne (modèle entraîné sur les 80%, scaler).
    """
    # Découpage chronologique 80 / 20
    split_idx = int(len(X) * 0.8)
    X_train_full, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train_full, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    logger.info(
        f"Découpage 80/20 — train: {len(X_train_full)} échantillons "
        f"| test : {len(X_test)} échantillons"
    )

    # --- Entraînement final sur les 80% + évaluation sur 20% ---
    scaler_final = StandardScaler()
    X_train_scaled = scaler_final.fit_transform(X_train_full)
    
    weights = compute_sample_weight(
        class_weight="balanced",
        y=y_train_full
    )

    final_model = HistGradientBoostingClassifier(
        learning_rate=0.03,
        max_depth=6,
        max_iter=500,
        random_state=42
    )
    final_model.fit(X_train_scaled, y_train_full, sample_weight=weights)

    X_test_scaled = scaler_final.transform(X_test)
    y_test_pred = final_model.predict(X_test_scaled)
    test_report = classification_report(y_test, y_test_pred, zero_division=0)
    test_report_dict = classification_report(y_test, y_test_pred, output_dict=True, zero_division=0)
    logger.info(
        f"Évaluation (20% les plus récents) — "
        f"accuracy: {test_report_dict['accuracy']:.4f} | "
        f"f1 weighted: {test_report_dict['weighted avg']['f1-score']:.4f}\n{test_report}"
    )

    return final_model, scaler_final


# ---------------------------------------------------------------------------
# Sauvegarde du modèle
# ---------------------------------------------------------------------------

def save_model(
    model, scaler, symbol: str, interval: str, horizon: int, threshold: float
) -> Path:
    """Sauvegarde le modèle et le scaler dans models/."""
    model_name = f"rf_{symbol}_{interval}_h{horizon}_t{threshold}.pkl"
    model_path = MODELS_DIR / model_name

    with open(model_path, "wb") as f:
        pickle.dump({"model": model, "scaler": scaler}, f)

    logger.info(f"Modèle sauvegardé : {model_path}")
    return model_path


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------

def run(
    symbol: str, interval: str, horizon: int, threshold: float,
    train_from: str | None = None, train_until: str | None = None,
) -> None:
    """Exécute le pipeline complet d'entraînement."""
    pg = PostgreSQLConnector().connect()

    try:
        id_symbol = get_symbol_id(pg.conn, symbol)
        if id_symbol is None:
            return

        X, y = build_dataset(
            pg.conn, id_symbol, symbol, interval, horizon, threshold,
            train_from=train_from, train_until=train_until,
        )
        model, scaler = train_and_evaluate(X, y)
        save_model(model, scaler, symbol, interval, horizon, threshold)

    finally:
        pg.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entraîne un modèle de prédiction de marché.")
    parser.add_argument("--symbol", required=True, help="Ex: BTCUSDT")
    parser.add_argument("--interval", required=True, help="Ex: 1h, 5m, 1d")
    parser.add_argument("--horizon", type=int, required=True, help="Nombre de candles à l'avenir")
    parser.add_argument("--threshold", type=float, required=True, help="Seuil de rendement (ex: 0.02)")
    parser.add_argument("--train_from", type=str, default=None, help="Date de début (ex: 2024-01-01)")
    parser.add_argument("--train_until", type=str, default=None, help="Date de fin (ex: 2025-01-01)")
    args = parser.parse_args()

    run(
        symbol=args.symbol,
        interval=args.interval,
        horizon=args.horizon,
        threshold=args.threshold,
        train_from=args.train_from,
        train_until=args.train_until,
    )
