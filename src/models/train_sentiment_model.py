"""
Pipeline de BENCHMARK supervisé hybride (Marché + Sentiments) pour données crypto.
Ce script compare les modèles sur plusieurs métriques SANS sauvegarder de fichier.

Étapes :
    1. Chargement et alignement des données (candles, features_candles, sentiments, labels)
    2. Entraînement de RandomForest, HistGradientBoosting et GradientBoosting
    3. Extraction et affichage d'un tableau comparatif multi-critères (Précision, Rappel, F1, etc.)
"""

import argparse
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance
from sklearn.metrics import classification_report
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier, GradientBoostingClassifier
from sklearn.utils.class_weight import compute_sample_weight

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger
from src.config import PROJECT_ROOT

# Dossier de sauvegarde des modèles
MODELS_DIR = PROJECT_ROOT / "models"
MODELS_DIR.mkdir(parents=True, exist_ok=True)

# Colonnes de features issues des candles brutes
CANDLE_COLS = ["open", "high", "low", "close", "volume"]

# Colonnes de features issues de features_candles
FEATURE_COLS = ["rsi_14", "macd", "macd_signal", "ema_20"]

# Colonnes de features issues des sentiments quotidiens
SENTIMENT_COLS = ["sentiment_score", "sentiment_smooth", "sentiment_weighted", "sentiment_weighted_smooth"]


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
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["id_candle", "open_time"] + CANDLE_COLS)
    for col in CANDLE_COLS:
        df[col] = df[col].astype(float)
    return df


def load_features(
    pg_conn, id_symbol: int, interval: str,
    train_from: str | None = None, train_until: str | None = None,
) -> pd.DataFrame:
    """Charge les features techniques depuis features_candles."""
    query = """
        SELECT id_candle, timestamp_candle, rsi_14, macd, macd_signal, ema_20
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
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["id_candle", "timestamp_candle"] + FEATURE_COLS)
    for col in FEATURE_COLS:
        df[col] = df[col].astype(float)
    return df


def load_sentiments_daily(
    pg_conn, base_asset: str,
    train_from: str | None = None, train_until: str | None = None,
) -> pd.DataFrame:
    """Charge les données de sentiments agrégées et lissées quotidiennement."""
    query = """
        SELECT date_dg, sentiment_score, sentiment_smooth, sentiment_weighted, sentiment_weighted_smooth
        FROM features_sentiment_daily
        WHERE base_asset = %s
    """
    params: list = [base_asset]
    if train_from:
        query += " AND date_dg >= %s"
        params.append(train_from)
    if train_until:
        query += " AND date_dg <= %s"
        params.append(train_until)
    query += " ORDER BY date_dg ASC"

    with pg_conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["date_dg"] + SENTIMENT_COLS)
    for col in SENTIMENT_COLS:
        df[col] = df[col].astype(float)
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
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["open_time", "label"])
    return df


# ---------------------------------------------------------------------------
# Préparation du dataset
# ---------------------------------------------------------------------------

def build_dataset(
    pg_conn, id_symbol: int, symbol: str, interval: str, horizon: int, threshold: float,
    train_from: str | None = None, train_until: str | None = None,
) -> tuple[pd.DataFrame, pd.Series]:
    """Assemble candles, features techniques, sentiments et labels."""
    base_asset = symbol.replace("USDT", "")

    df_candles = load_candles(pg_conn, id_symbol, interval, train_from, train_until)
    df_features = load_features(pg_conn, id_symbol, interval, train_from, train_until)
    df_sentiments = load_sentiments_daily(pg_conn, base_asset, train_from, train_until)
    df_labels = load_labels(pg_conn, id_symbol, interval, horizon, threshold, train_from, train_until)

    if df_candles.empty or df_features.empty or df_sentiments.empty or df_labels.empty:
        raise ValueError(f"Données insuffisantes pour créer l'alignement pour {symbol} | {interval}.")

    df_candles["open_time"] = pd.to_datetime(df_candles["open_time"])
    df_labels["open_time"] = pd.to_datetime(df_labels["open_time"])
    df_sentiments["open_time"] = pd.to_datetime(df_sentiments["date_dg"])

    # Alignement des indicateurs techniques sur les bougies
    df = df_candles.merge(df_features[["id_candle"] + FEATURE_COLS], on="id_candle", how="inner")
    # Alignement : Left merge pour conserver les jours sans actualités
    df = df.merge(df_sentiments[["open_time"] + SENTIMENT_COLS], on="open_time", how="left")
    # Propagation du sentiment précédent (ffill) pour combler le vide, puis 0 si le dataset commence par un trou
    df[SENTIMENT_COLS] = df[SENTIMENT_COLS].ffill().fillna(0.0)
    
    # Alignement final avec les labels
    df = df.merge(df_labels, on="open_time", how="inner")

    df = df.sort_values("open_time").reset_index(drop=True)
    all_feature_cols = CANDLE_COLS + FEATURE_COLS + SENTIMENT_COLS
    df = df.dropna(subset=all_feature_cols + ["label"]).reset_index(drop=True)

    X = df[all_feature_cols]
    y = df["label"].astype(int)

    logger.info(f"Dataset chargé : {len(X)} lignes. Distribution des classes : {y.value_counts().to_dict()}")
    return X, y


# -----------------------------------------------------------------------
# Prédictions custom avec utilisation de predict_proba au lieu de predict
# -----------------------------------------------------------------------
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

def train_and_evaluate(X: pd.DataFrame, y: pd.Series, symbol: str):
    # Découpage chronologique 80 / 20
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Training
    model = HistGradientBoostingClassifier(
        learning_rate=0.02, max_depth=6, max_iter=800,
        class_weight='balanced', random_state=42
    )
    model.fit(X_train_scaled, y_train)
    
    # Évaluation avec notre logique custom
    y_pred = predict_custom(model, X_test_scaled)
    
    # Rapport
    report = classification_report(y_test, y_pred, output_dict=True, zero_division=0)
    
    print(f"\n--- Performance {symbol} (Logique Métier Intégrée) ---")
    print(f"SELL Precision: {report.get('-1', {}).get('precision', 0):.4f}")
    print(f"BUY Precision: {report.get('1', {}).get('precision', 0):.4f}")
    print(f"HOLD Precision: {report.get('0', {}).get('precision', 0):.4f}")
    
    return model, scaler

def save_model(
    model, scaler, symbol: str, interval: str, horizon: int, threshold: float
) -> Path:
    """Sauvegarde le modèle et le scaler dans models/."""    
    model_name = f"hgb_{symbol}_{interval}_h{horizon}_t{threshold}.pkl"
    model_path = MODELS_DIR / model_name

    with open(model_path, "wb") as f:
        pickle.dump({"model": model, "scaler": scaler}, f)

    logger.info(f"Modèle sauvegardé avec succès : {model_path}")
    return model_path

def run(
    symbol: str, interval: str, horizon: int, threshold: float,
    train_from: str | None = None, train_until: str | None = None,
) -> None:
    """Exécute uniquement le chargement et le benchmark comparatif."""
    pg = PostgreSQLConnector().connect()

    try:
        id_symbol = get_symbol_id(pg.conn, symbol)
        if id_symbol is None:
            return

        X, y = build_dataset(
            pg.conn, id_symbol, symbol, interval, horizon, threshold,
            train_from=train_from, train_until=train_until,
        )
        
        model, scaler = train_and_evaluate(X, y, symbol)
        save_model(model, scaler, symbol, interval, horizon, threshold)        

    finally:
        pg.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark comparatif multi-modèles de marché.")
    parser.add_argument("--symbol", required=True, help="Ex: BTCUSDT")
    parser.add_argument("--interval", default="1d", help="Intervalle (Par défaut: 1d)")
    parser.add_argument("--horizon", type=int, required=True, help="Horizon de prédiction")
    parser.add_argument("--threshold", type=float, required=True, help="Seuil de rendement (ex: 0.02)")
    parser.add_argument("--train_from", type=str, default=None, help="Date de début (ex: 2025-01-01)")
    parser.add_argument("--train_until", type=str, default=None, help="Date de fin (ex: 2026-01-01)")
    args = parser.parse_args()

    run(
        symbol=args.symbol,
        interval=args.interval,
        horizon=args.horizon,
        threshold=args.threshold,
        train_from=args.train_from,
        train_until=args.train_until,
    )