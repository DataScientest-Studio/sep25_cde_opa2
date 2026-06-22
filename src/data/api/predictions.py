from fastapi import APIRouter, HTTPException, Query

from typing import List

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger

router = APIRouter(prefix="/predictions", tags=["predictions"])


@router.get("/")
async def root():
    """Route d'accueil de l'API predictions"""
    return {
        "message": "Service de récupération des prédictions",
        "endpoints": [
            "/predictions/symbols",
            "/predictions/intervals",
            "/predictions/versions",
            "/predictions",
        ],
    }


@router.get("/symbols", response_model=List[str])
async def get_prediction_symbols():
    """Retourne la liste des symboles ayant des prédictions."""
    try:
        pg_connector = PostgreSQLConnector().connect()
        conn = pg_connector.conn

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT s.symbol
                FROM predictions p
                JOIN symbols s ON s.id = p.id_symbol
                ORDER BY s.symbol;
                """
            )
            rows = cur.fetchall()

        pg_connector.close()
        return [row[0] for row in rows]

    except Exception as e:
        logger.error(f"Erreur get_prediction_symbols: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")


@router.get("/intervals", response_model=List[str])
async def get_prediction_intervals(
    symbol: str = Query(..., description="Symbole de trading (ex: BTCUSDT)"),
):
    """Retourne les intervalles disponibles pour un symbole donné."""
    try:
        pg_connector = PostgreSQLConnector().connect()
        conn = pg_connector.conn

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT p.interval
                FROM predictions p
                JOIN symbols s ON s.id = p.id_symbol
                WHERE s.symbol = %s
                ORDER BY p.interval;
                """,
                (symbol,),
            )
            rows = cur.fetchall()

        pg_connector.close()
        return [row[0] for row in rows]

    except Exception as e:
        logger.error(f"Erreur get_prediction_intervals: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")


@router.get("/versions", response_model=List[dict])
async def get_prediction_versions(
    symbol: str = Query(..., description="Symbole de trading (ex: BTCUSDT)"),
    interval: str = Query(..., description="Intervalle (ex: 1h)"),
):
    """Retourne les combinaisons (horizon, threshold) disponibles pour un symbole/intervalle."""
    try:
        pg_connector = PostgreSQLConnector().connect()
        conn = pg_connector.conn

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT p.horizon, p.threshold
                FROM predictions p
                JOIN symbols s ON s.id = p.id_symbol
                WHERE s.symbol = %s AND p.interval = %s
                ORDER BY p.horizon, p.threshold;
                """,
                (symbol, interval),
            )
            rows = cur.fetchall()

        pg_connector.close()
        return [{"horizon": int(row[0]), "threshold": float(row[1])} for row in rows]

    except Exception as e:
        logger.error(f"Erreur get_prediction_versions: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")


@router.get("", response_model=List[dict])
async def get_predictions(
    symbol: str = Query(..., description="Symbole de trading (ex: BTCUSDT)"),
    interval: str = Query(..., description="Intervalle (ex: 1h)"),
    horizon: int = Query(..., description="Horizon (nombre de candles)"),
    threshold: float = Query(..., description="Seuil θ (ex: 0.02 pour 2%)"),
    limit: int = Query(default=1000, ge=1, le=10000, description="Nombre maximum de prédictions à récupérer"),
):
    """
    Retourne l'historique des prédictions avec le prix de clôture et le label réel associés.
    """
    try:
        pg_connector = PostgreSQLConnector().connect()
        conn = pg_connector.conn

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.timestamp,
                    p.predicted_up_down,
                    p.created_at,
                    c.close,
                    l.label_up_down
                FROM predictions p
                JOIN symbols s ON s.id = p.id_symbol
                LEFT JOIN candles c
                    ON c.id_symbol = p.id_symbol
                    AND c.interval = p.interval
                    AND c.open_time = p.timestamp
                LEFT JOIN labels l
                    ON l.id_symbol = p.id_symbol
                    AND l.interval = p.interval
                    AND l.horizon = p.horizon
                    AND l.threshold = p.threshold
                    AND l.timestamp = p.timestamp
                WHERE s.symbol = %s
                  AND p.interval = %s
                  AND p.horizon = %s
                  AND p.threshold = %s
                ORDER BY p.timestamp DESC
                LIMIT %s;
                """,
                (symbol, interval, horizon, threshold, limit),
            )
            rows = cur.fetchall()

        pg_connector.close()

        if not rows:
            return []

        result = []
        for row in rows:
            timestamp, predicted_up_down, created_at, close, label_up_down = row
            result.append({
                "timestamp": timestamp.isoformat() if timestamp else None,
                "predicted_up_down": int(predicted_up_down) if predicted_up_down is not None else None,
                "created_at": created_at.isoformat() if created_at else None,
                "close": float(close) if close is not None else None,
                "label_up_down": int(label_up_down) if label_up_down is not None else None,
            })

        logger.info(
            f"{len(result)} prédictions récupérées pour {symbol}/{interval} "
            f"horizon={horizon} threshold={threshold}"
        )
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur get_predictions: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")
