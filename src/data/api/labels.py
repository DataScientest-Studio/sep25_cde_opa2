from fastapi import APIRouter, HTTPException, Query

from datetime import datetime
from typing import Optional, List

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger

router = APIRouter(prefix="/labels", tags=["labels"])


@router.get("/")
async def root():
    """Route d'accueil de l'API labels"""
    return {
        "message": "Service de récupération des labels",
        "endpoints": [
            "/labels/params",
            "/labels",
        ],
    }


@router.get("/params", response_model=List[dict])
async def get_label_params(
    symbol: str = Query(..., description="Symbole de trading (ex: BTCUSDT)"),
    interval: str = Query(..., description="Intervalle (ex: 1h, 1d, 5m, 1m)"),
):
    """
    Retourne les combinaisons (horizon, threshold) disponibles pour ce symbole/intervalle.
    """
    try:
        pg_connector = PostgreSQLConnector().connect()
        conn = pg_connector.conn

        query = """
            SELECT DISTINCT l.horizon, l.threshold
            FROM labels l
            JOIN symbols s ON s.id = l.id_symbol
            WHERE s.symbol = %s AND l.interval = %s
            ORDER BY l.horizon, l.threshold;
        """
        with conn.cursor() as cur:
            cur.execute(query, (symbol, interval))
            rows = cur.fetchall()

        pg_connector.close()
        return [{"horizon": int(row[0]), "threshold": float(row[1])} for row in rows]

    except Exception as e:
        logger.error(f"Erreur get_label_params: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")


@router.get("", response_model=List[dict])
async def get_labels(
    symbol: str = Query(..., description="Symbole de trading (ex: BTCUSDT)"),
    interval: str = Query(..., description="Intervalle (ex: 1h, 1d, 5m, 1m)"),
    horizon: int = Query(..., description="Horizon (nombre de candles)"),
    threshold: float = Query(..., description="Seuil θ (ex: 0.02 pour 2%)"),
    limit: int = Query(default=2000, ge=1, le=10000, description="Nombre maximum de labels à récupérer"),
    start_date: Optional[str] = Query(default=None, description="Date de début (format: YYYY-MM-DD HH:MM:SS)"),
    end_date: Optional[str] = Query(default=None, description="Date de fin (format: YYYY-MM-DD HH:MM:SS)"),
):
    """
    Retourne les labels pour les paramètres donnés, avec le prix de clôture associé.
    """
    try:
        pg_connector = PostgreSQLConnector().connect()
        conn = pg_connector.conn

        query = """
            SELECT l.timestamp, l.label_up_down, l.label_return, c.close
            FROM labels l
            JOIN symbols s ON s.id = l.id_symbol
            LEFT JOIN candles c ON c.id_symbol = l.id_symbol
                AND c.open_time = l.timestamp
                AND c.interval = l.interval
            WHERE s.symbol = %s AND l.interval = %s
              AND l.horizon = %s AND l.threshold = %s
        """
        params: list = [symbol, interval, horizon, threshold]

        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date)
                query += " AND l.timestamp >= %s"
                params.append(start_dt)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Format de start_date invalide. Utilisez: YYYY-MM-DD HH:MM:SS",
                )

        if end_date:
            try:
                end_dt = datetime.fromisoformat(end_date)
                query += " AND l.timestamp <= %s"
                params.append(end_dt)
            except ValueError:
                raise HTTPException(
                    status_code=400,
                    detail="Format de end_date invalide. Utilisez: YYYY-MM-DD HH:MM:SS",
                )

        query += " ORDER BY l.timestamp DESC LIMIT %s"
        params.append(limit)

        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        pg_connector.close()

        if not rows:
            return []

        result = []
        for row in rows:
            timestamp, label_up_down, label_return, close = row
            result.append({
                "timestamp": timestamp.isoformat() if timestamp else None,
                "label_up_down": int(label_up_down) if label_up_down is not None else None,
                "label_return": float(label_return) if label_return is not None else None,
                "close": float(close) if close is not None else None,
            })

        logger.info(f"{len(result)} labels récupérés pour {symbol}/{interval} horizon={horizon} threshold={threshold}")
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur get_labels: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")
