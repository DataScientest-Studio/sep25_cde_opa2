from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

import pandas as pd
from datetime import datetime
from typing import Optional, List

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger

router = APIRouter(prefix="/market", tags=["market", "candles"])

@router.get("/")
async def root():
    """Route d'accueil de l'API market"""
    return {
        "message": "Service de récupération des données market",
        "endpoints": [
            "/market/candles",
            "/market/candles/latest"
        ]
    }

@router.get("/candles", response_model=List[dict])
async def get_candles(
    limit: int = Query(default=100, ge=1, le=10000, description="Nombre maximum de candles à récupérer"),
    start_date: Optional[str] = Query(default=None, description="Date de début (format: YYYY-MM-DD HH:MM:SS)"),
    end_date: Optional[str] = Query(default=None, description="Date de fin (format: YYYY-MM-DD HH:MM:SS)")
):
    """
    Récupère les N dernières candles de la table PostgreSQL

    :param limit: Nombre maximum de candles à récupérer (1-10000)
    :param start_date: Date de début optionnelle
    :param end_date: Date de fin optionnelle
    :return: Liste des candles au format JSON
    """
    try:
        pg_connector = PostgreSQLConnector().connect()
        conn = pg_connector.conn

        # Construction de la requête SQL
        base_query = """
            SELECT c.id, s.symbol, c.interval, c.open_time, c.close_time,
                   c.open, c.high, c.low, c.close, c.volume
            FROM candles c
            JOIN symbols s ON s.id = c.id_symbol
        """

        conditions = []
        params = []

        # Ajout des filtres de date si fournis
        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date)
                conditions.append("c.open_time >= %s")
                params.append(start_dt)
            except ValueError:
                raise HTTPException(status_code=400, detail="Format de start_date invalide. Utilisez: YYYY-MM-DD HH:MM:SS")

        if end_date:
            try:
                end_dt = datetime.fromisoformat(end_date)
                conditions.append("c.open_time <= %s")
                params.append(end_dt)
            except ValueError:
                raise HTTPException(status_code=400, detail="Format de end_date invalide. Utilisez: YYYY-MM-DD HH:MM:SS")

        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        base_query += " ORDER BY c.open_time DESC LIMIT %s"
        params.append(limit)

        # Exécution de la requête
        # df = pd.read_sql_query(base_query, conn, params=params)
        # Suppression du UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) 
        # or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested.
        # Please consider using SQLAlchemy.
        with conn.cursor() as cur:
            cur.execute(base_query, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        pg_connector.close()
        df = pd.DataFrame(rows, columns=columns)

        if df.empty:
            logger.warning("Aucune donnée trouvée dans la table 'candles'")
            return []

        # Conversion des timestamps en format ISO
        if 'open_time' in df.columns:
            df['open_time'] = pd.to_datetime(df['open_time']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        if 'close_time' in df.columns:
            df['close_time'] = pd.to_datetime(df['close_time']).dt.strftime('%Y-%m-%dT%H:%M:%S')

        # Conversion en dictionnaire pour JSON
        result = df.to_dict('records')

        logger.info(f"Récupération de {len(result)} candles depuis la table 'candles'")
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des candles: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")

@router.get("/candles/latest", response_model=dict)
async def get_latest_candle():
    """
    Récupère la dernière candle disponible

    :return: La dernière candle au format JSON
    """
    try:
        pg_connector = PostgreSQLConnector().connect()
        conn = pg_connector.conn

        query = """
            SELECT c.id, s.symbol, c.interval, c.open_time, c.close_time,
                   c.open, c.high, c.low, c.close, c.volume
            FROM candles c
            JOIN symbols s ON s.id = c.id_symbol
            ORDER BY c.open_time DESC LIMIT 1
        """
        
        # Exécution de la requête
        # df = pd.read_sql_query(base_query, conn, params=params)
        # Suppression du UserWarning: pandas only supports SQLAlchemy connectable (engine/connection) 
        # or database string URI or sqlite3 DBAPI2 connection. Other DBAPI2 objects are not tested.
        # Please consider using SQLAlchemy.
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
        pg_connector.close()
        df = pd.DataFrame(rows, columns=columns)

        if df.empty:
            raise HTTPException(status_code=404, detail="Aucune donnée trouvée dans la table 'candles'")

        # Conversion des timestamps
        if 'open_time' in df.columns:
            df['open_time'] = pd.to_datetime(df['open_time']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        if 'close_time' in df.columns:
            df['close_time'] = pd.to_datetime(df['close_time']).dt.strftime('%Y-%m-%dT%H:%M:%S')

        result = df.to_dict('records')[0]
        logger.info("Récupération de la dernière candle depuis la table 'candles'")
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de la dernière candle: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")
