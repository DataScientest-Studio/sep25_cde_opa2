from fastapi import APIRouter, HTTPException, Query

import pandas as pd
from datetime import datetime
from typing import Optional

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger

router = APIRouter(prefix="/scraping", tags=["scraping"])

@router.get("/")
async def root():
    """Route d'accueil de l'API scraping"""
    return {
        "message": "Service de récupération des données liées au scraping",
        "endpoints": [
            "/sraping/sentiment",
        ]
    }

@router.get("/sentiment")
async def scraping_sentiment(
    limit: int = Query(default=100, ge=1, le=10000, description="Nombre maximum de sentiments à récupérer"),
    base_asset: Optional[str] = Query(default=None, description="Symbol crypto (format: BTC, ETH, ...)"),
    start_date: Optional[str] = Query(default=None, description="Date de début (format: YYYY-MM-DD HH:MM:SS)"),
    end_date: Optional[str] = Query(default=None, description="Date de fin (format: YYYY-MM-DD HH:MM:SS)")
):
    """
    Récupère les sentiments des articles scrapés pour une crypto.

    :param limit: Nombre maximum de sentiments à récupérer
    :base_asset limit: Symbol crypto optionnel (format: BTC, ETH, ...)
    :param start_date: Date de début optionnelle
    :param end_date: Date de fin optionnelle
    :return: Liste des sentiments au format JSON
    """
    try:
        pg_connector = PostgreSQLConnector().connect()
        conn = pg_connector.conn

        # Construction de la requête SQL
        base_query = f"""
            SELECT *
            FROM features_scraping_sentiment fss
        """

        conditions = []
        params = []

        # Ajout du symbol
        if base_asset:
            conditions.append("fss.base_asset = %s")
            params.append(base_asset)

        # Ajout des filtres de date si fournis
        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date)
                conditions.append("fss.published_at >= %s")
                params.append(start_dt)
            except ValueError:
                raise HTTPException(status_code=400, detail="Format de start_date invalide. Utilisez: YYYY-MM-DD HH:MM:SS")

        if end_date:
            try:
                end_dt = datetime.fromisoformat(end_date)
                conditions.append("fss.published_at < %s")
                params.append(end_dt)
            except ValueError:
                raise HTTPException(status_code=400, detail="Format de end_date invalide. Utilisez: YYYY-MM-DD HH:MM:SS")

        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)

        base_query += " ORDER BY fss.published_at DESC LIMIT %s"
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
            logger.warning("Aucune donnée trouvée dans la table 'features_scraping_sentiment'")
            return []

        # Conversion des timestamps en format ISO
        if 'published_at' in df.columns:
            df['published_at'] = pd.to_datetime(df['published_at']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at']).dt.strftime('%Y-%m-%dT%H:%M:%S')

        # Conversion en dictionnaire pour JSON
        result = df.to_dict('records')

        logger.info(f"Récupération de {len(result)} sentiments depuis la table 'features_scraping_sentiment'")
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des features_scraping_sentiment: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")    