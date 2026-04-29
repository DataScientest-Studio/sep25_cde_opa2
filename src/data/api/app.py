from typing import List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from datetime import datetime
from psycopg.rows import dict_row

import pandas as pd

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger

from src.data.api.market import router as market_router
from src.data.api.scraping import router as scraping_router

app = FastAPI(
    title="DATA API",
    description="API pour récupérer les données depuis PostgreSQL",
    version="1.0.0"
)

app.include_router(market_router)
app.include_router(scraping_router)

@app.get("/")
async def root():
    """Route d'accueil de l'API"""
    return {
        "message": "API DATA - Service de récupération des données PostgreSQL",
        "endpoints": [
            "/market",
            "/market/candles",
            "/market/candles/latest",
            "/scraping",
            "/scraping/sentiment",
            "/health",
            "/symbols"]
    }


@app.get("/health")
async def health_check():
    """Vérification de l'état de l'API et de la connexion à la base de données"""
    try:
        pg_connector = PostgreSQLConnector().connect()
        pg_connector.close()
        return {"status": "healthy", "database": "connected", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "database": "disconnected", "error": str(e), "timestamp": datetime.now().isoformat()}
        )

@app.get("/symbols", response_model=List[dict])
def symbols():
    """Retourne la liste des crypto monnaies disponibles"""
    try:
        pg_connector = PostgreSQLConnector().connect()
        conn = pg_connector.conn

        query = """
            SELECT DISTINCT ON (s.symbol)
                s.id, s.symbol, s.base_asset, s.quote_asset
                FROM symbols s
                ORDER BY s.symbol, s.id
        """
        
        # Exécution de la requête
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query)
            result = cur.fetchall()

        if not result:
            raise HTTPException(status_code=404, detail="Aucune donnée trouvée dans la table 'symbols'")

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des symbols: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")
    finally:
        pg_connector.close()
