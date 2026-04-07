from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from datetime import datetime

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger

from src.data.api.market import router as market_router

app = FastAPI(
    title="DATA API",
    description="API pour récupérer les données depuis PostgreSQL",
    version="1.0.0"
)

app.include_router(market_router)

@app.get("/")
async def root():
    """Route d'accueil de l'API"""
    return {
        "message": "API DATA - Service de récupération des données PostgreSQL",
        "endpoints": [
            "/market",
            "/market/candles",
            "/market/candles/latest"
            "/health"]
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