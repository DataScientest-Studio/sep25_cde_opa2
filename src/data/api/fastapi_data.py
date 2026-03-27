from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
import psycopg
import pandas as pd
from datetime import datetime
from typing import Optional, List

from src.config import DB_NAME, DB_BOT_USER, DB_BOT_PASSWORD, PG_HOST, PG_DB_PORT
from src.common.custom_logger import logger

app = FastAPI(
    title="DATA API",
    description="API pour récupérer les données depuis PostgreSQL",
    version="1.0.0"
)

def get_postgresql_connection():
    """Établit une connexion à PostgreSQL"""
    try:
        conn = psycopg.connect(
            dbname=DB_NAME,
            user=DB_BOT_USER,
            password=DB_BOT_PASSWORD,
            host=PG_HOST,
            port=PG_DB_PORT
        )
        return conn
    except Exception as e:
        logger.error(f"Erreur de connexion PostgreSQL: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur de connexion à la base de données: {str(e)}")

@app.get("/")
async def root():
    """Route d'accueil de l'API"""
    return {"message": "API DATA - Service de récupération des données PostgreSQL"}

@app.get("/klines", response_model=List[dict])
async def get_klines(
    limit: int = Query(default=100, ge=1, le=10000, description="Nombre maximum de klines à récupérer"),
    table_name: str = Query(default="klines", description="Nom de la table PostgreSQL"),
    start_date: Optional[str] = Query(default=None, description="Date de début (format: YYYY-MM-DD HH:MM:SS)"),
    end_date: Optional[str] = Query(default=None, description="Date de fin (format: YYYY-MM-DD HH:MM:SS)")
):
    """
    Récupère les N derniers klines de la table PostgreSQL
    
    :param limit: Nombre maximum de klines à récupérer (1-10000)
    :param table_name: Nom de la table PostgreSQL (par défaut: klines)
    :param start_date: Date de début optionnelle
    :param end_date: Date de fin optionnelle
    :return: Liste des klines au format JSON
    """
    try:
        conn = get_postgresql_connection()
        
        # Construction de la requête SQL
        base_query = f"""
            SELECT open_time, close_time, open_price, high_price, low_price, 
                   close_price, volume, quote_volume, trades_count,
                   taker_buy_base_volume, taker_buy_quote_volume
            FROM {table_name}
        """
        
        conditions = []
        params = []
        
        # Ajout des filtres de date si fournis
        if start_date:
            try:
                start_dt = datetime.fromisoformat(start_date)
                conditions.append("open_time >= %s")
                params.append(start_dt)
            except ValueError:
                raise HTTPException(status_code=400, detail="Format de start_date invalide. Utilisez: YYYY-MM-DD HH:MM:SS")
        
        if end_date:
            try:
                end_dt = datetime.fromisoformat(end_date)
                conditions.append("open_time <= %s")
                params.append(end_dt)
            except ValueError:
                raise HTTPException(status_code=400, detail="Format de end_date invalide. Utilisez: YYYY-MM-DD HH:MM:SS")
        
        if conditions:
            base_query += " WHERE " + " AND ".join(conditions)
        
        base_query += " ORDER BY open_time DESC LIMIT %s"
        params.append(limit)
        
        # Exécution de la requête
        df = pd.read_sql_query(base_query, conn, params=params)
        conn.close()
        
        if df.empty:
            logger.warning(f"Aucune donnée trouvée dans la table '{table_name}'")
            return []
        
        # Conversion des timestamps en format ISO
        if 'open_time' in df.columns:
            df['open_time'] = pd.to_datetime(df['open_time']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        if 'close_time' in df.columns:
            df['close_time'] = pd.to_datetime(df['close_time']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        # Conversion en dictionnaire pour JSON
        result = df.to_dict('records')
        
        logger.info(f"Récupération de {len(result)} klines depuis la table '{table_name}'")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des klines: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")

@app.get("/klines/latest", response_model=dict)
async def get_latest_kline(table_name: str = Query(default="klines", description="Nom de la table PostgreSQL")):
    """
    Récupère le dernier kline disponible
    
    :param table_name: Nom de la table PostgreSQL
    :return: Le dernier kline au format JSON
    """
    try:
        conn = get_postgresql_connection()
        
        query = f"""
            SELECT open_time, close_time, open_price, high_price, low_price, 
                   close_price, volume, quote_volume, trades_count,
                   taker_buy_base_volume, taker_buy_quote_volume
            FROM {table_name}
            ORDER BY open_time DESC LIMIT 1
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"Aucune donnée trouvée dans la table '{table_name}'")
        
        # Conversion des timestamps
        if 'open_time' in df.columns:
            df['open_time'] = pd.to_datetime(df['open_time']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        if 'close_time' in df.columns:
            df['close_time'] = pd.to_datetime(df['close_time']).dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        result = df.to_dict('records')[0]
        logger.info(f"Récupération du dernier kline depuis la table '{table_name}'")
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la récupération du dernier kline: {e}")
        raise HTTPException(status_code=500, detail=f"Erreur interne du serveur: {str(e)}")

@app.get("/health")
async def health_check():
    """Vérification de l'état de l'API et de la connexion à la base de données"""
    try:
        conn = get_postgresql_connection()
        conn.close()
        return {"status": "healthy", "database": "connected", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "database": "disconnected", "error": str(e), "timestamp": datetime.now().isoformat()}
        )