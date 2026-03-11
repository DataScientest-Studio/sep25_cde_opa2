import psycopg
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from decimal import Decimal
import time

from src.custom_logger import logger
from src.config import (
    DB_NAME, DB_BOT_USER, DB_BOT_PASSWORD, PG_DB_PORT, PG_HOST,
    MONGO_HOST, MONGO_DB_PORT
)


def connect_to_mongodb():
    """Connexion directe à MongoDB"""
    try:
        username = DB_BOT_USER
        password = DB_BOT_PASSWORD
        host = MONGO_HOST
        port = MONGO_DB_PORT
        db_name = DB_NAME
        
        if username and password and host and port and db_name:
            connection_string = f"mongodb://{username}:{password}@{host}:{port}/{db_name}"
        else:
            logger.error("Configuration MongoDB incomplète. Veuillez vérifier les variables d'environnement.")
            return None, None
        
        client = MongoClient(connection_string)
        db = client[db_name]
        
        # Test de la connexion
        client.admin.command('ping')
        logger.info(f"Connexion réussie à MongoDB: {host}:{port}")
        return client, db
        
    except PyMongoError as e:
        logger.error(f"Erreur de connexion à MongoDB: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Erreur lors de la création de la connexion MongoDB: {e}")
        return None, None


def connect_to_postgresql():
    """Connexion à PostgreSQL"""
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
        logger.error(f"Erreur de connexion à PostgreSQL: {e}")
        return None


def transform_kline_data(mongo_doc):
    """
    Transforme un document MongoDB kline vers le format PostgreSQL
    """
    return {
        'open_time': mongo_doc.get('open_time'),
        'close_time': mongo_doc.get('close_time'),
        'open_price': Decimal(str(mongo_doc.get('open_price', 0))),
        'high_price': Decimal(str(mongo_doc.get('high_price', 0))),
        'low_price': Decimal(str(mongo_doc.get('low_price', 0))),
        'close_price': Decimal(str(mongo_doc.get('close_price', 0))),
        'volume': Decimal(str(mongo_doc.get('volume', 0))) * 10, # Simulation d'une transformation (ex: conversion d'unités)
        'quote_volume': Decimal(str(mongo_doc.get('quote_volume', 0))) * 10, # Simulation d'une transformation (ex: conversion d'unités)
        'trades_count': int(mongo_doc.get('trades_count', 0)),
        'taker_buy_base_volume': Decimal(str(mongo_doc.get('taker_buy_base_volume', 0))),
        'taker_buy_quote_volume': Decimal(str(mongo_doc.get('taker_buy_quote_volume', 0))),
        'ignore': str(mongo_doc.get('ignore', '0'))
    }


def load_klines_batch(conn, klines_data):
    """
    Insert un batch de klines dans PostgreSQL avec mise à jour si existant
    """
    try:
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO klines (
                    open_time, close_time, open_price, high_price, low_price,
                    close_price, volume, quote_volume, trades_count,
                    taker_buy_base_volume, taker_buy_quote_volume, ignore
                ) VALUES (
                    %(open_time)s, %(close_time)s, %(open_price)s, %(high_price)s, %(low_price)s,
                    %(close_price)s, %(volume)s, %(quote_volume)s, %(trades_count)s,
                    %(taker_buy_base_volume)s, %(taker_buy_quote_volume)s, %(ignore)s
                ) ON CONFLICT (open_time, close_time) DO UPDATE SET
                    open_price = EXCLUDED.open_price,
                    high_price = EXCLUDED.high_price,
                    low_price = EXCLUDED.low_price,
                    close_price = EXCLUDED.close_price,
                    volume = EXCLUDED.volume,
                    quote_volume = EXCLUDED.quote_volume,
                    trades_count = EXCLUDED.trades_count,
                    taker_buy_base_volume = EXCLUDED.taker_buy_base_volume,
                    taker_buy_quote_volume = EXCLUDED.taker_buy_quote_volume,
                    ignore = EXCLUDED.ignore
            """
            
            cur.executemany(insert_query, klines_data)
            conn.commit()
            return cur.rowcount
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion/mise à jour: {e}")
        conn.rollback()
        return 0


def get_klines_from_mongodb(db, collection_name='klines_BTCUSDT_1m_ws', limit=None, sort_by='open_time'):
    """
    Récupère des données klines depuis MongoDB
    
    Args:
        db: Base de données MongoDB
        collection_name: Nom de la collection MongoDB
        limit: Nombre maximum de documents à récupérer (None = tous)
        sort_by: Champ pour trier les résultats
    
    Returns:
        List[Dict]: Liste des documents klines
    """
    try:
        collection = db[collection_name]
        
        query = collection.find({})
        if sort_by:
            query = query.sort(sort_by, 1)
        if limit:
            query = query.limit(limit)
        
        documents = list(query)
        logger.info(f"Récupéré {len(documents)} documents de la collection {collection_name}")
        return documents
        
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données: {e}")
        return []


def transform_and_load_klines_data(batch_size=1000, collection_name='klines_BTCUSDT_1m_ws'):
    """
    Transformation et chargement des données klines de MongoDB vers PostgreSQL
    """
    # Connexions
    mongo_client, mongo_db = connect_to_mongodb()
    if mongo_db is None:
        logger.error("Impossible de se connecter à MongoDB")
        return
    
    pg_conn = connect_to_postgresql()
    if not pg_conn:
        logger.error("Impossible de se connecter à PostgreSQL")
        return
    
    try:
        # Récupération des données MongoDB
        documents = get_klines_from_mongodb(mongo_db, collection_name=collection_name)
        total_docs = len(documents)
        logger.info(f"Nombre total de documents à traiter: {total_docs}")
        
        if total_docs == 0:
            logger.info("Aucune donnée à traiter")
            return
        
        # Traitement par batches
        processed = 0
        inserted = 0
               
        batch = []
        for doc in documents:
            try:
                transformed_data = transform_kline_data(doc)
                batch.append(transformed_data)
                
                if len(batch) >= batch_size:
                    # Insert du batch
                    rows_inserted = load_klines_batch(pg_conn, batch)
                    inserted += rows_inserted
                    processed += len(batch)
                    
                    logger.info(f"Traité: {processed}/{total_docs} - Inséré: {inserted}")
                    batch = []
                    
            except Exception as e:
                logger.error(f"Erreur lors du traitement du document {doc.get('_id')}: {e}")
                continue
        
        # Insert du dernier batch s'il reste des données
        if batch:
            rows_inserted = load_klines_batch(pg_conn, batch)
            inserted += rows_inserted
            processed += len(batch)
        
        logger.info(f"Traitement terminé. Traité: {processed}, Inséré: {inserted}")
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement: {e}")
    finally:
        # Fermeture des connexions
        if mongo_client:
            mongo_client.close()
        if pg_conn:
            pg_conn.close()


if __name__ == "__main__":
    delay_seconds = 10
    logger.info("Démarrage du processus de transformation et chargement en continu...")
    logger.info(f"Exécution toutes les {delay_seconds} secondes. Appuyez sur Ctrl+C pour arrêter.")
    
    try:
        while True:
            logger.info("Début du traitement des données klines...")
            start_time = time.time()
            
            transform_and_load_klines_data(collection_name='klines_BTCUSDT_1m_ws')
            
            end_time = time.time()
            duration = round(end_time - start_time, 2)
            logger.info(f"Traitement terminé en {duration} secondes.")
            
            logger.info(f"Attente de {delay_seconds} secondes avant le prochain traitement...")
            time.sleep(delay_seconds)
            
    except KeyboardInterrupt:
        logger.info("Arrêt du processus demandé par l'utilisateur (Ctrl+C)")
    except Exception as e:
        logger.error(f"Erreur inattendue dans la boucle principale: {e}")
    finally:
        logger.info("Processus de transformation et chargement arrêté.")