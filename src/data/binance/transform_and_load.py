import psycopg
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from decimal import Decimal
import time

from src.common.custom_logger import logger
from src.config import (
    DB_NAME, DB_BOT_USER, DB_BOT_PASSWORD, PG_DB_PORT, PG_HOST,
    MONGO_HOST, MONGO_DB_PORT
)

# Correspondance symbol -> id_symbol dans PostgreSQL
# BTCUSDT doit exister dans la table symbols
SYMBOL_NAME = 'BTCUSDT'
INTERVAL = '1m'


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


def get_or_create_symbol(conn, symbol_name):
    """
    Récupère l'id du symbol dans la table symbols, le crée s'il n'existe pas.
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM symbols WHERE symbol = %s;", (symbol_name,))
            row = cur.fetchone()
            if row:
                return row[0]
            # Création du symbol s'il n'existe pas
            cur.execute(
                "INSERT INTO symbols (symbol) VALUES (%s) RETURNING id;",
                (symbol_name,)
            )
            conn.commit()
            new_id = cur.fetchone()[0]
            logger.info(f"Symbol '{symbol_name}' créé avec id={new_id}")
            return new_id
    except Exception as e:
        logger.error(f"Erreur lors de la récupération/création du symbol: {e}")
        conn.rollback()
        return None


def transform_kline_data(mongo_doc, id_symbol, interval):
    """
    Transforme un document MongoDB kline vers le format de la table candles PostgreSQL.
    Mapping :
        open_time  -> open_time
        open_price -> open
        high_price -> high
        low_price  -> low
        close_price-> close
        volume     -> volume  (x10 : simulation de transformation)
    """
    return {
        'id_symbol': id_symbol,
        'interval': interval,
        'open_time': mongo_doc.get('open_time'),
        'open': Decimal(str(mongo_doc.get('open_price', 0))),
        'high': Decimal(str(mongo_doc.get('high_price', 0))),
        'low': Decimal(str(mongo_doc.get('low_price', 0))),
        'close': Decimal(str(mongo_doc.get('close_price', 0))),
        'volume': Decimal(str(mongo_doc.get('volume', 0))) * 10,  # Simulation d'une transformation
    }


def load_candles_batch(conn, candles_data):
    """
    Insère un batch de candles dans PostgreSQL avec mise à jour si existant.
    Le UNIQUE est sur (id_symbol, interval, open_time).
    """
    try:
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO candles (
                    id_symbol, interval, open_time,
                    open, high, low, close, volume
                ) VALUES (
                    %(id_symbol)s, %(interval)s, %(open_time)s,
                    %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s
                ) ON CONFLICT (id_symbol, interval, open_time) DO UPDATE SET
                    open   = EXCLUDED.open,
                    high   = EXCLUDED.high,
                    low    = EXCLUDED.low,
                    close  = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """
            cur.executemany(insert_query, candles_data)
            conn.commit()
            return cur.rowcount
    except Exception as e:
        logger.error(f"Erreur lors de l'insertion/mise à jour: {e}")
        conn.rollback()
        return 0


def get_klines_from_mongodb(db, collection_name='klines_BTCUSDT_1m_ws', limit=None, sort_by='open_time'):
    """
    Récupère des données klines depuis MongoDB.
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
    Transformation et chargement des données klines de MongoDB vers PostgreSQL (table candles).
    """
    mongo_client, mongo_db = connect_to_mongodb()
    if mongo_db is None:
        logger.error("Impossible de se connecter à MongoDB")
        return

    pg_conn = connect_to_postgresql()
    if not pg_conn:
        logger.error("Impossible de se connecter à PostgreSQL")
        return

    try:
        # Récupération ou création du symbol
        id_symbol = get_or_create_symbol(pg_conn, SYMBOL_NAME)
        if id_symbol is None:
            logger.error(f"Impossible de récupérer/créer le symbol '{SYMBOL_NAME}'")
            return

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
                transformed_data = transform_kline_data(doc, id_symbol, INTERVAL)
                batch.append(transformed_data)

                if len(batch) >= batch_size:
                    rows_inserted = load_candles_batch(pg_conn, batch)
                    inserted += rows_inserted
                    processed += len(batch)
                    logger.info(f"Traité: {processed}/{total_docs} - Inséré: {inserted}")
                    batch = []

            except Exception as e:
                logger.error(f"Erreur lors du traitement du document {doc.get('_id')}: {e}")
                continue

        # Dernier batch
        if batch:
            rows_inserted = load_candles_batch(pg_conn, batch)
            inserted += rows_inserted
            processed += len(batch)

        logger.info(f"Traitement terminé. Traité: {processed}, Inséré: {inserted}")

    except Exception as e:
        logger.error(f"Erreur lors du traitement: {e}")
    finally:
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