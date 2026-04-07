from decimal import Decimal
import time

from src.common.custom_logger import logger
from src.common.connectors import MongoConnector, PostgreSQLConnector


def load_symbols_from_mongo(mongo_db, pg_conn):
    """
    Charge les symboles depuis la collection MongoDB exchange_info_symbols
    vers la table symbols de PostgreSQL.
    Retourne un dict {symbol_name: id} pour les lookups ultérieurs.
    """
    try:
        collection = mongo_db['exchange_info_symbols']
        documents = list(collection.find({}))
        logger.info(f"Récupéré {len(documents)} symboles depuis MongoDB exchange_info_symbols")
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des symboles MongoDB: {e}")
        return {}

    symbol_map = {}
    try:
        with pg_conn.cursor() as cur:
            for doc in documents:
                symbol = doc.get('symbol')
                base_asset = doc.get('baseAsset')
                quote_asset = doc.get('quoteAsset')
                if not symbol:
                    continue
                cur.execute(
                    """
                    INSERT INTO symbols (symbol, base_asset, quote_asset)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        base_asset = EXCLUDED.base_asset,
                        quote_asset = EXCLUDED.quote_asset
                    RETURNING id
                    """,
                    (symbol, base_asset, quote_asset)
                )
                row = cur.fetchone()
                if row:
                    symbol_map[symbol] = row[0]
            pg_conn.commit()
        logger.info(f"{len(symbol_map)} symboles chargés/mis à jour dans PostgreSQL")
    except Exception as e:
        logger.error(f"Erreur lors du chargement des symboles dans PostgreSQL: {e}")
        pg_conn.rollback()

    return symbol_map


def get_symbol_id(pg_conn, symbol_name):
    """Retourne l'id du symbole dans la table symbols, ou None s'il n'existe pas."""
    try:
        with pg_conn.cursor() as cur:
            cur.execute("SELECT id FROM symbols WHERE symbol = %s", (symbol_name,))
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.error(f"Erreur lors de la récupération de l'id du symbole {symbol_name}: {e}")
        return None


def transform_kline_data(mongo_doc, id_symbol, interval):
    """
    Transforme un document MongoDB kline vers le format PostgreSQL (table candles)
    """
    return {
        'id_symbol': id_symbol,
        'interval': interval,
        'open_time': mongo_doc.get('open_time'),
        'close_time': mongo_doc.get('close_time'),
        'open': Decimal(str(mongo_doc.get('open_price', 0))),
        'high': Decimal(str(mongo_doc.get('high_price', 0))),
        'low': Decimal(str(mongo_doc.get('low_price', 0))),
        'close': Decimal(str(mongo_doc.get('close_price', 0))),
        'volume': Decimal(str(mongo_doc.get('volume', 0))),
    }


def load_candles_batch(conn, candles_data):
    """
    Insert un batch de candles dans PostgreSQL avec mise à jour si existant
    """
    try:
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO candles (
                    id_symbol, interval, open_time, close_time,
                    open, high, low, close, volume
                ) VALUES (
                    %(id_symbol)s, %(interval)s, %(open_time)s, %(close_time)s,
                    %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s
                ) ON CONFLICT (id_symbol, interval, open_time) DO UPDATE SET
                    close_time = EXCLUDED.close_time,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
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


def _parse_symbol_interval(collection_name):
    """
    Extrait le symbol et l'interval depuis un nom de collection du type
    klines_{SYMBOL}_{INTERVAL} ou klines_{SYMBOL}_{INTERVAL}_ws.
    Retourne (symbol, interval) ou (None, None) en cas d'échec.
    """
    name = collection_name
    if name.startswith('klines_'):
        name = name[len('klines_'):]
    if name.endswith('_ws'):
        name = name[:-3]
    parts = name.rsplit('_', 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, None


def transform_and_load_klines_data(batch_size=1000, collection_name='klines_BTCUSDT_1m_ws', symbol_map=None):
    """
    Transformation et chargement des données klines de MongoDB vers la table
    candles de PostgreSQL.
    """
    # Connexions
    mongo = MongoConnector().connect()
    pg_connector = PostgreSQLConnector().connect()
    pg_conn = pg_connector.conn

    try:
        # Chargement des symboles uniquement si pas déjà fourni
        if symbol_map is None:
            symbol_map = load_symbols_from_mongo(mongo.db, pg_conn)

        # Extraction du symbol et de l'interval depuis le nom de collection
        symbol_name, interval = _parse_symbol_interval(collection_name)
        if not symbol_name or not interval:
            logger.error(f"Impossible de parser le symbol/interval depuis '{collection_name}'")
            return

        id_symbol = symbol_map.get(symbol_name) or get_symbol_id(pg_conn, symbol_name)
        if id_symbol is None:
            logger.error(f"Le symbole '{symbol_name}' est introuvable dans la table symbols")
            return

        logger.info(f"Chargement des candles pour symbol={symbol_name} (id={id_symbol}), interval={interval}")

        # Récupération des données MongoDB
        documents = get_klines_from_mongodb(mongo.db, collection_name=collection_name)
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
                transformed_data = transform_kline_data(doc, id_symbol, interval)
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

        # Insert du dernier batch s'il reste des données
        if batch:
            rows_inserted = load_candles_batch(pg_conn, batch)
            inserted += rows_inserted
            processed += len(batch)

        logger.info(f"Traitement terminé. Traité: {processed}, Inséré: {inserted}")
        
    except Exception as e:
        logger.error(f"Erreur lors du traitement: {e}")
    finally:
        # Fermeture des connexions
        mongo.close()
        pg_connector.close()


def init_symbol_map():
    """
    Charge la table symbols depuis MongoDB (exchange_info_symbols) vers PostgreSQL
    et retourne le dict {symbol_name: id}. À appeler une seule fois avant la boucle principale.
    """
    mongo = MongoConnector().connect()
    pg_connector = PostgreSQLConnector().connect()
    pg_conn = pg_connector.conn
    symbol_map = {}
    try:
        symbol_map = load_symbols_from_mongo(mongo.db, pg_conn)
    finally:
        if mongo:
            mongo.close()
        pg_connector.close()
    return symbol_map


if __name__ == "__main__":
    delay_seconds = 10
    logger.info("Démarrage du processus de transformation et chargement en continu...")
    logger.info(f"Exécution toutes les {delay_seconds} secondes. Appuyez sur Ctrl+C pour arrêter.")

    # Chargement des symboles une seule fois avant la boucle
    symbol_map = init_symbol_map()

    try:
        while True:
            logger.info("Début du traitement des données klines...")
            start_time = time.time()

            transform_and_load_klines_data(collection_name='klines_BTCUSDT_1m_ws', symbol_map=symbol_map)

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