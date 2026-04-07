import psycopg

from src.config import DB_NAME, DB_ROOT_USER, DB_ROOT_PASSWORD, DB_BOT_USER, DB_BOT_PASSWORD, PG_DB_PORT, PG_HOST
from src.common.custom_logger import logger

CONN_INFO=dict(
    dbname='postgres',
    user=DB_ROOT_USER,
    password=DB_ROOT_PASSWORD,
    host=PG_HOST,
    port=PG_DB_PORT
)

# Création de la BDD et du user
with psycopg.connect(**CONN_INFO) as conn:
    
    conn.autocommit = True # Nécessaire pour la commande CREATE DATABASE
    with conn.cursor() as cur:

        # Creation de la base de données DB_NAME si elle n'existe pas déjà
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}';")
        existing_db=cur.fetchone()
        if not existing_db:
            cur.execute(f"CREATE DATABASE {DB_NAME};")
            logger.info(f"La base de données '{DB_NAME}' est créée.")
        else:
            logger.info(f"La base de données '{DB_NAME}' existe déjà.")

        # Creation du user DB_BOT_USER pour manipulation de DB_NAME
        cur.execute(f"SELECT 1 FROM pg_roles WHERE rolname='{DB_BOT_USER}';")
        existing_user=cur.fetchone()
        if not existing_user:
            # Creation du user DB_BOT_USER
            cur.execute(f"CREATE USER {DB_BOT_USER} WITH PASSWORD '{DB_BOT_PASSWORD}';")
            logger.info(f"L'utilisateur {DB_BOT_USER} créé.")
        else:
            logger.info(f"L'utilisateur {DB_BOT_USER} existe déjà.")

CRYPTOBOT_CONN_INFO=dict(
    dbname=DB_NAME,
    user=DB_ROOT_USER,
    password=DB_ROOT_PASSWORD,
    host=PG_HOST,
    port=PG_DB_PORT
)

# Attribution des droits au user
with psycopg.connect(**CRYPTOBOT_CONN_INFO) as conn:

    conn.autocommit = True
    with conn.cursor() as cur:

        # Attribue les droits au user DB_BOT_USER
        cur.execute(f"""
            GRANT CONNECT ON DATABASE {DB_NAME} TO {DB_BOT_USER};
            GRANT USAGE, CREATE ON SCHEMA public TO {DB_BOT_USER};
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO {DB_BOT_USER};
            GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO {DB_BOT_USER};
            GRANT ALL ON ALL FUNCTIONS IN SCHEMA public TO {DB_BOT_USER};
        """)

        # Donne les droits au DB_BOT_USER sur les tables éventuellement créées après le script d'init
        cur.execute(f"""
            ALTER DEFAULT PRIVILEGES IN SCHEMA public
            GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {DB_BOT_USER};

            ALTER DEFAULT PRIVILEGES IN SCHEMA public
            GRANT ALL ON SEQUENCES TO {DB_BOT_USER};

            ALTER DEFAULT PRIVILEGES IN SCHEMA public
            GRANT ALL ON FUNCTIONS TO {DB_BOT_USER};
        """)

        logger.info(f"Permissions accordées au user {DB_BOT_USER} sur la base {DB_NAME}")


# Création de la table klines si elle n'existe pas
with psycopg.connect(**CRYPTOBOT_CONN_INFO) as conn:

    conn.autocommit = True
    with conn.cursor() as cur:

        # Création des tables
        cur.execute("""
            CREATE TABLE IF NOT EXISTS symbols (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL UNIQUE,
                base_asset VARCHAR(20),
                quote_asset VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS candles (
                id SERIAL PRIMARY KEY,
                id_symbol INT NOT NULL,
                interval VARCHAR(10) NOT NULL,
                open_time TIMESTAMP NOT NULL,
                close_time TIMESTAMP NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                volume NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_symbol) REFERENCES symbols(id),
                UNIQUE(id_symbol, interval, open_time)
            );

            CREATE TABLE IF NOT EXISTS features_candles (
                id SERIAL PRIMARY KEY,
                id_symbol INT NOT NULL,
                id_candle INT,
                timestamp_candle TIMESTAMP,
                rsi_14 NUMERIC,
                macd NUMERIC,
                ema_20 NUMERIC,
                ema_50 NUMERIC,
                ema_100 NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_symbol) REFERENCES symbols(id),
                FOREIGN KEY (id_candle) REFERENCES candles(id)
            );

            CREATE TABLE IF NOT EXISTS features_orderbook (
                id SERIAL PRIMARY KEY,
                id_symbol INT NOT NULL,
                snapshot_time TIMESTAMP NOT NULL,
                best_bid_price NUMERIC,
                best_ask_price NUMERIC,
                spread NUMERIC,
                mid_price NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_symbol) REFERENCES symbols(id)
            );

            CREATE TABLE IF NOT EXISTS features_ticker_24 (
                id SERIAL PRIMARY KEY,
                id_symbol INT NOT NULL,
                snapshot_time TIMESTAMP NOT NULL,
                last_price NUMERIC,
                high_price NUMERIC,
                low_price NUMERIC,
                volume_24h NUMERIC,
                price_change_24h NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_symbol) REFERENCES symbols(id)
            );

            CREATE TABLE IF NOT EXISTS labels (
                id SERIAL PRIMARY KEY,
                id_symbol INT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                label_up_down INT,
                label_return NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_symbol) REFERENCES symbols(id)
            );

            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                id_symbol INT NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                predicted_value NUMERIC,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (id_symbol) REFERENCES symbols(id)
            );
                    
        """)
        
        # Accorder les permissions sur toutes les tables au user bot
        cur.execute(f"""
            GRANT SELECT, INSERT, UPDATE, DELETE ON symbols TO {DB_BOT_USER};
            GRANT SELECT, INSERT, UPDATE, DELETE ON candles TO {DB_BOT_USER};
            GRANT SELECT, INSERT, UPDATE, DELETE ON features_candles TO {DB_BOT_USER};
            GRANT SELECT, INSERT, UPDATE, DELETE ON features_orderbook TO {DB_BOT_USER};
            GRANT SELECT, INSERT, UPDATE, DELETE ON features_ticker_24 TO {DB_BOT_USER};
            GRANT SELECT, INSERT, UPDATE, DELETE ON labels TO {DB_BOT_USER};
            GRANT SELECT, INSERT, UPDATE, DELETE ON predictions TO {DB_BOT_USER};
        """)

        # Accorder les permissions sur toutes les séquences
        cur.execute(f"""
            GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {DB_BOT_USER};
        """)
        
        print("Tables créées avec succès.")
    

# Création des tables liées au scraping
with psycopg.connect(**CRYPTOBOT_CONN_INFO) as conn:

    conn.autocommit = True
    with conn.cursor() as cur:

        # Vérification de l'existence de la table features_scraping_sentiment
        cur.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'features_scraping_sentiment';
        """)

        table_exists = cur.fetchone()

        if table_exists:
            logger.warning("La table features_scraping_sentiment existe déjà.")
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS features_scraping_sentiment (
                    id SERIAL PRIMARY KEY,
                    article_id VARCHAR(50) NOT NULL,
                    base_asset VARCHAR(10) NOT NULL,
                    crypto_sentiment VARCHAR(20) NOT NULL,
                    crypto_confidence FLOAT NOT NULL,
                    crypto_emotion VARCHAR(20) NOT NULL,
                    crypto_intensity FLOAT NOT NULL,
                    article_polarity FLOAT NOT NULL,
                    article_subjectivity FLOAT NOT NULL,
                    published_at TIMESTAMP WITH TIME ZONE NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(article_id, base_asset)
                );
            """)
            
            # Créer un index combiné sur le base asset et la date de publication pour optimiser les requêtes temporelles
            cur.execute("""
                CREATE INDEX idx_sentiment_asset_date ON features_scraping_sentiment (base_asset, published_at DESC);
            """)
            
            # Accorder les permissions sur la nouvelle table au user bot
            cur.execute(f"""
                GRANT SELECT, INSERT, UPDATE, DELETE ON features_scraping_sentiment TO {DB_BOT_USER};
                GRANT USAGE, SELECT ON SEQUENCE features_scraping_sentiment_id_seq TO {DB_BOT_USER};
            """)
            
            logger.info("Table 'features_scraping_sentiment' créée avec succès.")