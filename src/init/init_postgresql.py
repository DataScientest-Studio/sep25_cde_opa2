from src.config import DB_NAME, DB_ROOT_USER, DB_ROOT_PASSWORD, DB_BOT_USER, DB_BOT_PASSWORD, PG_DB_PORT, PG_HOST
import psycopg

with psycopg.connect(
            dbname='postgres',
            user=DB_ROOT_USER,
            password=DB_ROOT_PASSWORD,
            host=PG_HOST,
            port=PG_DB_PORT
        ) as conn:
    
    conn.autocommit = True # Nécessaire pour la commande CREATE DATABASE
    with conn.cursor() as cur:

        # Creation de la base de données DB_NAME si elle n'existe pas déjà
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}';")
        existing_db=cur.fetchone()
        if not existing_db:
            cur.execute(f"CREATE DATABASE {DB_NAME};")
            print(f"La base de données '{DB_NAME}' est créée.")
        else:
            print(f"La base de données '{DB_NAME}' existe déjà.")

        # Creation du user DB_BOT_USER pour manipulation de DB_NAME
        cur.execute(f"SELECT 1 FROM pg_roles WHERE rolname='{DB_BOT_USER}';")
        existing_user=cur.fetchone()
        if not existing_user:
            # Creation du user DB_BOT_USER
            cur.execute(f"CREATE USER {DB_BOT_USER} WITH PASSWORD '{DB_BOT_PASSWORD}';")
            print(f"L'utilisateur {DB_BOT_USER} créé.")
        else:
            print(f"L'utilisateur {DB_BOT_USER} existe déjà.")


with psycopg.connect(
            dbname=DB_NAME,
            user=DB_ROOT_USER,
            password=DB_ROOT_PASSWORD,
            host=PG_HOST,
            port=PG_DB_PORT
        ) as conn:

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

            print(f"Permissions accordées au user {DB_BOT_USER} sur la base {DB_NAME}")


# Création de la table klines si elle n'existe pas
with psycopg.connect(
            dbname=DB_NAME,
            user=DB_ROOT_USER,
            password=DB_ROOT_PASSWORD,
            host=PG_HOST,
            port=PG_DB_PORT
        ) as conn:

        conn.autocommit = True
        with conn.cursor() as cur:

            # Vérifier si la table klines existe
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'klines'
                );
            """)
            
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Création de la table klines
                cur.execute("""
                    CREATE TABLE klines (
                        id SERIAL PRIMARY KEY,
                        open_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        close_time TIMESTAMP WITH TIME ZONE NOT NULL,
                        open_price DECIMAL(18, 8) NOT NULL,
                        high_price DECIMAL(18, 8) NOT NULL,
                        low_price DECIMAL(18, 8) NOT NULL,
                        close_price DECIMAL(18, 8) NOT NULL,
                        volume DECIMAL(18, 8) NOT NULL,
                        quote_volume DECIMAL(18, 8) NOT NULL,
                        trades_count INTEGER NOT NULL,
                        taker_buy_base_volume DECIMAL(18, 8) NOT NULL,
                        taker_buy_quote_volume DECIMAL(18, 8) NOT NULL,
                        ignore VARCHAR(10) DEFAULT '0',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(open_time, close_time)
                    );
                    CREATE TABLE symbols (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(20) NOT NULL UNIQUE,
                        base_asset VARCHAR(20),
                        quote_asset VARCHAR(20),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );

                    CREATE TABLE candles (
                        id SERIAL PRIMARY KEY,
                        id_symbol INT NOT NULL,
                        interval VARCHAR(10) NOT NULL,
                        open_time TIMESTAMP NOT NULL,
                        open NUMERIC,
                        high NUMERIC,
                        low NUMERIC,
                        close NUMERIC,
                        volume NUMERIC,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (id_symbol) REFERENCES symbols(id)
                    );

                    CREATE TABLE features_candles (
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

                    CREATE TABLE features_orderbook (
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

                    CREATE TABLE features_ticker_24 (
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

                    CREATE TABLE dataset_xxx (
                        id SERIAL PRIMARY KEY,
                        id_symbol INT NOT NULL,
                        timestamp_candle TIMESTAMP,
                        rsi_14 NUMERIC,
                        macd NUMERIC,
                        ema_20 NUMERIC,
                        sentiment_score FLOAT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (id_symbol) REFERENCES symbols(id)
                    );

                    CREATE TABLE labels (
                        id SERIAL PRIMARY KEY,
                        id_symbol INT NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        label_up_down INT,
                        label_return NUMERIC,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (id_symbol) REFERENCES symbols(id)
                    );

                    CREATE TABLE predictions (
                        id SERIAL PRIMARY KEY,
                        id_symbol INT NOT NULL,
                        timestamp TIMESTAMP NOT NULL,
                        predicted_value NUMERIC,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (id_symbol) REFERENCES symbols(id)
                    );

                    CREATE TABLE news_sentiments (
                        id SERIAL PRIMARY KEY,
                        id_symbol INT NOT NULL,
                        article_time TIMESTAMP,
                        sentiment_score FLOAT,
                        sentiment_label VARCHAR(20),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (id_symbol) REFERENCES symbols(id)
                    );

                    CREATE TABLE news_temporal (
                        id SERIAL PRIMARY KEY,
                        id_symbol INT NOT NULL,
                        published TIMESTAMP,
                        news_density_1h INT,
                        news_density_24h INT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (id_symbol) REFERENCES symbols(id)
                    );

                    CREATE TABLE news_market (
                        id SERIAL PRIMARY KEY,
                        id_symbol INT NOT NULL,
                        published TIMESTAMP,
                        return_1h FLOAT,
                        volatility_1h FLOAT,
                        trend_1h VARCHAR(20),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (id_symbol) REFERENCES symbols(id)
                    );

                """)
                
                # Créer un index sur open_time pour optimiser les requêtes temporelles
                cur.execute("""
                    CREATE INDEX idx_klines_open_time ON klines(open_time);
                """)
                
                # Accorder les permissions sur la nouvelle table au user bot
                cur.execute(f"""
                    GRANT SELECT, INSERT, UPDATE, DELETE ON klines TO {DB_BOT_USER};
                    GRANT USAGE, SELECT ON SEQUENCE klines_id_seq TO {DB_BOT_USER};
                """)
                
                print("Table 'klines' créée avec succès.")
            else:
                print("La table 'klines' existe déjà.")