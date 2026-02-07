import os
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

        cur.execute(f"SELECT 1 FROM pg_roles WHERE rolname='{DB_BOT_USER}';")
        existing_user=cur.fetchone()
        if not existing_user:
            # Creation du user DB_BOT_USER
            cur.execute(f"CREATE USER {DB_BOT_USER} WITH PASSWORD '{DB_BOT_PASSWORD}';")

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

            print(f"L'utilisateur {DB_BOT_USER} créé.")
        else:
            print(f"L'utilisateur {DB_BOT_USER} existe déjà.")

# @TODO Se connecter avec le user BOT pour créer les tables.