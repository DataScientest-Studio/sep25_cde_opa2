import sys
import psycopg

from src.config import DB_BOT_PASSWORD, DB_BOT_USER, DB_NAME, MONGO_DB_PORT, MONGO_HOST, PG_DB_PORT, PG_HOST
from src.common.custom_logger import logger

from pymongo import MongoClient
from pymongo.errors import PyMongoError

class MongoConnector:
    """Classe pour la connection à MongoDb"""
    
    def __init__(self):
        self.mongo_client = None
        self.db = None
        
    def connect(self):
        "Établit la connexion à MongoDB."

        try:
            # Construction de l'URI MongoDB
            username = DB_BOT_USER
            password = DB_BOT_PASSWORD
            host = MONGO_HOST
            port = MONGO_DB_PORT
            db_name = DB_NAME   
            
            if username and password and host and port and db_name:
                connection_string = f"mongodb://{username}:{password}@{host}:{port}/{db_name}"
            else:
                logger.error("Configuration MongoDB incomplète. Veuillez vérifier les variables d'environnement.")
                sys.exit(1)
            
            self.mongo_client = MongoClient(connection_string)
            self.db = self.mongo_client[db_name]
            
            # Test de la connexion
            self.mongo_client.admin.command('ping')
            logger.info(f"Connexion réussie à MongoDB: {host}:{port}")
            return self
            
        except PyMongoError as e:
            logger.error(f"Erreur de connexion MongoDB: {e}")
            sys.exit(1)

    def close(self):
        """Ferme la connexion à MongoDB."""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("Connexion MongoDB fermée")

class PostgreSQLConnector:
    """Classe pour la connection à PostgreSQL"""
    
    def __init__(self):
        self.conn = None
        
    def connect(self):
        "Établit la connexion à PostgreSQL."

        try:
            conn_info=dict(
                user = DB_BOT_USER,
                password = DB_BOT_PASSWORD,
                host = PG_HOST,
                port = PG_DB_PORT,
                dbname = DB_NAME   
            )
            
            if not None in conn_info.values():
                self.conn = psycopg.connect(**conn_info)
            else:
                logger.error("Configuration PostgreSQL incomplète. Veuillez vérifier les variables d'environnement.")
                sys.exit(1)
            
            # Test de la connexion
            try:
                self.conn.execute("SELECT 1")
                logger.info(f"Connexion réussie à PostgreSQL: {conn_info['host']}:{conn_info['port']}")
            except Exception as e:
                logger.error(f"Erreur de connexion PostgreSQL: {e}")
                sys.exit(1)

            return self
            
        except PyMongoError as e:
            logger.error(f"Erreur de connexion PostgreSQL: {e}")
            sys.exit(1)

    def close(self):
        """Ferme la connexion à PostgreSQL."""
        if self.conn:
            self.conn.close()
            logger.info("Connexion PostgreSQL fermée")