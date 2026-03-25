import sys

from src.config import DB_BOT_PASSWORD, DB_BOT_USER, DB_NAME, MONGO_DB_PORT, MONGO_HOST
from src.common.custom_logger import logger

from pymongo import MongoClient
from pymongo.errors import PyMongoError

class MongoConnector:
    """Classe pour la connection à MongoDb"""
    
    def __init__(self):
        """
        Initialise le collecteur de données.
        
        Args:
            mongodb_config: Configuration MongoDB
        """
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
                return False
            
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
            logger.info("Connexion fermée")