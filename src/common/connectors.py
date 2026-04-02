import sys
import psycopg

from src.config import DB_BOT_PASSWORD, DB_BOT_USER, DB_NAME, MONGO_DB_PORT, MONGO_HOST, PG_DB_PORT, PG_HOST
from src.common.custom_logger import logger

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from typing import List
from datetime import datetime
from bson.objectid import ObjectId


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

    def flag_articles(self, ids:List[ObjectId], flag: str, value: bool, collection_name: str) -> bool:
        """
        Ajout ou mise à jour d'un flag dans la collection.
        
        Args:
            ids: List d'ids à mettre à jour
            flag: Flag à mettre à jour
            value: Valeur du flag, boolean
            collection_name: Nom de la collection MongoDB
            
        Returns:
            bool: True si la sauvegarde est réussie
        """
        try:
            if not ids or not flag or not collection_name:
                logger.error("Impossible de mettre à jour les données car il manque un paramètre")
                return False
            
            collection = self.db[collection_name]
            now = datetime.now().timestamp()
            
            result=collection.update_many(
                    {'_id': {'$in': ids}},
                    {
                        '$set': {
                            flag: value,
                            'last_seen': now,
                        }
                    }
                )
                    
            total_requested = len(ids)
            matched = result.matched_count
            modified = result.modified_count

            if matched < total_requested:
                logger.warning(f"{total_requested - matched} IDs fournis n'ont pas été trouvés dans {collection_name}")

            logger.info(
                f"Succès [{flag}={value}]: {modified} modifiés, "
                f"{matched - modified} déjà à jour, "
                f"Total ciblé: {matched}"
            )
            return True
            
        except PyMongoError as e:
            logger.error(f"Erreur lors de la sauvegarde MongoDB: {e}")
            return False            

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