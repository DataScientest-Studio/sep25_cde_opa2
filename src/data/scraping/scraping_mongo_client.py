
from src.data.scraping.custom_logger import logger
from typing import Dict, List
from datetime import datetime

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.cursor import Cursor

class ScrappingMongoClient:
    """Classe pour la connection et le stockage dans MongoDB."""
    
    def __init__(self, mongodb_config: Dict[str, str]):
        """
        Initialise le collecteur de données.
        
        Args:
            mongodb_config: Configuration MongoDB
        """
        self.mongo_client = None
        self.db = None
        self.mongodb_config = mongodb_config
        
    def connect_to_mongodb(self) -> bool:
        """
        Établit la connexion à MongoDB.
        
        Returns:
            bool: True si la connexion est réussie, False sinon
        """
        try:
            # Construction de l'URI MongoDB
            username = self.mongodb_config['username']
            password = self.mongodb_config['password']
            host = self.mongodb_config['host']
            port = self.mongodb_config['port']
            db_name = self.mongodb_config['db_name']
            
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
            return True
            
        except PyMongoError as e:
            logger.error(f"Erreur de connexion MongoDB: {e}")
            return False
        
    def get_articles_to_complete(self, collection_name: str, limit: int = 1) -> Cursor:
        """
        Récupère les articles dont le contenu n'a pas encore été récupéré

        Args:
            collection_name: Nom de la collection MongoDB
            limit: Nombre max d'articles retournés
        """

        try:
            collection = self.db[collection_name]
            articles_to_complete=collection.find(
                {
                    "content_scraped": False
                },
                limit=limit
            )
            return articles_to_complete
        except PyMongoError as e:
            logger.error(f"Erreur lors de la récupération des articles: {e}")
            return False
    
    def update_articles(self, data:List[Dict], collection_name: str) -> bool:
        """
        Sauvegarde les nouvelles données d'un article.
        
        Args:
            data: Données scrappées à sauvegarder 
            collection_name: Nom de la collection MongoDB
            
        Returns:
            bool: True si la sauvegarde est réussie
        """
        try:
            if not data:
                logger.warning("Aucune donnée à sauvegarder")
                return False
            
            collection = self.db[collection_name]
            
            # Insertion des données
            updated_count = 0
            skipped_count  = 0
            
            for document in data:
                update_fields = {k: v for k, v in document.items() if k != "_id"}
                try:
                    now = datetime.now().timestamp()
                    result=collection.update_one(
                            {'_id': document['_id']}, 
                            {
                                '$set': {
                                    **update_fields,
                                    'last_seen': now,
                                    'content_scraped': True
                                }
                            }
                        )
                    if result.modified_count > 0:
                        updated_count+=1
                    else:
                        skipped_count+=1
                except PyMongoError as e:
                        logger.warning(f"Erreur lors de la mise à jour des articles: {e}")
                    
            logger.info(f"Mise à jour terminée: {updated_count} documents mis à jour, {skipped_count} doublons ignorés")
            return True
            
        except PyMongoError as e:
            logger.error(f"Erreur lors de la sauvegarde MongoDB: {e}")
            return False

    def save_scrapping_to_mongodb(self, data: List[Dict], collection_name: str) -> bool:
        """
        Sauvegarde les données scrappées dans MongoDB.
        
        Args:
            data: Données scrappées à sauvegarder 
            collection_name: Nom de la collection MongoDB
            
        Returns:
            bool: True si la sauvegarde est réussie
        """
        try:
            if not data:
                logger.warning("Aucune donnée à sauvegarder")
                return False
            
            collection = self.db[collection_name]
            
            # Création d'un index unique pour éviter les doublons
            collection.create_index([
                ('link_to_article', 1)
            ], unique=True)
            
            # Insertion des données
            upserted_id = []
            updated_count = 0
            skipped_count  = 0
            
            for document in data:
                try:
                    now = datetime.now().timestamp()
                    result=collection.update_one(
                        {"link_to_article": document["link_to_article"]},
                        {
                            "$set": {
                                "title": document["title"],
                                "summary": document["summary"],
                                "provider": document["provider"],
                                "published_at": document["published_at"],
                                "published_at_timestamp": document["published_at_timestamp"],
                                "link_to_comments": document["link_to_comments"], 
                                "last_seen": now
                            },
                            "$setOnInsert": {
                                "scrapped_at": now,
                                "first_seen": now,
                                "raw_content": None,
                                "text_content": None,
                                "content_scraped": False,
                                "comments": [],
                                "comments_scraped": False
                            }
                        },
                        upsert=True
                    )
                    if result.upserted_id:
                        upserted_id.append(result.upserted_id)
                    elif result.modified_count > 0:
                        updated_count+=1
                    else:
                        skipped_count+=1
                except PyMongoError as e:
                        logger.warning(f"Erreur lors de l'insertion: {e}")
            
            logger.info(f"Sauvegarde terminée: {len(upserted_id)} documents insérés, {updated_count} documents mis à jour, {skipped_count} doublons ignorés")
            return True
            
        except PyMongoError as e:
            logger.error(f"Erreur lors de la sauvegarde MongoDB: {e}")
            return False

    def close_connections(self):
        """Ferme la connexion à MongoDB."""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("Connexion fermée")