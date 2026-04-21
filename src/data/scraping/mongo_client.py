
from src.common.connectors import MongoConnector
from src.common.custom_logger import logger
from typing import Dict, List
from datetime import datetime

from pymongo import UpdateOne
from pymongo.errors import PyMongoError
from pymongo.cursor import Cursor

from src.common.mongo import MongoCommon

class MongoClient(MongoConnector):
    """Classe pour la recherche, l'insertion et la mise à jour des articles scrapés dans MongoDB."""

    def __init__(self):
        super().__init__()
        self.connect()
        self.common = MongoCommon(self)    

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

    def get_complete_articles(self, collection_name: str, limit: int = 1) -> Cursor:
        """
        Récupère les articles dont le contenu a été récupé

        Args:
            collection_name: Nom de la collection MongoDB
            limit: Nombre max d'articles retournés
        """

        try:
            collection = self.db[collection_name]
            complete_articles=collection.find(
                {
                    "content_scraped": True,
                    "crypto_detected": {"$ne": True}
                },
                limit=limit
            )
            return complete_articles
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

        if not data:
            logger.warning("Aucune donnée à sauvegarder")
            return False

        try:
            collection = self.db[collection_name]
            now = datetime.now().timestamp()            
            operations = []
            
            for document in data:
                update_fields = {k: v for k, v in document.items() if k != "_id"}

                op = UpdateOne(
                    {'_id': document['_id']}, 
                    {
                        "$set": {
                                **update_fields,
                                'last_seen': now,
                                'content_scraped': True
                            },
                    },
                )

                operations.append(op)
            
            # Execution
            result = collection.bulk_write(operations, ordered=False)
                    
            logger.info(f"Mise à jour terminée: {result.matched_count}, {result.modified_count} documents mis à jour sur {len(data)}.")
            
            return True
            
        except PyMongoError as e:
            logger.error(f"Erreur lors de la mise à jour dans MongoDB: {e}")
            return False    

    def save_to_mongodb(self, data: List[Dict], collection_name: str, is_source: bool = False):
        """
        Sauvegarde les données dans MongoDB.
        
        Args:
            data: Données à sauvegarder 
            collection_name: Nom de la collection MongoDB
            is_source: Permet de savoir la données est une donnée source ou non
            
        Returns:
            dict: Retourne la liste des ids crées
        """        
        if not data:
            return {"new_ids": [], "original_ids": []}

        try:
            collection = self.db[collection_name]
            operations = []
            now = datetime.now().timestamp()

            # Création d'un index unique pour éviter les doublons
            collection.create_index([
                ('link_to_article', 1)
            ], unique=True)

            for doc in data:
                payload = doc.copy()
                payload.pop('_id', None)
                payload["last_seen"] = now

                if is_source:
                    # La source de données provient du scraping
                    set_on_insert = {
                        "scraped_at": now,
                        "first_seen": now,
                        "raw_content": None,
                        "text_content": None,
                        "content_scraped": False,
                        "comments": [],
                        "comments_scraped": False
                    }
                else:
                    # Enrichissement et copie de la source vers une collection
                    # La collection enrichie servira de base pour le calcul des features
                    # Et l'envoi vers postgresSQL
                    set_on_insert = {
                        "enriched_at": now,
                        "feature_sentiment_analyzed": 0
                    }

                op = UpdateOne(
                    {"link_to_article": doc["link_to_article"]},
                    {
                        "$set": payload,
                        "$setOnInsert": set_on_insert
                    },
                    upsert=True
                )
                operations.append(op)

            # Exécution
            result = collection.bulk_write(operations, ordered=False)

            # Ids créés
            new_ids = list(result.upserted_ids.values())

            # Ids originaux
            original_ids = [d["original_id"] for d in data if "original_id" in d]

            logger.info(f"Sauvegarde terminée: {len(new_ids)} documents insérés, {result.matched_count} documents mis à jour")

            return {
                "new_ids": new_ids, 
                "original_ids": original_ids
            }

        except PyMongoError as e:
            logger.error(f"Erreur BulkWrite: {e}")
            return None