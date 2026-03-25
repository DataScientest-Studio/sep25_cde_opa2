
from src.common.connectors import MongoConnector
from src.common.custom_logger import logger
from typing import Dict, List
from datetime import datetime

from pymongo.errors import PyMongoError
from pymongo.cursor import Cursor
from bson.objectid import ObjectId

class MongoClient(MongoConnector):
    """Classe pour la récupération d'articles et la pose de flags après calcul des features."""

    def get_articles(self, collection_name: str) -> Cursor:
        """
        Récupère les articles dont le contenu n'a pas encore été récupéré

        Args:
            collection_name: Nom de la collection MongoDB
        """

        try:
            collection = self.db[collection_name]
            articles=collection.find(
                {
                    "features_calculated": False
                },
                limit=1
            )
            return articles
        
        except PyMongoError as e:
            logger.error(f"Erreur lors de la récupération des articles: {e}")
            return False

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