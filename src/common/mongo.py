from src.common.custom_logger import logger

from pymongo.errors import PyMongoError
from typing import List
from datetime import datetime
from bson.objectid import ObjectId


class MongoCommon():
    """Classe pour les manipulation communes au sein de MongoDb"""

    def __init__(self, connector):
        self.connector = connector    

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
            
            collection = self.connector.db[collection_name]
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