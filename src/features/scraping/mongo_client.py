
from src.common.connectors import MongoConnector
from src.common.custom_logger import logger

from pymongo.errors import PyMongoError
from pymongo.cursor import Cursor

from src.common.mongo import MongoCommon
from src.config import SCRAPER_FEATURE_SENTIMENT_LIMIT

class MongoClient(MongoConnector):
    """Classe pour la récupération d'articles."""
    def __init__(self):
        super().__init__()
        self.connect()
        self.common = MongoCommon(self)

    def get_articles(self, collection_name: str) -> Cursor:
        """
        Récupère les articles dont le contenu n'a pas encore été récupéré

        Args:
            collection_name: Nom de la collection MongoDB
        """

        try:
            collection = self.db[collection_name]
            articles=collection.find(
                {"feature_sentiment_analyzed": {"$in": [0, None]}},
                limit=int(SCRAPER_FEATURE_SENTIMENT_LIMIT)
            )
            return articles
        
        except PyMongoError as e:
            logger.error(f"Erreur lors de la récupération des articles: {e}")
            return False