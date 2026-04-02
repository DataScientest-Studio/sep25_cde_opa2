
from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger
from typing import Dict
from datetime import datetime, timezone

from psycopg.errors import DatabaseError, Error

class PGClient(PostgreSQLConnector):
    """Classe pour la manipulation des données dans la base de données PostgreSQL."""

    def insert_sentiment_analyse(self, analyse: Dict):
        """
        Insertion du résultat des analyses de sentiments par crypto detetectée au sein d'un article

        Args:
            analyses: analyses de sentiments pour une crypto detetectée au sein d' un article.
        """

        try:
            with self.conn.cursor() as cur:
                # Préparation des données pour une insertion groupée (batch)
                query = """
                    INSERT INTO features_scraping_sentiment (
                        article_id, base_asset, crypto_sentiment, crypto_confidence, 
                        crypto_emotion, crypto_intensity, article_polarity, 
                        article_subjectivity, published_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (article_id, base_asset) DO NOTHING;
                """
                
                # Préparation des données, une ligne par symbole trouvé
                rows_to_insert = [
                    (
                        str(analyse['article_id']),
                        result['symbol'],
                        result['sentiment'],
                        result['confidence'],
                        result['emotion'],
                        result['intensity'],
                        analyse['polarity'],
                        analyse['subjectivity'],
                        datetime.fromtimestamp(analyse['published_at_timestamp'], tz=timezone.utc)
                    ) for result in analyse['symbols']
                ]
                
                if rows_to_insert:
                    cur.executemany(query, rows_to_insert)
                    self.conn.commit()
    
                return True
            
        except (DatabaseError, Error) as e:
            self.conn.rollback()
            logger.error(f"DatabaseError lors de l'insertion des résultat de l'analyse': {e}")
            return False
