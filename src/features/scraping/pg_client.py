
from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger
from typing import Dict, Iterator
from datetime import datetime, timezone

from psycopg.errors import DatabaseError, Error

class PGClient(PostgreSQLConnector):
    """Classe pour la manipulation des données dans la base de données PostgreSQL."""

    def __init__(self):
        super().__init__()
        self.connect()

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
            logger.error(f"DatabaseError lors de l'insertion des résultats de l'analyse': {e}")
            return False

    def get_last_sentiment_daily_insertion(self, base_asset: str):
        """
        Retourne la dernière insertion en base pour les sentiments journaliers

        Args:
            base_asset: symbol souhaité
        """

        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(date_dg) FROM features_sentiment_daily WHERE base_asset = %s;
                """, (base_asset,))
                
                last_inserted_date = cur.fetchone()[0]
    
                return last_inserted_date
            
        except (DatabaseError, Error) as e:
            logger.error(f"DatabaseError lors de la récupération de la dernière insertion': {e}")
            return False

    def insert_sentiment_daily(self, base_asset: str, daily_sentiment: Iterator):
        """
        Insertion de l'aggrégation des sentiments sur une journée

        Args:
            base_asset: symbol souhaité
            daily_sentiment: agglomératon des sentiments d'un symbol sur une journée.
        """

        try:
            with self.conn.cursor() as cur:
                # Préparation des données pour une insertion groupée (batch)
                query = """
                    INSERT INTO features_sentiment_daily 
                    (base_asset, date_dg, sentiment_score, sentiment_smooth, sentiment_weighted, sentiment_weighted_smooth, articles_volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (base_asset, date_dg) 
                    DO UPDATE SET 
                        sentiment_score = EXCLUDED.sentiment_score,
                        sentiment_smooth = EXCLUDED.sentiment_smooth,
                        sentiment_weighted = EXCLUDED.sentiment_weighted,
                        sentiment_weighted_smooth = EXCLUDED.sentiment_weighted_smooth,
                        articles_volume = EXCLUDED.articles_volume;
                """                
                
                # Préparation des données, une ligne par symbole trouvé
                rows_to_insert = [
                    (
                        base_asset,
                        row['date_dg'].date(),
                        float(row['sentiment_score']),
                        float(row['sentiment_smooth']),
                        float(row['sentiment_weighted']),
                        float(row['sentiment_weighted_smooth']),
                        int(row['articles_volume'])
                    )
                    for _, row in daily_sentiment
                ]
                
                if rows_to_insert:
                    cur.executemany(query, rows_to_insert)
    
                return True
            
        except (DatabaseError, Error) as e:
            logger.error(f"DatabaseError lors de l'insertion des sentiments par jour': {e}")
            return False
