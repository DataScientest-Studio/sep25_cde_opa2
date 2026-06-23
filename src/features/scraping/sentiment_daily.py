import pandas as pd
import sys

import psycopg

from src.common.get_symbols_and_names import get_cryptos_symbols_and_names
from src.features.scraping.pg_client import PGClient
from src.common.custom_logger import logger

def aggregate_and_compute_sentiment(base_asset: str, pg_client: PGClient):
    # Tout le traitement d'un symbole est encapsulé dans une transaction isolée
    try:
        with pg_client.conn.transaction():
            
            # Récupération de la date de la dernière insertion pour la reprise
            last_inserted_date = pg_client.get_last_sentiment_daily_insertion(base_asset)
            
            start_fetch_date = None
            if last_inserted_date:
                start_fetch_date = pd.to_datetime(last_inserted_date) - pd.Timedelta(days=3)
                logger.info(f"[{base_asset}] Reprise et réamorçage à partir du {start_fetch_date.date()}")
            else:
                logger.info(f"[{base_asset}] Aucun historique. Calcul global.")

            # Récupération des données par article
            query = """
                SELECT published_at::date as date_dg, crypto_sentiment
                FROM features_scraping_sentiment
                WHERE base_asset = %s
            """
            params = [base_asset]
            if start_fetch_date:
                query += " AND published_at >= %s"
                params.append(start_fetch_date)
            query += " ORDER BY published_at ASC"
            
            # df_articles = pd.read_sql(query, pg_client.conn, params=params)
            with pg_client.conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

            df_articles = pd.DataFrame(rows)            

            print(df_articles.head())

            if df_articles.empty:
                logger.warning(f"[{base_asset}] Aucun nouvel article trouvé.")
                return

            # Mapping sentiment score
            sentiment_map = {'positive': 1, 'neutral': 0, 'negative': -1}
            df_articles['sentiment_score'] = df_articles['crypto_sentiment'].astype(str).str.lower().map(sentiment_map)                

            # Agrégation et calculs des fenêtres glissantes
            df_daily = df_articles.groupby('date_dg').agg(
                sentiment_score=('sentiment_score', 'mean'),
                articles_volume=('sentiment_score', 'size')
            ).reset_index()


            df_daily['date_dg'] = pd.to_datetime(df_daily['date_dg'])
            df_daily = df_daily.sort_values('date_dg').reset_index(drop=True)

            df_daily['sentiment_smooth'] = df_daily['sentiment_score'].rolling(window=3, min_periods=1).mean()
            df_daily['sentiment_weighted'] = df_daily['sentiment_score'] * df_daily['articles_volume']
            df_daily['sentiment_weighted_smooth'] = df_daily['sentiment_weighted'].rolling(window=3, min_periods=1).mean()

            # Nettoyage des données
            df_daily = df_daily.astype(object).where(df_daily.notna(), None)

            # Filtrage pour la reprise
            if last_inserted_date:
                update_threshold = pd.to_datetime(last_inserted_date) - pd.Timedelta(days=2)
                df_daily = df_daily[df_daily['date_dg'] >= update_threshold]

            if df_daily.empty:
                return

            # Enregistrement des données
            pg_client.insert_sentiment_daily(base_asset, df_daily.iterrows())
                
        logger.info(f"[{base_asset}] Synchro batch terminée ({len(df_daily)} jours mis à jour).")

    except Exception as e:
        # Si ça plante ici, le rollback automatique du bloc transaction a déjà eu lieu.
        # La connexion globale reste saine pour le symbole suivant !
        logger.error(f"Erreur lors du traitement de {base_asset}: {str(e)}")

def main():
    pg_client = None

    try:
        pg_client=PGClient()

        # Recupération du mapping symbol -> names et aliases
        # symbols_and_names=get_cryptos_symbols_and_names()
        top_cryptos = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'DOGE', 'DOT', 'LINK', 'USDT', 'USDC']
        # symbols = [sn['symbol'] for sn in symbols_and_names]
        
        logger.info(f"Début de la synchronisation de groupe pour : {top_cryptos}")

        for symbol in top_cryptos:
            aggregate_and_compute_sentiment(symbol, pg_client)
        
        pg_client.conn.commit()
        logger.info("Tous les assets ont été synchronisés.")
    except Exception as e:
            logger.error(f"Erreur critique dans le main : {e}")
            sys.exit(1)
    finally:
        # Close connexions
        if pg_client is not None: 
            pg_client.close()

if __name__ == "__main__":
    main()    