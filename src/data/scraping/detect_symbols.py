import argparse
import json
import re
import sys
import time

from pathlib import Path
from typing import cast

from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceRequestException
from pymongo.cursor import Cursor

from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

from src.config import BINANCE_API_KEY, BINANCE_API_SECRET, DB_BOT_PASSWORD, DB_BOT_USER, DB_NAME, MONGO_DB_PORT, MONGO_HOST, PROJECT_ROOT
from src.data.scraping.custom_logger import logger
from src.data.scraping.scraping_mongo_client import ScrappingMongoClient

# Define a default symbols list in case of api error, or file access errors.
DEFAULT_SYMBOLS = [
    {
        "symbol": "BTC",
        "name": "Bitcoin",
        "alisases": ["btc", "bitcoin"]
    }
]

CRYPTO_SYMBOL_BLACKLIST = {
    "ONE","KEY","DATA","HIGH","LOW","TOP","NEW","ALL","WIN",
    "BAD","HOT","FUN","ACE","SUN","GAS","MAP","DOG","CAT",
    "ANT","MAN","YOU","HER","HIM","NOW","DAY","RED","BLUE",
    "GREEN","OPEN","SAFE","FREE","FAST","LIVE","COOL","RICH",
    "HERO","MOON","STAR","WAVE"
} 

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Scrap un article sur investing.com afin d'enrichir ses données"
    )
    
    parser.add_argument(
        "--force-update",
        type=bool,
        default=False,
        help="Forcer la mise à jour du fichier"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Nombre d'articles à traiter"
    )    
    
    return parser.parse_args()

def get_cryptos_symbols_and_names(force_update=False) -> list[dict[str, str]] | None:
    file_name = "mapping_cryptos_symbol_name.json"
    path_to_symbols_and_names = Path(f"{PROJECT_ROOT}/data/external/{file_name}")

    # If file exist, return the data, except when force_update is set to True
    if not force_update and path_to_symbols_and_names.exists():
        try:
            with open(path_to_symbols_and_names, "r", encoding="utf-8") as f:
                return cast(list[dict[str, str]], json.load(f))
        except json.JSONDecodeError as e:
            logger.error(f"Erreur lors de la lecture du JSON: {e.msg}")
            logger.warning(f"Le fichier va être remplacé par un nouveau")

    # Call binance api if force_update is set to True, or if file doesn't exist.
    logger.info(f"Appel api afin de générer le fichier de mapping entre un symbole crypto et son nom")
    try:
        mapping_cryptos=list()
        binance_client = Client(api_key=BINANCE_API_KEY, api_secret=BINANCE_API_SECRET)
        server_time = binance_client.get_server_time()
        binance_client.timestamp_offset = server_time['serverTime'] - int(time.time() * 1000)
        coins_list=binance_client._request('get', f'{binance_client.MARGIN_API_URL}/v1/capital/config/getall', signed=True, data={})
        
        # Keep only used attributes
        for coin in coins_list:
            mapping_cryptos.append({
                "symbol": coin['coin'],
                "name": coin['name'],
                "aliases": [str.lower(coin['coin']), str.lower(coin['name'])]
            })
        
        # Save file
        try:
            with open(path_to_symbols_and_names, "w", encoding="utf-8") as f:
                json.dump(mapping_cryptos, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Erreur lors de l'écriture du fichier: {e}")
            return None
            
        return mapping_cryptos

    except BinanceAPIException as e:
        logger.error(f"Erreur API Binance: {e.code} - {e.message}")
        return None
    except BinanceRequestException as e:
        logger.error(f"Erreur Request Binance: {e.message}")
        return None  
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des données Binance: {e}")
        return None

def detect_crypto_symbol_in_article(article, symbols: list[dict[str, str]]):
    original_article_id=article.pop("_id")
    article_with_symbols=dict(article)
    detected_symbols=set()
    text_full_lower=f"{article['title']} {article['text_content']}".lower()
    
    filtered_symbols = [
        symbol for symbol in symbols 
            if len(symbol['symbol']) >= 3 
                and symbol['symbol'] not in CRYPTO_SYMBOL_BLACKLIST 
                and symbol['symbol'].lower() not in ENGLISH_STOP_WORDS
        ]

    # Scan text
    for symbol in filtered_symbols:
        for alias in symbol['aliases']:
            if re.search(rf"\b{re.escape(alias)}\b", text_full_lower, flags=re.IGNORECASE):
                detected_symbols.add(symbol['symbol'])
                break

    if not detected_symbols:
        logger.warning(f"Pas de symbol trouvé pour {original_article_id}, {article['title']}")
    
    article_with_symbols["original_id"] = original_article_id
    article_with_symbols["symbols"] = list(detected_symbols)

    return article_with_symbols

def detect_crypto_symbol_in_articles(articles: Cursor, symbols: list[dict[str, str]] | None = None):
    articles_to_update = list()
    if not symbols:
        symbols = DEFAULT_SYMBOLS

    for article in articles:
        article_to_update = detect_crypto_symbol_in_article(article, symbols)
        if article_to_update:
            articles_to_update.append(article_to_update)
    
    return articles_to_update

def connect_to_mongo():
    # Connect to mongo db and save datas
    mongodb_config = {
        'username': DB_BOT_USER,
        'password': DB_BOT_PASSWORD,
        'host': MONGO_HOST,
        'port': MONGO_DB_PORT,
        'db_name': DB_NAME
    }       

    # Initialisation du client mongo
    mongodb_client = ScrappingMongoClient(mongodb_config)

    # Connexion à MongoDB
    if not mongodb_client.connect_to_mongodb():
        logger.error("Impossible de se connecter à MongoDB")
        sys.exit(1)
    
    return mongodb_client

def main():
    args = parse_arguments()

    try:
        symbols_and_names=get_cryptos_symbols_and_names(force_update=args.force_update)
        if not symbols_and_names:
            logger.warning(
                """La récupération du mapping entre le symbol d'une crypto et son nom n'a pas été possible.
                Ainsi, seul le Bitcoin sera évalué dans l'identification des symbols présents dans les articles.
                """)

        mongodb_client=connect_to_mongo()
        collection_name='investing_articles'

        mongodb_client.db[collection_name].create_index([
            ("content_scraped", 1), 
            ("crypto_detected", 1)
        ])

        articles=mongodb_client.get_complete_articles(collection_name, args.limit)

        articles_with_symbols=detect_crypto_symbol_in_articles(articles, symbols=symbols_and_names)

        # Save in MongoDB
        results = mongodb_client.save_to_mongodb(articles_with_symbols, 'investing_articles_enriched')

        # Flag original articles
        if results and results["original_ids"]:
            success=mongodb_client.flag_articles(ids=results["original_ids"], flag="crypto_detected", value=True, collection_name="investing_articles")
            if success:
                logger.info(f"Detection de symboles terminée : {len(results['original_ids'])} articles traités et marqués.")
            else:
                logger.error("Échec du flag dans la source.")
                logger.error("Les données ont été sauvegardées, mais seront ré-analysées au prochain lancement car le flag n'a pas pu être mis à jour.")
                sys.exit(1)

    except Exception as e:
            logger.error(f"Erreur critique dans le main : {e}")
            sys.exit(1)
    finally:
        # Close connexions
        if mongodb_client: 
            mongodb_client.close_connections()

if __name__ == "__main__":
    main()