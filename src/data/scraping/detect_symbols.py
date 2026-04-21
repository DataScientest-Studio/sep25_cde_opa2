import argparse
import re
import sys

from pymongo.cursor import Cursor

from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

from src.common.get_symbols_and_names import get_cryptos_symbols_and_names
from src.config import BINANCE_API_KEY, BINANCE_API_SECRET, PROJECT_ROOT, SCRAPER_DETECT_LIMIT
from src.common.custom_logger import logger
from src.data.scraping.mongo_client import MongoClient

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
        default=SCRAPER_DETECT_LIMIT,
        help="Nombre d'articles à traiter"
    )    
    
    return parser.parse_args()

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

def detect_crypto_symbol_in_articles(articles: Cursor, symbols: list[dict[str, str]]):
    articles_to_update = list()

    for article in articles:
        article_to_update = detect_crypto_symbol_in_article(article, symbols)
        if article_to_update:
            articles_to_update.append(article_to_update)
    
    return articles_to_update

def main():
    args = parse_arguments()

    try:
        symbols_and_names=get_cryptos_symbols_and_names(force_update=args.force_update)

        mongodb_client=MongoClient()
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
            success=mongodb_client.common.flag_articles(ids=results["original_ids"], flag="crypto_detected", value=True, collection_name="investing_articles")
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
            mongodb_client.close()

if __name__ == "__main__":
    main()