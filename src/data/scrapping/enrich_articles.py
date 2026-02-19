import sys
import argparse

from pymongo.cursor import Cursor
from bs4 import BeautifulSoup as bs

from src.data.scrapping.cloudflare_scraper import scraper
from src.data.scrapping.scrapping_mongo_client import ScrappingMongoClient
from src.data.scrapping.custom_logger import logger
from src.config import DB_NAME, MONGO_DB_PORT, DB_BOT_USER, DB_BOT_PASSWORD, MONGO_HOST

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Scrap un article sur investing.com afin d'enrichir ses données"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=1,
        help="Nombre d'articles à compléter"
    )
    
    return parser.parse_args()

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

def complete_articles(articles_to_complete: Cursor):
    articles_completed=list()
    for article in articles_to_complete:
        article_data=dict()

        # Get the complete article
        article_complete_html = scraper.get(article['link_to_article']).text
        article_complete_soup=bs(article_complete_html, 'lxml')
        article_complete_content=article_complete_soup.find(id='article')
        if article_complete_content:
             article_data['_id']=article['_id']
             article_data['raw_content']=str(article_complete_content)
             article_data['text_content']=article_complete_content.text
        
        articles_completed.append(article_data)
   
    return articles_completed


def main():
    args = parse_arguments()

    mongodb_client=connect_to_mongo()
    collection_name='investing_articles'

    articles_to_complete=mongodb_client.get_articles_to_complete(collection_name, args.limit)
    
    articles_completed=complete_articles(articles_to_complete)
    mongodb_client.update_articles(articles_completed, collection_name)

if __name__ == "__main__":
    main()