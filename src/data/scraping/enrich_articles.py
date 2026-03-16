import random
import sys
import argparse

from pymongo.cursor import Cursor
from bs4 import BeautifulSoup as bs

from src.data.scraping.antibot import close_cookie_modal, close_playwright, close_signup_modal, get_html_with_playwright, human_sleep, start_playwright_session
from src.data.scraping.mongo_client import MongoClient
from src.custom_logger import logger
from src.config import DB_NAME, ENV, MONGO_DB_PORT, DB_BOT_USER, DB_BOT_PASSWORD, MONGO_HOST

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
    mongodb_client = MongoClient(mongodb_config)

    # Connexion à MongoDB
    if not mongodb_client.connect_to_mongodb():
        logger.error("Impossible de se connecter à MongoDB")
        sys.exit(1)
    
    return mongodb_client

def complete_articles(articles_to_complete: Cursor):
    # Playwright is used to bypass cloudflare protection of investing.com
    p, browser, page = start_playwright_session()

    # Init session lifecycle and error variables
    session_page_limit = 1 if ENV == "docker" else random.randint(3, 6)  
    pages_scraped = 0
    consecutive_403 = 0

    articles_completed=list()

    articles=list(articles_to_complete)

    for index, article in enumerate(articles):
        article_data=dict()

        # Session lifecycle control
        if pages_scraped >= session_page_limit or consecutive_403 >= 2:

            logger.warning("Redémarrage de la session du browser")

            # Close page and playwright session
            page.close()
            close_playwright(p, browser)

            human_sleep(sleep=random.uniform(30, 60), msg="Attente humaine après limit de page dans la session ou 403")

            # Start a new playwright session
            p, browser, page = start_playwright_session()

            # Reset session lifecycle and error variables
            session_page_limit = 1 if ENV == "docker" else random.randint(3, 6)
            pages_scraped = 0
            consecutive_403 = 0

        # Get the complete article
        logger.info(f"Page scrappée: {article['link_to_article']}")

        article_complete_html=get_html_with_playwright(page=page, url=article['link_to_article'], selector='[id="article"]')

        if not article_complete_html:
            consecutive_403 += 1
            continue

        consecutive_403 = 0
        pages_scraped += 1

        # As a human, close cookie and signup modals
        close_cookie_modal(page)
        close_signup_modal(page)        

        article_complete_soup=bs(article_complete_html, 'lxml')
        article_complete_content=article_complete_soup.find(id='article')
        if article_complete_content:
            article_data['_id']=article['_id']
            article_data['raw_content']=str(article_complete_content)
            article_data['text_content']=article_complete_content.text
        
        logger.info(f"Récupération du contenu de l'article {index+1} terminée")
        articles_completed.append(article_data)

        # Due to the limit from query, we can use this method to count the number of the articles
        nb_articles_to_complete=len(articles)
        if session_page_limit != 1 and nb_articles_to_complete > 1 and index < nb_articles_to_complete-1:
            human_sleep(sleep=random.uniform(20, 60), msg="Attente humaine entre 2 pages")          
   
    logger.info(f"Récupération de {pages_scraped} article(s) sur {nb_articles_to_complete} terminée")
    return articles_completed


def main():
    args = parse_arguments()

    try: 
        mongodb_client=connect_to_mongo()
        collection_name='investing_articles'

        articles_to_complete=mongodb_client.get_articles_to_complete(collection_name, args.limit)
        
        articles_completed=complete_articles(articles_to_complete)
        
        success=mongodb_client.update_articles(articles_completed, collection_name)
        if not success:
            logger.error("Échec de la mise à jour des articles.")
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