import random
import sys
import argparse

from pymongo.cursor import Cursor
from bs4 import BeautifulSoup as bs

from src.data.scraping.antibot import close_cookie_modal, close_playwright, close_signup_modal, get_html_with_playwright, human_sleep, start_playwright_session
from src.data.scraping.mongo_client import MongoClient
from src.common.custom_logger import logger
from src.config import ENV, SCRAPER_ENRICH_LIMIT

def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Scrap un article sur investing.com afin d'enrichir ses données"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=SCRAPER_ENRICH_LIMIT,
        help="Nombre d'articles à compléter"
    )
    
    return parser.parse_args()

def complete_articles(articles_to_complete: Cursor, client: MongoClient, collection: str):
    # Playwright is used to bypass cloudflare protection of investing.com
    p, browser, page = start_playwright_session()

    # Init session lifecycle and error variables
    session_page_limit = 1 if ENV == "docker" else random.randint(3, 6)  
    pages_scraped = 0
    consecutive_403 = 0
    articles_scraped = 0

    articles_in_error=list()

    articles=list(articles_to_complete)
    nb_articles_to_complete=len(articles)

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
        articles_scraped += 1

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
        
        # Save immediately.
        # If an error occurs, save data in a list and try to save at the end of the loop.
        success=client.update_articles([article_data], collection)
        if not success:
            logger.error(f"Échec de la mise à jour l'article {index+1}, stockage de l'article dans une liste pour retenter en sortie de boucle.")
            articles_in_error.append(article_data)
        else:
            logger.info(f"Sauvegarde de l'article {index+1} terminée")

        # Due to the limit from query, we can use this method to count the number of the articles
        if session_page_limit != 1 and nb_articles_to_complete > 1 and index < nb_articles_to_complete-1:
            human_sleep(sleep=random.uniform(20, 60), msg="Attente humaine entre 2 pages")          
   
    logger.info(f"Récupération de {articles_scraped} article(s) sur {nb_articles_to_complete} terminée.")
    logger.info(f"{articles_scraped-len(articles_in_error)} article(s) sauvées immédiatement.")
    logger.info(f"{len(articles_in_error)} articles en erreur avec nouvelle tentative en fin de boucle.")
    return articles_in_error

def main():
    args = parse_arguments()

    try: 
        mongodb_client=MongoClient().connect()
        collection='investing_articles'

        articles_to_complete=mongodb_client.get_articles_to_complete(collection, args.limit)
        
        articles_in_error=complete_articles(articles_to_complete, client=mongodb_client, collection=collection)

        # Try to save only the rest of articles if exists.
        # The list could have some articles to save because of on error in a previous try during the enrich loop.
        if len(articles_in_error):
            success=mongodb_client.update_articles(articles_in_error, collection)
            if not success:
                logger.error("Échec de la mise à jour des articles restants à sauver.")
                sys.exit(1)
            else:
                logger.info(f"{len(articles_in_error)} articles restants sauvés en fin de boucle.")

    except Exception as e:
            logger.error(f"Erreur critique dans le main : {e}")
            sys.exit(1)
    finally:
        # Close connexions
        if mongodb_client: 
            mongodb_client.close()  
        

if __name__ == "__main__":
    main()