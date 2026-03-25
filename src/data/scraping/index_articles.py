import argparse
import sys
from bs4 import BeautifulSoup as bs
from typing import Dict, List
from datetime import datetime
import random

from src.data.scraping.mongo_client import MongoClient
from src.common.custom_logger import logger
from src.config import ENV, SCRAPER_INDEX_LIMIT
from src.data.scraping.antibot import close_cookie_modal, close_playwright, close_signup_modal, get_html_with_playwright, human_sleep, init_playwright, start_playwright_session


def parse_arguments():
    """Parse les arguments de ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Scrappe une liste d'articles provenant du site investing.com"
    )
    
    parser.add_argument(
        "--page_number_start",
        type=int,
        default=1,
        help="Numéro de page de départ"
    )
    
    parser.add_argument(
        '--nb_page',
        type=int,
        default=SCRAPER_INDEX_LIMIT,
        help="Nombre de page à scrapper"
    )
    
    return parser.parse_args()

def scrap_pages(client: MongoClient, collection: str):
    args = parse_arguments()

    if args.nb_page == 1:
        logger.info(f'{args.nb_page} page sera scrappée à partir de la page {args.page_number_start}')
    else:
        logger.info(f'{args.nb_page} pages seront scrappées à partir de la page {args.page_number_start}')
    
    pages_to_scrap=list(range(args.page_number_start, args.page_number_start+args.nb_page))

    base_url='https://www.investing.com/news/cryptocurrency-news'

    articles_data=list()

    # Playwright is used to bypass cloudflare protection of investing.com
    p, browser, page = start_playwright_session()

    # Init session lifecycle and error variables
    session_page_limit = 1 if ENV == "docker" else random.randint(3, 6)  
    pages_scraped = 0
    consecutive_403 = 0

    articles_found = 0
    articles_saved = 0

    for index, page_to_scrap in enumerate(pages_to_scrap):
        url = base_url if page_to_scrap == 1 else f"{base_url}/{page_to_scrap}"

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
        
        logger.info(f'Page scrappée: {url}')

        list_page_html=get_html_with_playwright(page=page, url=url, selector='[data-test="news-list"]')

        if not list_page_html:
            consecutive_403 += 1
            continue

        consecutive_403 = 0
        pages_scraped += 1        

        # As a human, close cookie and signup modals
        close_cookie_modal(page)
        close_signup_modal(page)

        list_page_soup=bs(list_page_html, 'lxml')

        container=list_page_soup.find('ul', {
            'data-test': 'news-list'
        })

        if not container:
            logger.error("Container news-list introuvable, passage à la page suivante")
            continue

        logger.info(f"Container news-liste trouvé, début de la récupération")

        articles=container.find_all('article')
        articles_found += len(articles)

        for article in articles:
            article_data=dict({
                'title': None,
                'link_to_article':None,
                'summary':None,
                'published_at':None,
                'published_at_timestamp':None,
                'provider':None,
                'link_to_comments':None,
            })

            # Get the title and the complete article link
            title_elm=article.find('a', {
                    'data-test': 'article-title-link'
                })
            if title_elm:
                article_data['title']=title_elm.text
                article_data['link_to_article']=title_elm.get('href')
                
            # Get the summary
            summary=article.find('p', {
                'data-test': 'article-description'
            })
            if summary:
                article_data['summary']=summary.text

            # Get the published at date
            published_at=article.find('time')
            if published_at:
                article_data['published_at']=published_at.get('datetime')
                iso_clean=article_data['published_at'].replace("Z", "+00:00")
                article_data['published_at_timestamp']=datetime.fromisoformat(iso_clean).timestamp()

            # Get the provider name
            provider=article.find('span', {
                'data-test': 'news-provider-name'
            })
            if provider:
                article_data['provider']=provider.text

            # Get comments page link
            footer_ul=article.find('ul')
            if footer_ul:
                summary_footer_elm=footer_ul.find_all('li')
                if len(summary_footer_elm) > 1:
                    comments_link=summary_footer_elm[1]
                    if comments_link.find('a') and comments_link.find('a').get('href'):
                        article_data['link_to_comments']=comments_link.find('a').get('href')
          
            articles_data.append(article_data)
        
        logger.info(f'Récupération des articles de la page {page_to_scrap} terminée.')
        
        # Save immediately.
        # If an error occurs, save data in a list and try to save at the end of the loop.
        results=save_to_mongo(client, collection, articles_data, close=False)
        if not results:
            logger.error(f"Échec de la sauvegarde des articles de la page {page_to_scrap}, nouvelle tentative en sortie de boucle.")
        else:
            articles_saved += len(articles_data)
            articles_data=list()
            logger.info(f"Sauvegarde de la page {page_to_scrap} terminée.")        

        if session_page_limit != 1 and len(pages_to_scrap) > 1 and index < len(pages_to_scrap)-1:
            human_sleep(sleep=random.uniform(20, 60), msg="Attente humaine entre 2 pages")  
            
    
    logger.info(f"Récupération de la liste d'articles terminées, {articles_found} trouvés.")
    logger.info(f"{articles_saved} article(s) sauvées immédiatement.")
    logger.info(f"{articles_found - articles_saved} articles en erreur avec nouvelle tentative en fin de boucle.")

    # Close page and playwright session
    page.close()
    close_playwright(p, browser)

    return articles_data

def save_to_mongo(client: MongoClient, collection: str, data: List[Dict], close: bool = True):
    if not data or not len(data):
        logger.info('Aucune données à insérer!')
        return None
        
    try:
        results = client.save_to_mongodb(data, collection, is_source=True)
        return results
    except Exception as e:
            logger.error(f"Erreur critique dans la sauvegarde des données : {e}")
            return None
    finally:
        # Close connexions
        if client and close: 
            client.close()

def main():
    try:
        client=MongoClient().connect()
        collection='investing_articles'
        articles_data=scrap_pages(client, collection)

        # Try to save only the rest of articles if exists.
        # The list could have some articles to save because of on error in a previous try during the enrich loop.
        if len(articles_data):
            results=save_to_mongo(client, collection, articles_data)
            if results and results["new_ids"]:
                logger.info(f"{len(results['new_ids'])} nouveaux articles ajoutés à la base.")
            else:
                logger.error("Échec de la mise à jour des articles restants à sauver.")
                sys.exit(1)
        
    except Exception as e:
        logger.error(f"Erreur critique dans le main : {e}")
        sys.exit(1)

    finally:
        # Close connexions
        if client: 
            client.close()

if __name__ == "__main__":
    main()