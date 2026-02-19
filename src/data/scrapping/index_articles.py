import argparse
import sys
from bs4 import BeautifulSoup as bs
from typing import Dict, List
from datetime import datetime

from src.data.scrapping.cloudflare_scraper import scraper
from src.data.scrapping.scrapping_mongo_client import ScrappingMongoClient
from src.data.scrapping.custom_logger import logger
from src.config import DB_NAME, MONGO_DB_PORT, DB_BOT_USER, DB_BOT_PASSWORD, MONGO_HOST


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
        default=1,
        help="Nombre de page à scrapper"
    )
    
    return parser.parse_args()

def scrap_pages():
    args = parse_arguments()
    if args.nb_page == 1:
        logger.info(f'{args.nb_page} page sera scrappée à partir de la page {args.page_number_start}')
    else:
        logger.info(f'{args.nb_page} seront scrappées à partir de la page {args.page_number_start}')
    
    pages_to_scrap=list(range(args.page_number_start, args.page_number_start+args.nb_page))

    base_url='https://www.investing.com/news/cryptocurrency-news'

    articles_data=list()
    for page_to_scrap in pages_to_scrap: 
        if page_to_scrap > 1:
            base_url=f'https://www.investing.com/news/cryptocurrency-news/{page_to_scrap}'

        logger.info(f'Page scrappée: {base_url}')

        list_page_html = scraper.get(base_url).text

        list_page_soup=bs(list_page_html, 'lxml')

        container=list_page_soup.find('ul', {
            'data-test': 'news-list'
        })

        if not container:
            logger.error("Container news-list introuvable")
            articles = []
        else:
            logger.info(f"Container news-liste trouvé, début de la récupération")
            articles=container.find_all('article')

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
            
    
    logger.info(f"Récupération de la liste d'articles terminées, {len(articles_data)} trouvés.")
    return articles_data

def connect_to_mongo_and_save_data(data: List[Dict]):
    if not data or not len(data):
        logger.info('Aucune données à insérer!')
    else:
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

        # Sauvegarde dans MongoDB
        save_scrapping = mongodb_client.save_scrapping_to_mongodb(data, 'investing_articles')

        # Fermeture des connexions
        mongodb_client.close_connections()

        if save_scrapping:
            logger.info("Sauvegarde du scrapping terminée avec succès\n")
        else:
            logger.error("Erreur lors de la sauvegarde\n")
            sys.exit(1)

def main():

    articles_data=scrap_pages()
    connect_to_mongo_and_save_data(articles_data)


if __name__ == "__main__":
    main()