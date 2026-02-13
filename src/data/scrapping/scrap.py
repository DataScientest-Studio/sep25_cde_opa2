import cloudscraper
from bs4 import BeautifulSoup as bs
from datetime import datetime

# Used to bypass cloudflare protection of investing.com
scraper = cloudscraper.create_scraper(
    browser={
        "browser": "chrome",
        "platform": "windows",
        "mobile": False
    }
)

url='https://www.investing.com/news/cryptocurrency-news'

list_page_html = scraper.get(url).text

list_page_soup=bs(list_page_html, 'lxml')

articles_data=list()

container=list_page_soup.find('ul', {
    'data-test': 'news-list'
})
articles=container.find_all('article')

for article in articles:
    article_data=dict()

    # Get the title and the complete article link
    title_elm=article.find('a', {
            'data-test': 'article-title-link'
        })
    if title_elm:
        article_data['title']=title_elm.text
        article_data['link_to_article']=title_elm.get('href')
        
        # Get the complete article
        article_complete_html = scraper.get(article_data['link_to_article']).text
        article_complete_soup=bs(article_complete_html, 'lxml')
        article_complete_content=article_complete_soup.find(id='article')
        if article_complete_content:
             article_data['raw_content']=article_complete_content
             article_data['text_content']=article_complete_content.text

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
        article_data['published_at_timestamp']=datetime.fromisoformat(published_at.get('datetime')).timestamp()

    # Get the provider name
    provider=article.find('span', {
        'data-test': 'news-provider-name'
    })
    if provider:
        article_data['provider']=provider.text

    # Get comments page link
    summary_footer_elm=article.find('ul').find_all('li')
    if len(summary_footer_elm) > 1:
        comments_link=summary_footer_elm[1]
        if comments_link.find('a') and comments_link.find('a').get('href'):
            article_data['link_to_comments']=comments_link.find('a').get('href')
    
    
    articles_data.append(article_data)

# @TODO 
# Get informations around the article, as crypto tendances, or some keywords, the author, ...
# This informations are not in the main article div.
# => Inspect the code and find the id or class to identify these elements.