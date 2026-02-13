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

html = scraper.get(url).text

soup=bs(html, 'lxml')

articles_data=list()

container=soup.find('ul', {
    'data-test': 'news-list'
})
articles=container.find_all('article')

for article in articles:
    article_data=dict()

    # Get the title
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
