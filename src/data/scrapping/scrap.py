import cloudscraper
from bs4 import BeautifulSoup as bs

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

container=soup.find('ul', {
    'data-test': 'news-list'
})
news=container.find_all('article')
print(len(news))
