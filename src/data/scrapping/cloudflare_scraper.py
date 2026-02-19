import cloudscraper

# Used to bypass cloudflare protection of investing.com
scraper = cloudscraper.create_scraper(
    browser={
        "browser": "chrome",
        "platform": "windows",
        "mobile": False
    }
)

scraper
