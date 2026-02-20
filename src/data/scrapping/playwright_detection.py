from playwright.sync_api import sync_playwright
from src.data.scrapping.custom_logger import logger

def get_html_with_playwright(url: str, selector: str):
    if not url or not selector:
        logger.error("Url ou selecteur non défini!")
    else: 
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()   

            # Charge la page et attend le DOM
            response = page.goto(url, wait_until="domcontentloaded", timeout=60000)
            if response:
                logger.info(f"Get page status: {response.status}")
            else:
                logger.error("No response from url!")

            # Attend que l'élement désiré soit présent.
            page.wait_for_selector(selector, timeout=30000)

            html = page.content()
            browser.close()
    
    return html