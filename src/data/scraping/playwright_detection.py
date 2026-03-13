import time
import random
from playwright.sync_api import sync_playwright, Browser, Page, BrowserContext, Playwright
from src.data.scraping.custom_logger import logger
from src.config import ENV

def human_sleep(sleep: int, msg: str):
    logger.info(f'{msg}: {sleep}')
    time.sleep(sleep)

def close_cookie_modal(page: Page):
    reject_cookie_btn = page.locator("#onetrust-reject-all-handler")
    if reject_cookie_btn.count() > 0 and reject_cookie_btn.first.is_visible():
        logger.info('Click sur reject cookie button')
        reject_cookie_btn.first.click()
        human_sleep(sleep=random.uniform(0.5, 1.5), msg="Attente humaine après fermeture de la popup cookies")   

def close_signup_modal(page: Page):
    signup_close_btn = page.locator('[data-test="sign-up-dialog-close-button"]')
    if signup_close_btn.count() > 0:
        logger.info('Click sur signup close button')
        signup_close_btn.click(force=True)
        human_sleep(sleep=random.uniform(0.5, 1.5), msg="Attente humaine après fermeture de la popup singup")

def init_playwright() -> tuple[Playwright, Browser, BrowserContext]:
    headless = True if ENV == "docker" else False  

    p = sync_playwright().start()
    
    browser = p.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--no-proxy-server"
        ]
    )

    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36",
        viewport={
            "width": random.randint(1200, 1920),
            "height": random.randint(800, 1080)
        },
        java_script_enabled=True,
        color_scheme="light",
        locale="fr-FR",
        timezone_id="Europe/Paris",
        storage_state=None
    )

    return p, browser, context

def close_playwright(p: Playwright, browser: Browser):
    if not browser:
        logger.error("Browser non défini!")
        return None 
    browser.close()
    p.stop()

def start_playwright_session() -> tuple[Playwright, Browser, Page]:
    p, browser, context = init_playwright()
    page = context.new_page()
    return p, browser, page


def get_html_with_playwright(page: Page, url: str, selector: str):

    max_retry = random.randint(2, 4)

    if not url or not selector:
        logger.error("Url ou selecteur non défini!")
        return None
    
    for attempt in range(max_retry):
        try:
            # human_sleep(sleep=random.uniform(5, 15), msg="Attente humaine")

            page.set_extra_http_headers({
                "Accept-Language": "fr-FR,fr;q=0.9"
            })

            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """)

            # Load page and waiting for DOM
            response = page.goto(url, wait_until="domcontentloaded", timeout=60000)

            if not response:
                continue

            logger.info(f"Statut de la page pour la tentative {attempt+1}: {response.status}")

            if response.status == 403:
                logger.warning("Page en 403, probable détection du scraping automatisé")

                # Wait long before retry
                human_sleep(sleep=random.uniform(60, 180), msg="Attente humaine avant retry")
                return None

            # Waiting for the element
            page.wait_for_selector(selector, timeout=30000)

            human_sleep(sleep=random.uniform(6, 20), msg="Attente humaine après chargement")

            # Natural human behaviors as mouse move, scroll
            # page.evaluate(
            #     "window.scrollTo(0, document.body.scrollHeight)"
            # )
            # human_sleep(sleep=random.uniform(5, 15), msg="Attente humaine après scroll")

            for _ in range(random.randint(2, 4)):
                page.mouse.wheel(
                    0,
                    random.randint(300, 800)
                )
                human_sleep(sleep=random.uniform(1, 2), msg="Attente humaine après mouse wheel")             

            for _ in range(random.randint(2, 5)):
                page.mouse.move(
                    random.randint(100, 1000),
                    random.randint(100, 800),
                    steps=random.randint(10, 30)
                )
                human_sleep(sleep=random.uniform(0.5, 1.5), msg="Attente humaine après mouse move")                      

            return page.content()

        except Exception as e:
            logger.error(str(e))
            human_sleep(sleep=random.uniform(30, 90), msg="Attente humaine après erreur")

    return None