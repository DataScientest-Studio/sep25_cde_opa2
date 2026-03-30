from pathlib import Path

from dotenv import load_dotenv
import os

load_dotenv()

# PATHS
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Generals
DB_NAME=os.getenv('DB_NAME')

DB_ROOT_USER=os.getenv('DB_ROOT_USER')
DB_ROOT_PASSWORD=os.getenv('DB_ROOT_PASSWORD')

DB_BOT_USER=os.getenv('DB_BOT_USER')
DB_BOT_PASSWORD=os.getenv('DB_BOT_PASSWORD')

ENV=os.getenv('ENV')

# Mongo
MONGO_DB_PORT=os.getenv('MONGO_DB_PORT')
MONGO_HOST='localhost'
if ENV == 'docker':
    MONGO_HOST=os.getenv('MONGO_HOST', 'mongo')

# PostgresSQL
PG_DB_PORT=os.getenv('PG_DB_PORT')
PG_HOST='localhost'
if ENV == 'docker':
    PG_HOST=os.getenv('PG_HOST', 'postgres')

# Binance api
BINANCE_API_KEY=os.getenv('BINANCE_API_KEY')
BINANCE_API_SECRET=os.getenv('BINANCE_API_SECRET')

# API
API_PORT=os.getenv('API_PORT', '8000')  # Port par défaut: 8000
API_HOST='localhost'
if ENV == 'docker':
    API_HOST=os.getenv('API_HOST', 'data-api')

# Scraper
SCRAPER_INDEX_LIMIT=os.getenv('SCRAPER_INDEX_LIMIT', 2)
SCRAPER_ENRICH_LIMIT=os.getenv('SCRAPER_ENRICH_LIMIT', 50)
SCRAPER_DETECT_LIMIT=os.getenv('SCRAPER_DETECT_LIMIT', 100)
