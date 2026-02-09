from dotenv import load_dotenv
import os

load_dotenv()

# Generals
DB_NAME=os.getenv('DB_NAME')

DB_ROOT_USER=os.getenv('DB_ROOT_USER')
DB_ROOT_PASSWORD=os.getenv('DB_ROOT_PASSWORD')

DB_BOT_USER=os.getenv('DB_BOT_USER')
DB_BOT_PASSWORD=os.getenv('DB_BOT_PASSWORD')


# Mongo
MONGO_DB_PORT=os.getenv('MONGO_DB_PORT')
MONGO_HOST='localhost'
if os.getenv('ENV') == 'docker':
    MONGO_HOST=os.getenv('MONGO_HOST', 'mongo')


# PostgresSQL
PG_DB_PORT=os.getenv('PG_DB_PORT')
PG_HOST='localhost'
if os.getenv('ENV') == 'docker':
    PG_HOST=os.getenv('PG_HOST', 'postgres')
