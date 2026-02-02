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

# PostgresSQL
PG_DB_PORT=os.getenv('PG_DB_PORT')
