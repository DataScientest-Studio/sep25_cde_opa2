import os
import time
from src.config import DB_NAME, DB_ROOT_USER, DB_ROOT_PASSWORD,MONGO_DB_PORT, DB_BOT_USER, DB_BOT_PASSWORD, MONGO_HOST
from pymongo import MongoClient, errors

mongo_db_uri=f'mongodb://{DB_ROOT_USER}:{DB_ROOT_PASSWORD}@{MONGO_HOST}:{MONGO_DB_PORT}/admin'

# Boucle de retry, pour attendre que mongodb soit bien démarré
# @TODO, regarder du coté de docker avec wait-for-it peut être ?
# Je ne connais pas encore assez pour le moment.
for i in range(10):
    try:
        client = MongoClient(mongo_db_uri, serverSelectionTimeoutMS=2000)
        client.admin.command("ping")
        print("Connection MongoDB réussie en tant que root")
        break
    except errors.ServerSelectionTimeoutError:
        print("MongoDB pas encore pret, retry dans 2s.")
        time.sleep(2)
else:
    raise Exception("Connecter à MongoDB ompossible.")

db=client[DB_NAME]
existing_users=db.command('usersInfo')

if not any(u["user"] == DB_BOT_USER for u in existing_users.get("users", [])):
    db.command(
        "createUser",
        DB_BOT_USER,
        pwd=DB_BOT_PASSWORD,
        roles=[{"role": "readWrite", "db": DB_NAME},
               {"role": "dbAdmin", "db": DB_NAME}],
    )
    print(f"Le user pour '{DB_NAME}' '{DB_BOT_USER}' est créé.")
else:
    print(f"Le user pour '{DB_NAME}' '{DB_BOT_USER}' existe déjà.")