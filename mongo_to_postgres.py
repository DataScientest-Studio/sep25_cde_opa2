from pymongo import MongoClient
import psycopg2
from datetime import datetime

# -------------------------
# Connexion Mongo
# -------------------------
mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["crypto"]
collection = mongo_db["klines"]

documents = collection.find()

# -------------------------
# Connexion PostgreSQL
# -------------------------
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="crypto_db",
    user="crypto_user",
    password="crypto_pass"  # adapte si besoin
)

cur = conn.cursor()

# -------------------------
# Récupérer id_symbol BTCUSDT
# -------------------------
cur.execute("SELECT id FROM symbols WHERE symbol = %s", ("BTCUSDT",))
result = cur.fetchone()

if not result:
    raise Exception(" error : BTCUSDT non trouvé dans table symbols")

id_symbol = result[0]

# -------------------------
# Insertion
# -------------------------
for doc in documents:
    open_time_ts = datetime.fromtimestamp(doc["open_time"] / 1000)

cur.execute("""
    INSERT INTO candles (
        id_symbol,
        interval,
        open_time,
        open,
        high,
        low,
        close,
        volume
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id_symbol, interval, open_time) DO NOTHING
""", (
    id_symbol,
    "1h",
    open_time_ts,
    doc["open"],
    doc["high"],
    doc["low"],
    doc["close"],
    doc["volume"]
))

conn.commit()
cur.close()
conn.close()

print("Done ––  Transfert Mongo → PostgreSQL terminé")
