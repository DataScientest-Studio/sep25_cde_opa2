import subprocess

print("Étape 1 : API → Mongo")
subprocess.run(["python", "test_fill_mongo.py"], check=True)

print("Étape 2 : Mongo → PostgreSQL")
subprocess.run(["python", "mongo_to_postgres.py"], check=True)

print("Pipeline complet terminé")
