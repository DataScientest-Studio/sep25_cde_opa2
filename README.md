Projet Crypto Bot
Formation Data Engineer
================================

Ce projet a pour objectif de construire un pipeline complet de Data Engineering autour des données de marché des cryptomonnaies.
Il collecte des données depuis Binance et Investing.com, stocke les données brutes dans MongoDB, transforme et charge les données utiles dans PostgreSQL, calcule des features et des labels, exécute des modèles de prédiction, expose les données via une API FastAPI et présente les résultats dans un dashboard Streamlit.

L'objectif principal est de fournir un environnement reproductible permettant d'ingérer, enrichir, transformer, analyser et visualiser des données crypto dans le cadre d'un cas d'usage de machine learning et d'analyse de sentiment.

Contributeurs
-------------------------------
Ludovic Lacorne
Ilyass Moulif
Alexandre Ninassi


Composants principaux du projet
-------------------------------

    ├── Collecte Binance             <- Récupération des données de marché historiques et temps réel.
    ├── Web scraping                 <- Collecte des actualités crypto depuis Investing.com.
    ├── MongoDB                      <- Stockage des données brutes et semi-structurées.
    ├── PostgreSQL                   <- Stockage structuré des features, labels et prédictions.
    ├── Feature engineering          <- Calcul des indicateurs techniques et des jeux de données supervisés.
    ├── Analyse de sentiment         <- Extraction du sentiment à partir des articles collectés.
    ├── Machine learning             <- Scripts d'entraînement et de prédiction.
    ├── FastAPI                      <- API exposant les données traitées et les prédictions.
    └── Streamlit                    <- Dashboard et interface de présentation du projet.

Prérequis
---------

Le projet est principalement conçu pour être lancé avec Docker Compose.

    ├── Docker
    ├── Docker Compose
    ├── Make
    ├── Bash
    └── Python 3.12.13               <- Utile pour les tests locaux hors Docker.

Avant de lancer le projet, créer les fichiers d'environnement à partir du fichier d'exemple :

    cp .env.sample .env.dev
    cp .env.sample .env.prod

Puis compléter les valeurs dans ces fichiers :

    ├── DB_ROOT_USER
    ├── DB_ROOT_PASSWORD
    ├── DB_ROOT_EMAIL
    ├── DB_BOT_USER
    ├── DB_BOT_PASSWORD
    ├── BINANCE_API_KEY
    ├── BINANCE_API_SECRET
    ├── DOCKER_REGISTRY
    ├── BUILD_CONTEXT_DEV
    └── Variables IMG_*

La configuration des serveurs pgAdmin doit également être créée à partir du fichier d'exemple :

    cp docker/pgadmin/.servers.sample docker/pgadmin/servers.json

Remplacer les valeurs d'exemple par les identifiants PostgreSQL correspondant au fichier d'environnement actif.

Initialiser les dossiers locaux et les dépendances Python
---------------------------------------------------------

Le projet utilise des dossiers locaux pour persister les données MongoDB, PostgreSQL et pgAdmin.
Ces dossiers sont créés sous :

    ~/DATAS/datascientest/projet/

Pour initialiser l'environnement local :

    make init

Cette commande lance le script `init.sh`, crée les dossiers locaux attendus, prépare un environnement virtuel Python et installe les dépendances nécessaires aux scripts locaux et au web scraping.

Lancer le projet en environnement de développement
--------------------------------------------------

L'environnement de développement est prévu pour construire les images Docker localement à partir du code source.
Il est adapté aux phases de développement, de test, de debug et de modification du code.

Configuration attendue dans `.env.dev` :

    DOCKER_REGISTRY=""
    BUILD_CONTEXT_DEV="."

Les noms d'images peuvent rester des images locales au projet, par exemple :

    IMG_API="cryptobot-data-api:latest"
    IMG_COLLECTOR="cryptobot-data-collector:latest"
    IMG_TRANSFORMER="cryptobot-data-transformer:latest"

Pour démarrer l'environnement de développement :

    make dev

Cette commande :

    ├── arrête les conteneurs existants
    ├── lie le fichier `.env` vers `.env.dev`
    ├── construit les images Docker localement
    └── démarre l'ensemble de la stack Docker Compose

Commandes utiles en développement :

    make status                         <- Affiche l'environnement actif et l'état des conteneurs.
    make down                           <- Arrête la stack Docker Compose.
    make up                             <- Démarre la stack Docker Compose.
    make rebuild                        <- Reconstruit toutes les images Docker sans cache.
    make rebuild_streamlit_image        <- Reconstruit uniquement l'image Streamlit.

Commandes manuelles utiles :

    docker compose logs -f
    docker compose logs -f scraper
    docker compose logs -f data-api
    docker compose exec scraper python -m src.data.scraping.index_articles
    docker compose exec scraper python -m src.data.scraping.enrich_articles
    docker compose exec scraper python -m src.data.scraping.detect_symbols
    docker compose exec data-transformer python -m src.data.binance.transform_and_load --incremental

Services accessibles localement :

    ├── Streamlit      <- http://localhost:8501
    ├── FastAPI        <- http://localhost:8000/docs
    ├── Mongo Express  <- http://localhost:8081
    └── pgAdmin        <- http://localhost:8080

Lancer le projet en environnement de production
-----------------------------------------------

L'environnement de production est prévu pour lancer des images Docker déjà construites, généralement poussées sur Docker Hub par GitHub Actions.
Il est adapté à une exécution plus stable du projet, sans reconstruction locale systématique des images.

Le workflow `.github/workflows/deploy.yml` construit et pousse les images de production lorsque du code est poussé sur la branche `master`.

Secrets GitHub requis :

    ├── DOCKERHUB_USERNAME
    └── DOCKERHUB_TOKEN

Configuration attendue dans `.env.prod` :

    DOCKER_REGISTRY="dockerhub_username/"
    BUILD_CONTEXT_DEV=""

Pour les services Python qui partagent l'image de production commune, utiliser :

    IMG_MONGO_INIT="cryptobot-python-base:latest"
    IMG_POSTGRES_INIT="cryptobot-python-base:latest"
    IMG_API="cryptobot-python-base:latest"
    IMG_COLLECTOR="cryptobot-python-base:latest"
    IMG_TRANSFORMER="cryptobot-python-base:latest"
    IMG_HISTORIC="cryptobot-python-base:latest"
    IMG_COMPUTE_LABELS="cryptobot-python-base:latest"
    IMG_COMPUTE_FEATURES="cryptobot-python-base:latest"
    IMG_MODELS_PREDICT="cryptobot-python-base:latest"
    IMG_MODELS_PREDICT_SENTIMENT="cryptobot-python-base:latest"

Les images dédiées sont également récupérées depuis le registre :

    ├── cryptobot-scraper:latest
    ├── cryptobot-sentiment:latest
    └── cryptobot-streamlit:latest

Pour démarrer l'environnement de production :

    make prod

Cette commande :

    ├── arrête les conteneurs existants
    ├── lie le fichier `.env` vers `.env.prod`
    ├── récupère les images Docker depuis le registre
    └── démarre la stack Docker Compose

Commandes utiles en production :

    make status
    docker compose ps
    docker compose logs -f

La persistance des données en production utilise les mêmes dossiers locaux qu'en développement :

    ~/DATAS/datascientest/projet/mongo_data
    ~/DATAS/datascientest/projet/postgresql_data
    ~/DATAS/datascientest/projet/pgadmin_data

Sous Linux, il peut être nécessaire d'ajuster les droits sur les dossiers PostgreSQL et pgAdmin :

    sudo chown -R 999:999 ~/DATAS/datascientest/projet/postgresql_data
    sudo chown -R 5050:5050 ~/DATAS/datascientest/projet/pgadmin_data

Planification des traitements
-----------------------------

Le projet utilise Ofelia comme planificateur de jobs Docker.
La configuration est définie dans le fichier `scheduler.ini`.

Les traitements planifiés comprennent :

    ├── Indexation des articles scrapés
    ├── Enrichissement des articles collectés
    ├── Détection des symboles crypto dans les articles
    ├── Exécution de l'analyse de sentiment
    ├── Transformation des données de marché MongoDB vers PostgreSQL
    ├── Calcul des features
    ├── Calcul des labels
    └── Exécution des scripts de prédiction

Le conteneur `scheduler` dispose d'un accès au socket Docker afin d'exécuter les commandes dans les conteneurs applicatifs.

Organisation du projet
----------------------

    ├── LICENSE
    ├── README.md          <- Fichier README principal pour les développeurs utilisant ce projet.
    ├── .env.sample        <- À copier en .env.dev et .env.prod, contient les variables d'environnement du projet.
    ├── data
    │   ├── external       <- Données issues de sources tierces.
    │   │   └── mapping_cryptos_symbol_name.json    <- Mapping entre symboles et noms de cryptomonnaies.
    │   ├── interim        <- Données intermédiaires déjà transformées.
    │   ├── processed      <- Jeux de données finaux et canoniques pour la modélisation.
    │   └── raw            <- Données brutes originales et immuables.
    │
    ├── docker             <- Configuration Docker.
    │   ├── pgadmin        <- Contient le fichier servers.json pour configurer les serveurs.
    │   │   └── .servers.sample    <- À renommer en servers.json et à compléter avec les valeurs de l'environnement actif.
    │   │
    │   ├── python         <- Contient le Dockerfile des conteneurs Python génériques.
    │   ├── scraper        <- Contient le Dockerfile du conteneur de scraping.
    │   ├── scraper-feature-sentiment <- Contient le Dockerfile du conteneur de sentiment.
    │   └── streamlit      <- Contient le Dockerfile du conteneur Streamlit.
    │
    ├── logs               <- Logs générés lors des entraînements et prédictions.
    │
    ├── models             <- Modèles entraînés et sérialisés, prédictions ou résumés de modèles.
    │
    ├── notebooks          <- Notebooks Jupyter. Convention de nommage : numéro d'ordre,
    │                         initiales du créateur et courte description séparée par des `-`, par exemple
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Dictionnaires de données, manuels et autres documents explicatifs.
    │
    ├── reports            <- Analyses générées au format HTML, PDF, LaTeX, etc.
    │   └── figures        <- Graphiques et figures générés pour les rapports.
    │
    ├── requirements.txt   <- Fichier global des dépendances pour reproduire l'environnement d'analyse.
    │
    ├── requirements       <- Dépendances séparées par service ou composant du projet.
    │   ├── api.txt
    │   ├── binance.txt
    │   ├── db.txt
    │   ├── features.txt
    │   ├── models.txt
    │   ├── scraper.txt
    │   ├── sentiment.txt
    │   └── streamlit.txt
    │
    ├── compose.yml        <- Fichier Docker Compose permettant de lancer les différents conteneurs du projet.
    │
    ├── scheduler.ini      <- Configuration du scheduler Ofelia.
    │
    ├── Makefile           <- Makefile permettant d'automatiser les différentes étapes du projet.
    │
    ├── src                <- Code source du projet.
    │   ├── __init__.py    <- Permet de considérer src comme un module Python.
    │   │
    │   ├── common         <- Connecteurs, logger et fonctions utilitaires partagés.
    │   │
    │   ├── data           <- Scripts de téléchargement, scraping, transformation ou exposition des données.
    │   │   ├── api        <- Application FastAPI exposant les données de marché, labels, prédictions et données scrapées.
    │   │   ├── binance
    │   │   │   ├── BinanceDataCollector.py    <- Classe de collecte des données Binance et sauvegarde dans MongoDB.
    │   │   │   ├── extract_exchange_info_data.py
    │   │   │   ├── extract_klines_data.py
    │   │   │   ├── extract_realtime_data.py
    │   │   │   ├── extract_kline_data_ws.py
    │   │   │   └── transform_and_load.py
    │   │   └── scraping
    │   │       ├── antibot.py             <- Détection anti-bot avec Playwright.
    │   │       ├── detect_symbols.py      <- Détection des symboles crypto dans le texte.
    │   │       ├── enrich_articles.py     <- Enrichissement des articles collectés.
    │   │       ├── index_articles.py      <- Collecte des articles depuis une source.
    │   │       ├── main.py                <- Lancement des scripts de scraping avec ProcessPoolExecutor.
    │   │       └── mongo_client.py        <- Client de connexion et sauvegarde MongoDB.
    │   │
    │   ├── features       <- Scripts transformant les données brutes en features pour la modélisation.
    │   │   ├── compute_features.py
    │   │   ├── compute_features.sh
    │   │   ├── labels
    │   │   │   ├── compute_labels.py
    │   │   │   └── compute_labels.sh
    │   │   └── scraping   <- Features de sentiment calculées à partir des articles scrapés.
    │   │
    │   ├── init           <- Scripts d'initialisation des bases de données.
    │   │   ├── init_mongo.py
    │   │   └── init_postgresql.py
    │   │
    │   ├── models         <- Scripts d'entraînement et d'utilisation des modèles pour produire des prédictions.
    │   │   ├── predict_model.py
    │   │   ├── predict_sentiment_model.py
    │   │   ├── train_model.py
    │   │   └── train_sentiment_model.py
    │   │
    │   ├── visualization  <- Dashboard Streamlit et pages de présentation du projet.
    │   │   └── streamlit
    │   │       ├── app.py
    │   │       ├── home.py
    │   │       ├── market.py
    │   │       ├── predictions.py
    │   │       ├── sentiment.py
    │   │       └── slides
    │   │
    │   └── config.py      <- Expose les variables d'environnement, importées par les autres scripts.
    │
    └── .github
        └── workflows      <- Workflows GitHub Actions.
--------
