FROM python:3.11-slim

# Installer les dépendances système nécessaires
# RUN apt-get update && apt-get install -y \
#     curl \
#     && rm -rf /var/lib/apt/lists/*

# Définir le répertoire de travail
WORKDIR /app

# Copier les requirements et installer les dépendances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le code source
COPY src/ src/

# Créer le répertoire de configuration Streamlit et copier la config
RUN mkdir -p /app/.streamlit
COPY docker/streamlit/config.toml /app/.streamlit/config.toml

# Exposer le port Streamlit
EXPOSE 8501

# Définir le répertoire de travail pour Streamlit
WORKDIR /app/src/visualization/streamlit

# Commande par défaut pour lancer Streamlit
CMD ["streamlit", "run", "klines_viewer.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]