FROM python:3.11-slim
ARG PYTHONPATH="."
ENV PYTHONPATH=$PYTHONPATH

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
WORKDIR /app

# Commande par défaut pour lancer Streamlit
CMD ["streamlit", "run", "src/visualization/streamlit/klines_viewer.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]