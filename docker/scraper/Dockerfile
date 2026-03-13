FROM mcr.microsoft.com/playwright/python:v1.58.0-noble

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN python -m playwright install --with-deps chromium

COPY src/ src/

# TODO: Adapter pour lancer automatiquement le scrapping en temps voulu.