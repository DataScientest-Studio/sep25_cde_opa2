FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m textblob.download_corpora

RUN python -c "from transformers import pipeline; \
    pipeline('sentiment-analysis', model='ProsusAI/finbert'); \
    pipeline('text-classification', model='j-hartmann/emotion-english-distilroberta-base')"

COPY src/ src/