FROM python:3.11-slim

WORKDIR /app

COPY requirements/sentiment.txt requirements/sentiment.txt
RUN pip install --no-cache-dir -r requirements/sentiment.txt
RUN python -m textblob.download_corpora

RUN python -c "from transformers import pipeline; \
    pipeline('sentiment-analysis', model='ProsusAI/finbert'); \
    pipeline('text-classification', model='j-hartmann/emotion-english-distilroberta-base')"

COPY src/ src/