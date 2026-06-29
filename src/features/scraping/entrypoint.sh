#!/bin/sh

# STOP IMMEDIAT en cas d'erreur
set -e

# Force les logs en temps réel
export PYTHONUNBUFFERED=1

python -m src.features.scraping.sentiment
python -m src.features.scraping.sentiment_daily

