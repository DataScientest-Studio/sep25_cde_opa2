#!/bin/bash

source .env

echo "Début de l'entrainement des models..."

SYMBOLS=("BTCUSDT" "ETHUSDT")

# --- 1d : décisions sur 4 jours de trading ---
# --- choix réalisés après lecteur de la matrice de corrélation entre le sentiment et l'évolution du prix du symbol
CONFIGS=("BTCUSDT:0.02" "ETHUSDT:0.02")
HORIZON="4"
for CONFIG in "${CONFIGS[@]}"; do
    # On sépare le symbole et le seuil
    SYMBOL="${CONFIG%%:*}"
    THRESHOLD="${CONFIG#*:}"

    echo "========================================================"
    echo "Entrainement des models : $SYMBOL | 1d | Horizon: $HORIZON j | Seuil: $THRESHOLD"
    echo "========================================================"
    
    python -m src.models.train_sentiment_model \
        --symbol "$SYMBOL" \
        --interval "1d" \
        --horizon "$HORIZON" \
        --threshold "$THRESHOLD" || true

done