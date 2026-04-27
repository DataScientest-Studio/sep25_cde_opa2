#!/bin/bash

# Script pour collecter les données historiques Binance
# Ajouter/modifier les commandes selon les besoins

echo "Début de la collecte des données historiques Binance..."

# Calcul automatique du nombre de jours depuis le début
START_DATE="2026-01-01"
TODAY=$(date +%Y-%m-%d)
DAYS=$(( ( $(date -d "$TODAY" +%s) - $(date -d "$START_DATE" +%s) ) / 86400 + 1 ))


SYMBOLS=("BTCUSDT" "ETHUSDT")
INTERVALS=("1M" "1w" "1d" "1h" "5m" "1m")

for SYMBOL in "${SYMBOLS[@]}"; do
    for INTERVAL in "${INTERVALS[@]}"; do
        echo "Collecte des données $SYMBOL ($INTERVAL, $DAYS jours depuis $START_DATE)..."
        python -m src.data.binance.extract_klines_data --symbol $SYMBOL --start_date $START_DATE --interval $INTERVAL --days $DAYS --resume_from_last || true
    done
done

# Récupération des exchange_info de Binance (symbols)
echo "Récupération des informations sur les symboles Binance..."
python -m src.data.binance.extract_exchange_info_data || true

# On pourrait ajouter d'autres symboles ou intervalles si nécessaire ou même éditer les commandes ci-dessus 
# pour collecter des données en fonction de nos besoins spécifiques.

echo "Collecte des données historiques terminée."