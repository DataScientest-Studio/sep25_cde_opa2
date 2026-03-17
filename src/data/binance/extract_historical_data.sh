#!/bin/bash

# Script pour collecter les données historiques Binance
# Ajouter/modifier les commandes selon les besoins

echo "Début de la collecte des données historiques Binance..."

# Calcul automatique du nombre de jours depuis le début
START_DATE="2025-01-01"
TODAY=$(date +%Y-%m-%d)
DAYS=$(( ( $(date -d "$TODAY" +%s) - $(date -d "$START_DATE" +%s) ) / 86400 ))

# Collecte des données BTCUSDT - depuis 2025-01-01 jusqu'à aujourd'hui avec intervalle 1h
echo "Collecte des données BTCUSDT (1h, $DAYS jours depuis $START_DATE)..."
python -m src.data.binance.extract_klines_data --symbol BTCUSDT --start_date $START_DATE --interval 1h --days $DAYS

# Collecte des données BTCUSDT - 10 jours avec intervalle 1m
echo "Collecte des données BTCUSDT (1m, 10 jours)..."
python -m src.data.binance.extract_klines_data --symbol BTCUSDT --start_date $START_DATE --interval 1m --days 10

# Récupération des exchange_info de Binance (symbols)
echo "Récupération des informations sur les symboles Binance..."
python -m src.data.binance.extract_exchange_info_data

# On pourrait ajouter d'autres symboles ou intervalles si nécessaire ou même éditer les commandes ci-dessus 
# pour collecter des données en fonction de nos besoins spécifiques.

echo "Collecte des données historiques terminée."