#!/bin/bash
echo "Lancement de l'extraction de données de Binance en temps réel (kline de BTCUSDT) pour tester streamlit ..."
echo -e "Aller voir Streamlit dans son conteneur en cliquant sur \e[34mhttp://localhost:8501\e[0m \n"

# Extraction de kline data en temps réel pour BTCUSDT avec un intervalle de 1 minute
python3 extract_kline_data_ws.py --symbol BTCUSDT --interval 1m