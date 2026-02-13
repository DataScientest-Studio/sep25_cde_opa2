
#!/bin/bash
# Exemple de récupération de données de Binance pour le symbole BTCUSDT

# Récupération des données de Binance pour le symbole BTCUSDT sur une période de 30 jours à partir du 1er janvier 2024, avec un intervalle de 1 heure
python3 extract_klines_data.py --symbol BTCUSDT --start_date 2024-01-01 --interval 1h --days 30
# Récupération des données de Binance pour le symbole BTCUSDT sur une période de 10 jours à partir du 1er janvier 2024, avec un intervalle de 1 minute
python3 extract_klines_data.py --symbol BTCUSDT --start_date 2024-01-01 --interval 1m --days 3

# Récupération des exchange_info de Binance (symbols)
python3 extract_exchange_info_data.py 