#!/bin/bash

# Script pour calculer les prédictions du modèle entraîné sur les données les plus récentes
python -m src.models.predict_model --symbol BTCUSDT --interval 1h --horizon 24 --threshold 0.02 --n_candles 100

python -m src.models.predict_model --symbol BTCUSDT --interval 1m --horizon 30 --threshold 0.001 --n_candles 100