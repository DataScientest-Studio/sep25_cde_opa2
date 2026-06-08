#!/bin/bash

# Script pour calculer les prédictions du modèle entraîné avec les paramètres courants (.env) sur les données les plus récentes
python -m src.models.predict_model --symbol $MODEL_SYMBOL --interval $MODEL_INTERVAL --horizon $MODEL_HORIZON --threshold $MODEL_THRESHOLD --n_candles $MODEL_N_CANDLES --loop