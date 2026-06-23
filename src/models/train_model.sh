#!/bin/bash

source .env
# Script pour entraîner les modèles d'apprentissage supervisé sur les données de features et labels calculées avec les paramètres courants (.env)
python -m src.models.train_model --symbol $MODEL_SYMBOL --interval $MODEL_INTERVAL --horizon $MODEL_HORIZON --threshold $MODEL_THRESHOLD --train_from 2026-01-01 --train_until 2026-06-01