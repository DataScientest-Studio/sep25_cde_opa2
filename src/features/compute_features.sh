#!/bin/bash

# Script pour calculer les features pour les modèles d'apprentissage supervisé avec les paramètres courants (.env)
python -m src.features.compute_features --symbol $MODEL_SYMBOL --interval $MODEL_INTERVAL