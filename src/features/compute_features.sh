#!/bin/bash

# Script pour calculer les features pour les modèles d'apprentissage supervisé
# Paramètres choisis pour les features :
#   - Symboles : BTCUSDT (ajouter d'autres symboles si besoin)
#   - Intervalles : 1m
#   - Limit : 50000 (nombre de candles max à charger pour le calcul des features)
#
python -m src.features.compute_features --symbol BTCUSDT --interval 1m --limit 50000