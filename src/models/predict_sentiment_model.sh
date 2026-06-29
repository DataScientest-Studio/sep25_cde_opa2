#!/bin/bash

# Chargement des variables d'environnement
source .env

# Définition par défaut si non présent dans .env
MODEL_N_CANDLES=${MODEL_N_CANDLES:-100}

echo "========================================================"
echo "Début de la prédiction des modèles : $(date)"
echo "========================================================"

# Liste des configurations (Symbole:Seuil)
CONFIGS=("BTCUSDT:0.02" "ETHUSDT:0.02")
HORIZON="4"

for CONFIG in "${CONFIGS[@]}"; do
    # On sépare le symbole et le seuil
    SYMBOL="${CONFIG%%:*}"
    THRESHOLD="${CONFIG#*:}"

    echo "[INFO] Traitement : $SYMBOL | 1d | Horizon: $HORIZON j | Seuil: $THRESHOLD"
    
    # Appel du script python
    python -m src.models.predict_sentiment_model \
        --symbol "$SYMBOL" \
        --interval "1d" \
        --horizon "$HORIZON" \
        --threshold "$THRESHOLD" \
        --n_candles "$MODEL_N_CANDLES"
    
    # Gestion des erreurs
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] Prédiction pour $SYMBOL terminée."
    else
        echo "[ERROR] Échec pour $SYMBOL."
    fi
done

echo "========================================================"
echo "Fin du batch : $(date)"
echo "========================================================"