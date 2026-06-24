#!/bin/bash

# Script pour calculer les labels d'apprentissage supervisé
# sur différentes combinaisons de symboles, intervalles, horizons et seuils.
# Ajouter/modifier les combinaisons selon les besoins.
#
# Usage : bash src/features/labels/compute_labels.sh
#
# Les paramètres horizon et threshold sont choisis en tentant d'être cohérents avec chaque intervalle :
#   1d  → fenêtre de 4 jours     seuil 2%
#   1h  → fenêtre de 12-24h,     seuil 1-2%
#   5m  → fenêtre de 1-2h,       seuil 0.3-0.5%
#   1m  → fenêtre de 30-60 min,  seuil 0.1-0.3%
#
# Chaque combinaison génère une version indépendante en base (table `labels`).

echo "Début du calcul des labels..."

SYMBOLS=("BTCUSDT" "ETHUSDT")

# --- 1d : décisions sur 4 jours de trading ---
# --- choix réalisés après lecture de la matrice de corrélation entre le sentiment et l'évolution du prix du symbol
CONFIGS=("BTCUSDT:0.02" "ETHUSDT:0.02")
HORIZON="4"
for CONFIG in "${CONFIGS[@]}"; do
    # On sépare le symbole et le seuil
    SYMBOL="${CONFIG%%:*}"
    THRESHOLD="${CONFIG#*:}"

    echo "========================================================"
    echo "Calcul labels : $SYMBOL | 1d | Horizon: $HORIZON j | Seuil: $THRESHOLD"
    echo "========================================================"
    
    python -m src.features.labels.compute_labels \
        --symbol "$SYMBOL" \
        --interval "1d" \
        --horizon "$HORIZON" \
        --threshold "$THRESHOLD" || true

done

# --- 1h : décisions sur 12 à 24 heures (référence principale) ---
for SYMBOL in "${SYMBOLS[@]}"; do
    for HORIZON in 12 24; do
        for THRESHOLD in 0.01 0.02; do
            echo "Calcul labels : $SYMBOL | interval=1h | horizon=$HORIZON h | seuil=$THRESHOLD ..."
            python -m src.features.labels.compute_labels \
                --symbol $SYMBOL --interval 1h \
                --horizon $HORIZON --threshold $THRESHOLD || true
        done
    done
done

# --- 5m : décisions sur 1 à 2 heures ---
for SYMBOL in "${SYMBOLS[@]}"; do
    for HORIZON in 12 24; do
        for THRESHOLD in 0.003 0.005; do
            echo "Calcul labels : $SYMBOL | interval=5m | horizon=$HORIZON candles ($(( HORIZON * 5 )) min) | seuil=$THRESHOLD ..."
            python -m src.features.labels.compute_labels \
                --symbol $SYMBOL --interval 5m \
                --horizon $HORIZON --threshold $THRESHOLD || true
        done
    done
done

# --- 1m : décisions sur 30 à 60 minutes (scalping) ---
for SYMBOL in "${SYMBOLS[@]}"; do
    for HORIZON in 30 60; do
        for THRESHOLD in 0.001 0.003; do
            echo "Calcul labels : $SYMBOL | interval=1m | horizon=$HORIZON min | seuil=$THRESHOLD ..."
            python -m src.features.labels.compute_labels \
                --symbol $SYMBOL --interval 1m \
                --horizon $HORIZON --threshold $THRESHOLD || true
        done
    done
done

# Script pour calculer les labels avec les paramètres courants (.env) sur les données les plus récentes
# python -m src.features.labels.compute_labels --symbol $MODEL_SYMBOL --interval $MODEL_INTERVAL --horizon $MODEL_HORIZON --threshold $MODEL_THRESHOLD --loop || true