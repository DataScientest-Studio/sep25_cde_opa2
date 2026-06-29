"""
Tests pour le calcul des indicateurs techniques (RSI, MACD, EMA).
On teste la logique pure sans base de données ni Docker.
"""
import pytest
import pandas as pd
import numpy as np
from src.features.compute_features import compute_indicators


def make_fake_candles(n=150):
    """
    Crée un DataFrame de candles fictives pour les tests.
    On a besoin d'au moins 100 lignes pour que l'EMA(100) puisse se calculer.
    """
    np.random.seed(42)  # Pour avoir des résultats reproductibles
    close_prices = 50000 + np.cumsum(np.random.randn(n) * 100)  # Prix simulés autour de 50000$

    return pd.DataFrame({
        'id_candle':  range(1, n + 1),
        'open_time':  pd.date_range(start='2024-01-01', periods=n, freq='1min'),
        'open':       close_prices + np.random.randn(n) * 10,
        'high':       close_prices + abs(np.random.randn(n)) * 20,
        'low':        close_prices - abs(np.random.randn(n)) * 20,
        'close':      close_prices,
        'volume':     abs(np.random.randn(n)) * 1000,
    })


def test_compute_indicators_colonnes_presentes():
    # Après le calcul, le DataFrame doit contenir les 5 nouvelles colonnes
    df = make_fake_candles()
    df = compute_indicators(df)

    assert 'rsi_14'  in df.columns
    assert 'macd'    in df.columns
    assert 'ema_20'  in df.columns
    assert 'ema_50'  in df.columns
    assert 'ema_100' in df.columns


def test_compute_indicators_rsi_entre_0_et_100():
    # Le RSI est toujours compris entre 0 et 100 par définition
    df = make_fake_candles()
    df = compute_indicators(df)

    rsi_values = df['rsi_14'].dropna()
    assert (rsi_values >= 0).all(), "RSI ne doit pas être négatif"
    assert (rsi_values <= 100).all(), "RSI ne doit pas dépasser 100"


def test_compute_indicators_ema_nan_sur_premieres_lignes():
    # Les premières lignes doivent être NaN car il n'y a pas assez de données
    # EMA(100) a besoin de 100 candles minimum
    df = make_fake_candles(n=150)
    df = compute_indicators(df)

    # Les 99 premières valeurs de ema_100 doivent être NaN
    assert df['ema_100'].iloc[:99].isna().all(), "Les 99 premières valeurs de EMA(100) doivent être NaN"
    # À partir de la 100ème, ema_100 doit avoir des valeurs
    assert df['ema_100'].iloc[99:].notna().any(), "EMA(100) doit avoir des valeurs après 100 candles"


def test_compute_indicators_pas_assez_de_donnees():
    # Avec seulement 10 candles, tous les indicateurs doivent être NaN
    # car il n'y a pas assez de données pour calculer quoi que ce soit
    df = make_fake_candles(n=10)
    df = compute_indicators(df)

    assert df['rsi_14'].isna().all(),  "RSI doit être NaN avec seulement 10 candles"
    assert df['ema_100'].isna().all(), "EMA(100) doit être NaN avec seulement 10 candles"


def test_compute_indicators_ne_modifie_pas_les_colonnes_originales():
    # Le calcul des indicateurs ne doit pas modifier les colonnes de prix originales
    df = make_fake_candles()
    close_original = df['close'].copy()

    df = compute_indicators(df)

    pd.testing.assert_series_equal(df['close'], close_original)
#    # On peut aussi vérifier que les autres colonnes (open, high, low) n'ont pas été modifiées