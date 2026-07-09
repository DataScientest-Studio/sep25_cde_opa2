"""
Tests pour le calcul des étiquettes (labels) d'apprentissage supervisé.
On teste la logique pure de compute_labels() sans base de données ni Docker.
"""
import pytest
import pandas as pd
from src.features.labels.compute_labels import compute_labels


def make_candles_from_prices(prices):
    """
    Crée un DataFrame de candles à partir d'une liste de prix de clôture.
    Pratique pour construire des scénarios de test précis et prévisibles.
    """
    n = len(prices)
    return pd.DataFrame({
        'id_candle': range(1, n + 1),
        'open_time': pd.date_range(start='2024-01-01', periods=n, freq='1h'),
        'close':     [float(p) for p in prices],
    })


def test_compute_labels_colonnes_presentes():
    # Après le calcul, le DataFrame doit contenir label_return et label_up_down
    df = make_candles_from_prices([100, 101, 102, 103, 104])
    result = compute_labels(df, horizon=1, threshold=0.01)

    assert 'label_return'  in result.columns
    assert 'label_up_down' in result.columns


def test_compute_labels_buy():
    # Le prix passe de 100 à 102 (+2%) sur l'horizon, au-dessus du seuil 1% → BUY (+1)
    df = make_candles_from_prices([100, 102])
    result = compute_labels(df, horizon=1, threshold=0.01)

    assert result.iloc[0]['label_up_down'] == 1
    assert result.iloc[0]['label_return'] == pytest.approx(0.02)


def test_compute_labels_sell():
    # Le prix passe de 100 à 98 (-2%) sur l'horizon, en dessous de -1% → SELL (-1)
    df = make_candles_from_prices([100, 98])
    result = compute_labels(df, horizon=1, threshold=0.01)

    assert result.iloc[0]['label_up_down'] == -1
    assert result.iloc[0]['label_return'] == pytest.approx(-0.02)


def test_compute_labels_hold():
    # Le prix passe de 100 à 100.5 (+0.5%), sous le seuil 1% → HOLD (0)
    df = make_candles_from_prices([100, 100.5])
    result = compute_labels(df, horizon=1, threshold=0.01)

    assert result.iloc[0]['label_up_down'] == 0


def test_compute_labels_dernieres_lignes_supprimees():
    # Les N dernières lignes n'ont pas de prix futur → elles doivent être supprimées
    # Avec horizon=2 et 5 candles, on doit récupérer 3 lignes valides (5 - 2)
    df = make_candles_from_prices([100, 101, 102, 103, 104])
    result = compute_labels(df, horizon=2, threshold=0.01)

    assert len(result) == 3


def test_compute_labels_calcul_return_correct():
    # Vérifie la formule : r(t) = (close(t+N) - close(t)) / close(t)
    # close(t)=100, close(t+2)=110 → r = (110-100)/100 = 0.10
    df = make_candles_from_prices([100, 105, 110, 120, 130])
    result = compute_labels(df, horizon=2, threshold=0.01)

    assert result.iloc[0]['label_return'] == pytest.approx(0.10)