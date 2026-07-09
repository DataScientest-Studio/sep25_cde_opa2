import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "6. Dashboard",
    "Explorer le pipeline de bout en bout",
    "Une application Streamlit multi-pages pour visualiser données de marché, sentiments, labels et prédictions du modèle ML."
)

# ── Pages de l'application ───────────────────────────────────────────────────
left, right = st.columns([1, 1], gap="large")

with left:
    bullets([
        "<strong>📈 Marché</strong><br/>"
        "<ul>"
        "<li>Graphique candlestick OHLCV + barre de volume synchronisée</li>"
        "<li>Données depuis MongoDB en direct ou PostgreSQL via l'API</li>"
        "</ul>",

        "<strong>📊 Sentiment</strong><br/>"
        "<ul>"
        "<li>Volume d'articles scrappés par jour et par crypto</li>"
        "<li>Score de sentiment agrégé quotidiennement (VADER / NLP)</li>"
        "<li>Superposition cours du marché + sentiment</li>"
        "<li>Matrice de corrélation rendements futurs vs sentiment</li>"
        "</ul>",
    ])

with right:
    bullets([
        "<strong>🏷️ Labels</strong><br/>"
        "<ul>"
        "<li>Répartition des 3 classes BUY / SELL / HOLD</li>"
        "<li>Histogramme des rendements futurs</li>"
        "<li>Prix de clôture annoté des 3 classes</li>"
        "<li>Courbe du rendement futur r(t)</li>"
        "</ul>",

        "<strong>🎲 Prédictions</strong><br/>"
        "<ul>"
        "<li>Prédictions ML superposées au prix de clôture</li>"
        "<li>Comparaison avec les labels réels (vérité terrain)</li>"
        "</ul>",
    ])

# ── Stack et points clés ─────────────────────────────────────────────────────
bullets([
    "<strong>Stack technique</strong><br/>"
    "<ul>"
    "<li><strong>Frontend :</strong> Streamlit (navigation multi-pages) + Plotly (candlesticks, scatter, camembert) + Paramètres dynamiques (symbole, intervalle, etc.)</li>"
    "<li><strong>Backend :</strong>"
    "<ul>"
    "<li>FastAPI (API REST) exposant PostgreSQL (candles, sentiments, features, labels, prédictions)</li>"
    "<li>MongoDB (klines Binance, articles investing.com)</li></ul></li>"
    "</ul>",
])
