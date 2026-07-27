import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "6. Machine learning",
    "Prédire la prochaine direction du marché",
    "Un modèle de classification supervisée transforme les indicateurs techniques en signaux de trading."
)

# ── Objectif : 3 classes ────────────────────────────────────────────────────
st.markdown(
    """
    <div class="metric-row">
        <div class="metric-box"><div class="metric-value"><span class="color-box" style="background:#00ff88"></span>&nbsp;&nbsp;+1</div><div class="metric-label"><strong>BUY</strong> — le prix va monter significativement</div></div>
        <div class="metric-box"><div class="metric-value"><span class="color-box" style="background:#aaaaaa"></span>&nbsp;&nbsp;0</div><div class="metric-label"><strong>HOLD</strong> — mouvement trop faible, ne rien faire</div></div>
        <div class="metric-box"><div class="metric-value"><span class="color-box" style="background:#ff4444"></span>&nbsp;&nbsp;−1</div><div class="metric-label"><strong>SELL</strong> — le prix va chuter significativement</div></div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ── Contenu en 2 colonnes ────────────────────────────────────────────────────
left, right = st.columns([1, 1], gap="large")

with left:
    bullets([
        "<strong>Étiquetage (labels) — Forward Return à horizon fixe</strong><br/>"
        "Pour chaque candle à l'instant <em>t</em> :"
        "<ul>"
        "<li>Calcul rendement futur : <code>r(t) = (close(t+N) − close(t)) / close(t)</code></li>"
        "<li>Étiquetage : "
            "<ul>"
                "<li>Si r(t) &gt; +θ → <strong>BUY (+1)</strong></li>"
                "<li>Si r(t) &lt; −θ → <strong>SELL (−1)</strong></li>"
                "<li>Si |r(t)| ≤ θ → <strong>HOLD (0)</strong></li>"
            "</ul>"
        "</li>"
        "</ul>",

        "<strong>Features d'entrée possibles (selon modèle)</strong><br/>"
        "<ul>"
        "<li>Marché :"
            "<ul>"
                "<li>OHLCV brutes : <code>open, high, low, close, volume</code></li>"
                "<li>RSI (14) — surachat / survente</li>"
                "<li>MACD + Signal MACD — tendance et accélération</li>"
                "<li>EMA (20 / 50 / 100) — moyennes mobiles multi-horizon</li>"
            "</ul>"
        "</li>"
        "<li>Sentiments (score, smooth, weighted, weighted_smooth)</li>"
        "</ul>",
    ])

with right:
    bullets([
        "<strong>Pipeline d'entraînement</strong><br/>"
        "<ul>"
        "<li>Découpage <strong>chronologique</strong> (pas de fuite de données)"
            "<ul>"
                "<li>Train : <strong>80 %</strong> des données les plus anciennes</li>"
                "<li>Test : <strong>20 %</strong> les plus récentes</li>"
            "</ul>"
        "</li>"
        "<li>Algorithme de classification : <code>HistGradientBoostingClassifier</code></li>"
        "<li><code>StandardScaler</code> sur toutes les features</li>"
        
        "<li><code>sample_weight</code> : rééquilibrage des classes pour compenser la majorité de labels <strong>HOLD</strong></li>"
        "<li>Plusieurs modèles entrainés</li>"
        "</ul>",
        "<strong>Mise en production</strong><br/>"
        "<ul>"
        "<li>Modèles sérialisés (<em>pickle</em>) dans <code>models/</code></li>"
        "<li>Calcul des prédictions à partir des dernières features depuis PostgreSQL</li>"
        "<li>Prédictions insérées dans la table <code>predictions</code></li>"
        "<li>Résultats visualisés dans le dashboard (page <em>Prédictions</em>)</li>"
        "</ul>",
    ])
