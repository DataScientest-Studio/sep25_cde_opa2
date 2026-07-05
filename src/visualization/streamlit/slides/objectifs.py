import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "2. Objectifs",
    "Construire un pipeline crypto de bout en bout",
    "L'objectif n'est pas seulement de prédire : il faut collecter, historiser, enrichir, exposer et visualiser les données."
)

st.markdown(
    """
    <div class="metric-row">
        <div class="metric-box"><div class="metric-value">1</div><div class="metric-label">Collecter les données marché et news</div></div>
        <div class="metric-box"><div class="metric-value">2</div><div class="metric-label">Transformer en features et labels</div></div>
        <div class="metric-box"><div class="metric-value">3</div><div class="metric-label">Restituer via API et dashboard</div></div>
    </div>
    """,
    unsafe_allow_html=True,
)

left, right = st.columns([1.1, 1.2], gap="large")
with left:
    bullets([
        "<strong>Ingestion</strong><br/>Collecter en continu les candles Binance via WebSocket et API REST, ainsi que les articles et signaux de sentiment liés au marché.",
        "<strong>Structuration</strong><br/>Organiser la donnée en couches distinctes — brute, nettoyée, features, labels et prédictions — pour garantir traçabilité et fiabilité.",
        "<strong>Exploitation</strong><br/>Rendre ces données actionnables en les exposant via FastAPI et en les visualisant dans un dashboard Streamlit interactif."
    ])
with right:
    show_image("architecture_globale.png", "Vue d'ensemble du projet et de ses composants.")