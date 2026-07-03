import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "2. Objectifs",
    "Construire un pipeline crypto de bout en bout",
    "L’objectif n’est pas seulement de prédire : il faut collecter, historiser, enrichir, exposer et visualiser les données."
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
        "<strong>Ingestion</strong><br/>Récupérer automatiquement les candles, volumes, articles et signaux de sentiment.",
        "<strong>Structuration</strong><br/>Séparer données brutes, données nettoyées, features, labels et prédictions.",
        "<strong>Exploitation</strong><br/>Servir les données via FastAPI et les visualiser dans Streamlit."
    ])
with right:
    show_image("architecture_globale.png", "Vue d’ensemble du projet et de ses composants.")
