import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "3. Binance",
    "La source de données du projet",
    "Binance met à disposition des API publiques et gratuites qui alimentent l'ensemble de notre pipeline."
)

st.markdown(
    """
    <div class="metric-row">
        <div class="metric-box"><div class="metric-value">📊</div><div class="metric-label">Klines / Candlesticks</div></div>
        <div class="metric-box"><div class="metric-value">📈</div><div class="metric-label">Ticker 24h</div></div>
        <div class="metric-box"><div class="metric-value">📖</div><div class="metric-label">Order Book</div></div>
    </div>
    """,
    unsafe_allow_html=True,
)

left, right = st.columns([1.1, 1.2], gap="large")
with left:
    bullets([
        "<strong>Klines (Candlesticks)</strong><br/>Bougies OHLCV sur différents intervalles (1m, 5m, 1h, 1d...), base de tous nos indicateurs techniques.",
        "<strong>WebSocket</strong><br/>Flux temps réel qui pousse chaque nouvelle bougie dès sa clôture, pour une collecte en continu.",
        "<strong>API REST</strong><br/>Requêtes ponctuelles pour récupérer l'historique et initialiser la base avec plusieurs mois de données."
    ])
with right:
    show_image("binance.jpg", "Sources binance.")
