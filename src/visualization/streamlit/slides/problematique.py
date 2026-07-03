import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "1. Problématique",
    "Anticiper un marché crypto très volatil",
    "Le projet cherche à croiser données de marché et d'actualités pour produire une lecture exploitable des tendances crypto."
)

left, right = st.columns([0.95, 1.25], gap="large")
with left:
    bullets([
        "<strong>Un marché bruité</strong><br/>Les prix évoluent vite, avec une forte sensibilité aux annonces et au sentiment collectif.",
        "<strong>Des signaux hétérogènes</strong><br/>Candles, articles, sentiments et labels doivent être réunis dans un même pipeline.",
        "<strong>L'enjeu du Data Engineering</strong><br/>Construire un socle fiable, rejouable et exploitable avant toute approche prédictive."
    ])
with right:
    show_image("flux_donnees.png", "Du signal brut vers des indicateurs exploitables.")
