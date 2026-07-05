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
        "<strong>Un marché sans interruption</strong><br/>Le marché crypto tourne 24h/24 et 7j/7, impossible à surveiller en continu par un humain.",
        "<strong>Une volatilité extrême</strong><br/>Les prix peuvent varier de plusieurs pourcents en quelques minutes, rendant toute réaction manuelle trop lente.",
        "<strong>L'enjeu du Data Engineering</strong><br/>Construire un socle fiable, rejouable et exploitable avant toute approche prédictive."
    ])
with right:
    show_image("flux_donnees.png", "Du signal brut vers des indicateurs exploitables.")