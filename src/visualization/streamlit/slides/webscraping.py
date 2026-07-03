import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "04. Webscraping",
    "Transformer les actualités en données",
    "Le module de scraping indexe les articles, les enrichit progressivement, puis prépare les textes pour l’analyse de sentiment."
)

left, right = st.columns([1.05, 1.2], gap="large")
with left:
    bullets([
        "<strong>Indexation</strong><br/>Collecte des titres, liens, résumés, dates, fournisseurs et liens de commentaires.",
        "<strong>Enrichissement</strong><br/>Récupération du HTML complet, extraction du texte et détection des cryptomonnaies citées.",
        "<strong>Idempotence</strong><br/>Les champs <code>first_seen</code>, <code>last_seen</code> et <code>content_scraped</code> rendent le pipeline rejouable."
    ])
with right:
    show_image("sequence_scraping.png", "Séquence de scraping et d’enrichissement Investing.com.")
