import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "Cryptobot OPA 2",
    "Promotion - sep25_continu_de",
    "Projet de fin d'études en Data Engineering - Liora Learn"
)

left, right = st.columns([0.9, 1.35], gap="large")
with left:
    bullets([
        "<strong>Groupe :</strong><br/>sep25_cde_opa2",
        "<strong>Equipe projet :</strong><br/>Ludovic LACORNE<br/>Ilyass MOULIF<br/>Alexandre NINASSI",
        "<strong>Encadrant :</strong><br/>Nicolas FRADIN",
        "<strong>Date de soutenance :</strong><br/>4 août 2026"
        ])
with right:
    show_image("cryptobot.jpg", "")
