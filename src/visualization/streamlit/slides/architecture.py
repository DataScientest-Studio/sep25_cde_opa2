import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "5. Architecture",
    "Une architecture modulaire et conteneurisée",
    "Le projet sépare ingestion, stockage, transformation, API, dashboard et planification."
)

left, right = st.columns([0.9, 1.35], gap="large")
with left:
    bullets([
        "<strong>Docker Compose</strong><br/>Les services techniques sont isolés et reproductibles.",
        "<strong>MongoDB & PostgreSQL</strong><br/>MongoDB conserve les données brutes alors que PostgreSQL conserve les données structurées pour l'entrainement, la prédiction, et la visualisation.",
        "<strong>FastAPI & Streamlit</strong><br/>L’API expose les données et Streamlit rend le projet lisible et démontrable au travers de différents dashboard.",
        "<strong>Airflow</strong><br/>L’orchestration automatise les traitements planifiés du projet afin de lancer les pipelines au bon moment, suivre leur état et identifier rapidement les échecs.",
        "<strong>Prometheus + Grafana</strong><br/>La supervision suit l’état de la machine, de FastAPI et de Docker afin d’identifier rapidement erreurs, lenteurs ou saturation."
    ])
with right:
    show_image("services_docker.jpg", "Organisation des principaux services applicatifs.")
