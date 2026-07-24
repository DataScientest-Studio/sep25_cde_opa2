import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "9. Limites & améliorations",
    "Rendre le pipeline plus robuste et industrialisable",
    "Le projet fonctionne comme preuve de concept avancée.<br />Les prochaines étapes seraient de mettre en place de la supervision et une qualité prédictive accrue."
)

bullets([
    "<strong>Données marchés</strong><br/>Récolter un historique plus conséquent afin d'avoir une qualité prédictive accrue.",
    "<strong>Scraping</strong><br/>Prévoir du monitoring et des alertes en cas de changement de structure HTML. Scraper de nouvelles sources via scrapping d'api.",
    "<strong>Orchestration</strong><br/>Continuer le remplacement progressif des scripts planifiés via Ofelia par Airflow.",
    "<strong>Modèles</strong><br/>Ajouter du backtesting, comparaison de modèles et métriques métier."
])