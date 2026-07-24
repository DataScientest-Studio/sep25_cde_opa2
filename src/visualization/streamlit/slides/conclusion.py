import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "10. Conclusion",
    "Un socle Data Engineering complet",
    "Le projet met en place une chaîne cohérente allant de la donnée brute crypto jusqu’à la visualisation de signaux exploitables."
)

bullets([
    "<strong>Pipeline complet</strong><br/>Collecte, stockage, transformation, features, labels, prédictions et restitution.",
    "<strong>Architecture évolutive</strong><br/>Chaque module peut progresser indépendamment sans remettre en cause l’ensemble.",
    "<strong>Valeur projet</strong><br/>Une base solide pour poursuivre vers le backtesting, l’orchestration et l’industrialisation."
])
