import streamlit as st
from src.visualization.streamlit.slides._common import apply_slide_style, slide_header, bullets, show_image

apply_slide_style()
slide_header(
    "08. Qualité logicielle",
    "Tests unitaires et CI/CD : deux niveaux de sécurisation",
    "La qualité du pipeline repose à la fois sur des tests ciblés du code métier et sur une automatisation des contrôles à chaque évolution."
)

# left, right = st.columns([1.18, 1.05], gap="large")
# with left:
st.markdown(
    """
    <div class="slide-card">
        <strong>Tests unitaires</strong><br/>
        Les tests vérifient que les traitements critiques produisent les résultats attendus : calcul de features,
        génération des labels, transformations de données et logique exposée par l’API. Ils permettent de détecter
        rapidement une régression sur une fonction isolée, sans relancer tout le pipeline.
    </div>
    <div class="slide-card">
        <strong>CI/CD</strong><br/>
        La chaîne GitHub Actions automatise les contrôles lors des modifications du code : installation de l’environnement,
        exécution des tests et validation de la cohérence du projet. Elle prépare aussi l’industrialisation future en
        posant les bases d’un déploiement plus automatisé des services.
    </div>
    <div class="slide-card">
        <strong>Traçabilité</strong><br/>
        Les logs communs complètent ces contrôles en documentant les volumes traités, les erreurs et les étapes critiques
        des scripts planifiés.
    </div>
    """,
    unsafe_allow_html=True,
)