import streamlit as st

# Démonstration
home_page       = st.Page("home.py",        title="Accueil",     icon="💰")
klines_page     = st.Page("market.py",      title="Marché",      icon="📈")
sentiment_page  = st.Page("sentiment.py",   title="Sentiment",   icon="📊")
labels_page     = st.Page("labels.py",      title="Labels",      icon="🏷️")
predictions_page= st.Page("predictions.py", title="Prédictions", icon="🎲")
api_page        = st.Page("api.py",         title="API",         icon="🔌")
grafana_page    = st.Page("grafana.py",     title="Grafana",     icon="👁️")

# Présentation
# Le choix des parties et de leur ordre est arbitraire, c'est à discuter
slide_problematique   = st.Page("slides/problematique.py",   title="1. Problématique",          icon="❓")
slide_objectifs       = st.Page("slides/objectifs.py",       title="2. Objectifs",              icon="🎯")
slide_binance         = st.Page("slides/binance.py",         title="3. Binance",                icon="🪙")
slide_webscraping     = st.Page("slides/webscraping.py",     title="4. Webscraping",            icon="🕷️")
slide_architecture    = st.Page("slides/architecture.py",    title="5. Architecture du projet", icon="🏗️")
slide_dashboard       = st.Page("slides/dashboard.py",       title="6. Dashboard",              icon="📊")
slide_ml              = st.Page("slides/machine_learning.py",title="7. Machine learning",       icon="🤖")
slide_tests           = st.Page("slides/tests_ci.py",        title="8. Tests unitaires et CI",  icon="✅")
slide_limites         = st.Page("slides/limites.py",         title="9. Limites et améliorations",icon="⚠️")
slide_conclusion      = st.Page("slides/conclusion.py",      title="10. Conclusion",            icon="🏁")

pg = st.navigation({
    "Démonstration": [home_page, klines_page, sentiment_page, labels_page, predictions_page, api_page, grafana_page],
    "Présentation": [
        slide_problematique, slide_objectifs, slide_binance, slide_webscraping,
        slide_architecture, slide_dashboard, slide_ml, slide_tests,
        slide_limites, slide_conclusion,
    ],
})

pg.run()

