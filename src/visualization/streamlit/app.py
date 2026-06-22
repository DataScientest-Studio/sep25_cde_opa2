import streamlit as st

home_page=st.Page("home.py", title="Accueil", icon="💰")
klines_page=st.Page("market.py", title="Marché", icon="📈")
sentiment_page=st.Page("sentiment.py", title="Sentiment", icon="📊")
labels_page=st.Page("labels.py", title="Labels", icon="🏷️")
predictions_page=st.Page("predictions.py", title="Prédictions", icon="🎲")
api_page=st.Page("api.py", title="API", icon="🔌")

pg = st.navigation([home_page, klines_page, sentiment_page, labels_page, predictions_page, api_page])

pg.run()

