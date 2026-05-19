import streamlit as st

home_page=st.Page("home.py", title="Accueil", icon="💰")
klines_page=st.Page("market.py", title="Marché", icon="📈")
sentiment_page=st.Page("sentiment.py", title="Sentiment", icon="📊")

pg = st.navigation([home_page, klines_page, sentiment_page])

pg.run()

