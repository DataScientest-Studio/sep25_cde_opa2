from datetime import datetime, date, timedelta

import pandas as pd
from pymongo.errors import PyMongoError
import requests
import streamlit as st

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.common.api import get_api_base_url
from src.common.connectors import MongoConnector
from src.common.custom_logger import logger

# Configuration de la page
st.set_page_config(
    page_title="Visualisation des sentiments calculés au sein d'actualités", 
    page_icon="📊", 
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data(ttl=1)
def get_symbols():
    """Retourne la liste des symbols disponibles"""
    st.session_state.loading_data = True

    try:
        api_base_url = get_api_base_url()

        # Appel à l'API
        response = requests.get(f"{api_base_url}/symbols", timeout=30)

        if response.status_code != 200:
            st.error(f"Erreur API (status {response.status_code}): {response.text}")
            return pd.DataFrame()

        # Récupération des données JSON
        data = response.json()

        if not data:
            logger.warning("Aucune donnée reçue de l'API.")
            return pd.DataFrame()

        # Conversion en DataFrame
        df = pd.DataFrame(data)

        # Filtrage sur les principales cryptos
        top_cryptos = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'DOGE', 'DOT', 'LINK', 'USDT', 'USDC']
        df_filtered = df[df['base_asset'].isin(top_cryptos)]

        return df_filtered
        
    except requests.exceptions.RequestException as e:
        st.error(f"Erreur de connexion à l'API: {e}")
        logger.error(f"Erreur de connexion à l'API: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erreur lors de la récupération des données via l'API: {e}")
        logger.error(f"Erreur lors de la récupération des données via l'API: {e}")
        return pd.DataFrame()
    finally:
        st.session_state.loading_data = False

@st.cache_data(ttl=1)  
def load_sentiments(start_date=None, end_date=None, limit=None, base_asset=None):
    """Récupère les données de sentiments"""
    st.session_state.loading_data = True

    try:
        api_base_url = get_api_base_url()

        # Construction des paramètres de requête
        params = {"limit": limit, "base_asset": base_asset}

        # Ajout des filtres de date si fournis
        if start_date:
            if isinstance(start_date, datetime):
                params["start_date"] = start_date.strftime("%Y-%m-%dT%H:%M:%S")
            else:
                params["start_date"] = str(start_date)

        if end_date:
            if isinstance(end_date, datetime):
                params["end_date"] = end_date.strftime("%Y-%m-%dT%H:%M:%S")
            else:
                params["end_date"] = str(end_date)

        # Appel à l'API
        response = requests.get(f"{api_base_url}/scraping/sentiment", params=params, timeout=30)

        if response.status_code != 200:
            st.error(f"Erreur API (status {response.status_code}): {response.text}")
            return pd.DataFrame()

        # Récupération des données JSON
        data = response.json()

        if not data:
            logger.warning("Aucune donnée reçue de l'API.")
            return pd.DataFrame()

        # Conversion en DataFrame
        df = pd.DataFrame(data)

        # Conversion des colonnes de dates
        if 'published_at' in df.columns:
            df['published_at'] = pd.to_datetime(df['published_at'])
        if 'created_at' in df.columns:
            df['created_at'] = pd.to_datetime(df['created_at'])

        # Mapping sur les sentiments
        sentiment_map = {'positive': 1, 'neutral': 0, 'negative': -1}
        df['sentiment_score'] = df['crypto_sentiment'].map(sentiment_map)

        logger.info(f"Récupération de {len(df)} sentiments via l'API")
        return df
        
    except requests.exceptions.RequestException as e:
        st.error(f"Erreur de connexion à l'API: {e}")
        logger.error(f"Erreur de connexion à l'API: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erreur lors de la récupération des données via l'API: {e}")
        logger.error(f"Erreur lors de la récupération des données via l'API: {e}")
        return pd.DataFrame()
    finally:
        st.session_state.loading_data = False

# @TODO: Cette fonction devra utiliser les données provenant d'une api postgres lorsqu'elle sera disponible.
# Elle devra également utiliser les klines ayant un interval de 24h lorsqu'elles seront récupérées.
@st.cache_data(ttl=1)
def load_klines_data_from_mongodb(collection_name: str, start_date=None, end_date=None, limit=1000):
    st.session_state.loading_data = True
    
    try:
        connector = MongoConnector().connect()
        if connector is None:
            return pd.DataFrame()
        
        db = connector.db
        collection = db[collection_name]
        
        # Construction de la requête
        start_date = datetime.combine(start_date, datetime.min.time())
        end_date = datetime.combine(end_date, datetime.max.time())

        query = {}
        if start_date and end_date:
            query['open_time'] = {
                '$gte': start_date,
                '$lte': end_date
            }
        
        # Récupération des données des limit derniers points
        cursor = collection.find(query).sort('open_time', -1).limit(limit)
        data = list(cursor)
        
        if not data:
            logger.warning("Aucune données trouvées")
            return pd.DataFrame()
        
        # Conversion en DataFrame
        df = pd.DataFrame(data)
        if 'open_time' in df.columns:
            df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')

        numeric_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df[numeric_columns + ['open_time']]
        df = df.rename(columns={
            'open_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'close_price': 'close'
        })

        return df
        
    except PyMongoError as e:
        st.error(f"Erreur lors de la récupération des données MongoDB: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erreur inattendue: {e}")
        return pd.DataFrame()
    finally:
        st.session_state.loading_data = False

def showArticlesByDay(df: pd.DataFrame):
    # Aggrégation et comptage le nombre d'articles par date et par crypto
    df_volume_articles = df.groupby(['date', 'base_asset']).size().reset_index(name='article_count')

    # Graphique en barres empilées
    fig = px.bar(
        df_volume_articles, 
        x='date', 
        y='article_count', 
        color='base_asset',
        title="Volume d'articles par jour",
        labels={'article_count': 'Nombre d\'articles', 'date': 'Date', 'base_asset': 'Crypto'},
    )

    st.subheader("Volume d'articles par jour")
    st.plotly_chart(fig)

def showMeanScoreByDay(df: pd.DataFrame) -> pd.DataFrame:
    # Agrégation et calcul de la moyenne du score par jour
    daily_sentiment = df.groupby(['base_asset', 'date'])['sentiment_score'].mean().reset_index()

    fig = px.line(
        daily_sentiment, 
        x='date', 
        y='sentiment_score', 
        color='base_asset',
        title="Évolution quotidienne du sentiment par Crypto",
        labels={'score': 'Sentiment moyen (-1 à 1)', 'date': 'Date'},
        markers=True
    )
    
    # Ajout d'une ligne horizontale à 0 pour repérer la neutralité
    fig.add_hline(y=0, line_dash="dash", line_color="gray")

    st.subheader("Analyse Tendancielle")
    st.plotly_chart(fig) 

    return daily_sentiment

def showScoreVsPrice(df_sentiments: pd.DataFrame, df_klines: pd.DataFrame):
    if df_klines.empty:
        return False

    # Convertion en objet datetime de la date.
    df_sentiments['date_dt'] = pd.to_datetime(df_sentiments['date'])

    # Aggregation des données klines sur une journée
    df_klines_daily = df_klines.resample('D', on='open_time').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).reset_index()

    # Merge des dataframes sentiments, et klines
    merged_df = pd.merge(df_sentiments, df_klines_daily, left_on='date_dt', right_on='open_time', how='inner')

    # Calcul de la moyenne mobile sur 3 jours pour lisser le sentiment
    merged_df['sentiment_smooth'] = merged_df['sentiment_score'].rolling(window=3, min_periods=1).mean()

    # Création de la figure avec deux axes Y
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Ajout de la courbe de Sentiment (Axe Y gauche)
    fig.add_trace(
        go.Scatter(
            x=merged_df['date_dt'], 
            y=merged_df['sentiment_smooth'], 
            name="Sentiment",
            line=dict(color='royalblue', width=2)
        ),
        secondary_y=False,
    )

    # Ajout de la courbe du Prix Close (Axe Y droit)
    fig.add_trace(
        go.Scatter(
            x=merged_df['date_dt'], 
            y=merged_df['close'], 
            name="Prix BTC (USDT)",
            line=dict(color='firebrick', width=2)
        ),
        secondary_y=True,
    )

    # Mise en page
    fig.update_layout(
        title_text="Sentiment Journalier vs Prix",
        hovermode="x unified"
    )

    # Noms des axes
    fig.update_yaxes(title_text="Score Sentiment (-1 à 1)", secondary_y=False)
    fig.update_yaxes(title_text="Prix", secondary_y=True)

    st.plotly_chart(fig)          
    

def main():
    st.header("Graphiques sur les tendances d'une crypto en fonction d'articles scrappés depuis le site investing.com", divider="gray")

    with st.spinner("Chargement des données..."):
        try:
            df_symbols=get_symbols()
        except Exception as e:
            st.error(f"Erreur lors du chargement: {e}")
            df_symbols = pd.DataFrame()
      
    if df_symbols.empty:
        st.warning(f"Aucuns symbols trouvés ou erreur de connexion.")
    
    symbols=df_symbols['base_asset'].unique()
    selected_symbols = st.sidebar.multiselect(
        "Sélectionner les cryptos à comparer",
        options=symbols,
        default=[symbol for symbol in symbols if symbol == 'BTC' ]
    )

    init_end_date = date.today()
    init_start_date = init_end_date - timedelta(days=30)

    dates = st.sidebar.date_input(
        "Période d'analyse d'articles",
        value=(init_start_date, init_end_date),
        min_value=date(2025, 1, 1),
        max_value=init_end_date
    )    

    if selected_symbols and len(dates) == 2:

        with st.spinner("Chargement des données..."):
            try:
                start_date, end_date = dates
                df_sentiments = load_sentiments(start_date, end_date, base_asset=selected_symbols)
                df_klines_1h = pd.DataFrame()
                if len(selected_symbols) == 1 and selected_symbols[0] == 'BTC':
                    df_klines_1h = load_klines_data_from_mongodb(collection_name="klines_BTCUSDT_1h", start_date=start_date, end_date=end_date)
            except Exception as e:
                st.error(f"Erreur lors du chargement: {e}")
                df_sentiments = pd.DataFrame()
        
        if df_sentiments.empty:
            st.warning(f"Aucunes données de sentiments trouvées ou erreur de connexion.")

        else: 
            # Convertion de la date en objet datetime, conservation de la date, mais pas de l'heure.
            df_sentiments['date'] = pd.to_datetime(df_sentiments['published_at']).dt.date

            # Affichage d'un graphique montrant le volume d'articles par jour.
            showArticlesByDay(df=df_sentiments)

            # Affichage d'un graphique montrant la moyenne du score de sentiment par jour
            daily_sentiment = showMeanScoreByDay(df=df_sentiments)

            # Affichage d'un graphique surperposant la tendance du score de sentiment vs le prix de la crypto
            # Ce graphique ne s'affiche que si la crypto possède des données de type klines.
            showScoreVsPrice(df_sentiments=daily_sentiment, df_klines=df_klines_1h)

    else:
        st.warning("Sélectionnez au moins une crypto et une date de début et de fin de période afin de générer les graphiques.")
   
if __name__ == "__main__":
    main()