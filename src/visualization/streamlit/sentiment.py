from datetime import datetime, date, timedelta

import pandas as pd
import requests
import streamlit as st

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.common.api import get_api_base_url
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
def load_sentiments_daily(start_date=None, end_date=None, limit=None, base_asset=None):
    """Récupère les données de sentiments pré-calculées quotidiennement depuis l'API PostgreSQL"""
    st.session_state.loading_data = True

    try:
        api_base_url = get_api_base_url()

        # Construction des paramètres de requête
        params = {"limit": limit, "base_asset": base_asset}

        # Ajout des filtres de date (Format YYYY-MM-DD attendu par l'API daily)
        if start_date:
            params["start_date"] = str(start_date)

        if end_date:
            params["end_date"] = str(end_date)

        # Appel à la route /scraping/sentiment/daily
        response = requests.get(f"{api_base_url}/scraping/sentiment/daily", params=params, timeout=30)

        if response.status_code != 200:
            st.error(f"Erreur API (status {response.status_code}): {response.text}")
            return pd.DataFrame()

        # Récupération des données JSON
        data = response.json()

        if not data:
            logger.warning("Aucune donnée reçue de l'API daily.")
            return pd.DataFrame()

        # Conversion en DataFrame
        df = pd.DataFrame(data)

        # Conversion de la colonne de date provenant de la table fsd
        if 'date_dg' in df.columns:
            df['date_dg'] = pd.to_datetime(df['date_dg'])

        logger.info(f"Récupération de {len(df)} lignes d'agrégations quotidiennes via l'API")
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

@st.cache_data(ttl=1)  
def load_candles(base_asset=None, interval="1d", start_date=None, end_date=None, limit=1000):
    """Récupère les candles (Données de marché)."""
    st.session_state.loading_data = True

    try:
        api_base_url = get_api_base_url()

        # Construction des paramètres de requête
        params = {"limit": limit, "symbol": f"{base_asset}USDT", "interval": interval}

        # Ajout des filtres de date si fournis
        if start_date:
            if isinstance(start_date, datetime):
                params["start_date"] = start_date.strftime("%Y-%m-%d %H:%M:%S")
            else:
                params["start_date"] = str(start_date)

        if end_date:
            if isinstance(end_date, datetime):
                params["end_date"] = end_date.strftime("%Y-%m-%d %H:%M:%S")
            else:
                params["end_date"] = str(end_date)

        # Appel à l'API
        response = requests.get(f"{api_base_url}/market/candles", params=params, timeout=30)

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
        if 'open_time' in df.columns:
            df['open_time'] = pd.to_datetime(df['open_time'])
        if 'close_time' in df.columns:
            df['close_time'] = pd.to_datetime(df['close_time'])

        # Conversion des colonnes numériques
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Sélection des colonnes nécessaires
        available_columns = [col for col in numeric_columns + ['open_time', 'symbol', 'interval'] if col in df.columns]
        df = df[available_columns]

        logger.info(f"Récupération de {len(df)} candles via l'API")
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

def showArticlesByDay(df: pd.DataFrame):
    """Affiche le volume d'articles quotidien"""
    fig = px.bar(
        df, 
        x='date_dg', 
        y='articles_volume', 
        color='base_asset',
        title="Volume d'articles par jour",
        labels={'articles_volume': 'Nombre d\'articles', 'date_dg': 'Date', 'base_asset': 'Crypto'},
    )

    st.subheader("Volume d'articles par jour")
    st.plotly_chart(fig)

def showMeanScoreByDay(df: pd.DataFrame):
    """Affiche le sentiment moyen quotidien"""
    fig = px.line(
        df, 
        x='date_dg', 
        y='sentiment_score', 
        color='base_asset',
        title="Évolution quotidienne du sentiment par Crypto",
        labels={'sentiment_score': 'Sentiment moyen (-1 à 1)', 'date_dg': 'Date'},
        markers=True
    )
    
    fig.add_hline(y=0, line_dash="dash", line_color="gray")

    st.subheader("Analyse Tendancielle")
    st.plotly_chart(fig) 

def showScoreVsPrice(symbol: str, df_sentiments: pd.DataFrame, df_klines: pd.DataFrame):
    """Superpose la courbe du sentiment lissé 3J avec le cours de clôture"""
    if df_klines.empty:
        return False

    # Fusion des dataframes sur la date commune
    merged_df = pd.merge(df_sentiments, df_klines, left_on='date_dg', right_on='open_time', how='inner')

    # Création de la figure double axe Y
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Utilisation directe du sentiment lissé pré-calculé en BDD
    fig.add_trace(
        go.Scatter(
            x=merged_df['date_dg'], 
            y=merged_df['sentiment_smooth'], 
            name="Sentiment (Lissé 3J)",
            line=dict(color='royalblue', width=2)
        ),
        secondary_y=False,
    )

    # Ajout du Prix Close (Axe Y droit)
    fig.add_trace(
        go.Scatter(
            x=merged_df['date_dg'], 
            y=merged_df['close'], 
            name=f"Prix {symbol}USDT",
            line=dict(color='firebrick', width=2)
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title_text=f"{symbol}USDT",
        hovermode="x unified"
    )

    fig.update_yaxes(title_text="Score Sentiment (-1 à 1)", secondary_y=False)
    fig.update_yaxes(title_text="Prix", secondary_y=True)

    st.subheader("Corrélation sentiment journalier et prix")
    st.plotly_chart(fig)

def showCorrelationMatrix(symbol: str, df_sentiments: pd.DataFrame, df_klines: pd.DataFrame):
    """Calcule et affiche la matrice de corrélation prédictive des rendements futurs"""
    if df_klines.empty:
        return False

    # Fusion des dataframes
    merged_df = pd.merge(df_sentiments, df_klines, left_on='date_dg', right_on='open_time', how='inner')

    # Tri temporel strict avant calcul des rendements
    merged_df = merged_df.sort_values('date_dg').reset_index(drop=True)

    # Rendement du jour J (% de variation du prix de clôture)
    merged_df['return_J'] = merged_df['close'].pct_change()

    # Génération automatique des rendements futurs de J+1 à J+7 via shift
    for i in range(1, 8):
        merged_df[f'return_J+{i}'] = merged_df['return_J'].shift(-i)

    # Définition propre des axes de la matrice de corrélation
    columns_for_predictive_return_vol_corr = [
        'sentiment_score',
        'sentiment_smooth',
        'sentiment_weighted',
        'sentiment_weighted_smooth',
        'return_J',
        'return_J+1',
        'return_J+2',
        'return_J+3',
        'return_J+4',
        'return_J+5',
        'return_J+6',
        'return_J+7'
    ]  

    # Sécurité pour éviter toute KeyError
    columns_return_vol_to_use = [col for col in columns_for_predictive_return_vol_corr if col in merged_df.columns]

    # Calcul de la matrice
    predictive_returns_vol_corr = merged_df[columns_return_vol_to_use].corr()

    # Création de la Heatmap Plotly
    fig_predictive_return_vol = px.imshow(
        predictive_returns_vol_corr,
        text_auto=".2f",
        aspect="auto",
        color_continuous_scale="RdBu_r",
        range_color=[-1, 1],
        title=f"Matrice de Corrélation Prédictive (Sentiment pré-calculé vs Rendements Futurs) pour {symbol}USDT"
    )    

    st.subheader("Analyse Prédictive du Sentiment")
    st.plotly_chart(fig_predictive_return_vol, use_container_width=True)
    

def main():
    st.header("Graphiques sur les tendances d'une crypto en fonction d'articles scrappés depuis le site investing.com", divider="gray")

    with st.spinner("Chargement des données..."):
        try:
            df_symbols = get_symbols()
        except Exception as e:
            st.error(f"Erreur lors du chargement: {e}")
            df_symbols = pd.DataFrame()
      
    if df_symbols.empty:
        st.warning(f"Aucuns symbols trouvés ou erreur de connexion.")
        return
    
    symbols = df_symbols['base_asset'].unique()
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
                # Chargement via l'API pré-calculée daily
                df_sentiments = load_sentiments_daily(start_date, end_date, base_asset=selected_symbols)
                df_klines_by_symbol = dict()
                for symbol in selected_symbols:
                    df_klines_by_symbol[symbol] = load_candles(base_asset=symbol, interval="1d", start_date=start_date, end_date=end_date)

            except Exception as e:
                st.error(f"Erreur lors du chargement: {e}")
                df_sentiments = pd.DataFrame()
        
        if df_sentiments.empty:
            st.warning(f"Aucunes données de sentiments trouvées ou erreur de connexion.")

        else: 
            # Graphique 1 : Volume d'articles
            showArticlesByDay(df=df_sentiments)

            # Graphique 2 : Tendance du score moyen
            showMeanScoreByDay(df=df_sentiments)

            # Boucle d'affichage pour les graphiques individuels par crypto (Prix vs Sentiment et Matrice)
            for symbol in selected_symbols:
                df_klines = df_klines_by_symbol.get(symbol, pd.DataFrame())
                # Extraction des lignes correspondantes à la crypto courante (sans modifier le DataFrame d'origine)
                symbol_daily_sentiment = df_sentiments[df_sentiments['base_asset'] == symbol]
                
                if not symbol_daily_sentiment.empty:
                    showScoreVsPrice(symbol=symbol, df_sentiments=symbol_daily_sentiment, df_klines=df_klines)
                    showCorrelationMatrix(symbol=symbol, df_sentiments=symbol_daily_sentiment, df_klines=df_klines)

    else:
        st.warning("Sélectionnez au moins une crypto et une date de début et de fin de période afin de générer les graphiques.")
   
if __name__ == "__main__":
    main()