from pymongo.errors import PyMongoError
import pandas as pd
from datetime import date, datetime, timedelta
import requests

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.common.api import get_api_base_url
from src.config import API_PORT, API_HOST
from src.common.connectors import MongoConnector
from src.common.custom_logger import logger

# Configuration de la page
st.set_page_config(
    page_title="Candles BTCUSDT Viewer", 
    page_icon="📈", 
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def get_mongodb_connection():
    try:
        return MongoConnector().connect()
    except SystemExit:
        st.error("Erreur de connexion MongoDB")
        return None


@st.cache_data(ttl=1)
def load_klines_data_mongodb(collection_name: str, start_date=None, end_date=None, limit=1000):
    st.session_state.loading_data = True
    
    try:
        connector = get_mongodb_connection()
        if connector is None:
            return pd.DataFrame()
        
        db = connector.db
        collection = db[collection_name]
        
        # Construction de la requête
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
            logger.warning("No data found for the given query.")
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


@st.cache_data(ttl=1)  
def load_candles_data_api(start_date=None, end_date=None, limit=1000, interval="1m", symbol="BTCUSDT"):
    """Récupère les données candles via l'API FastAPI"""
    st.session_state.loading_data = True

    try:
        api_base_url = get_api_base_url()

        # Construction des paramètres de requête
        params = {"limit": limit, "interval": interval, "symbol": symbol}

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
            logger.warning("Aucune donnée reçue de l'API (table 'candles').")
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
        st.error(f"Erreur lors de la récupération des données via l'API (table 'candles'): {e}")
        logger.error(f"Erreur lors de la récupération des données via l'API (table 'candles'): {e}")
        return pd.DataFrame()
    finally:
        st.session_state.loading_data = False


def create_candlestick_chart(df, symbol="BTCUSDT", interval="1m"):
    if df.empty:
        return None
    
    # Graphique principal avec sous-graphique pour le volume
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=(f'Prix {symbol}', 'Volume'),
        row_width=[0.7, 0.3]
    )
    
    # Graphique candlestick
    fig.add_trace(
        go.Candlestick(
            x=df['open_time'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            name=symbol,
            increasing_line_color='#00ff88',
            decreasing_line_color='#ff4444'
        ),
        row=1, col=1
    )
    
    # Graphique du volume
    colors = ['#00ff88' if c >= o else '#ff4444'
              for c, o in zip(df['close'], df['open'])]
    
    fig.add_trace(
        go.Bar(
            x=df['open_time'],
            y=df['volume'],
            name="Volume",
            marker_color=colors,
            opacity=0.7
        ),
        row=2, col=1
    )
    
    # Mise en forme
    fig.update_layout(
        title=f"Graphique {symbol} ({interval})",
        yaxis_title="Prix (USDT)",
        yaxis2_title="Volume",
        template="plotly_dark",
        height=600,
        showlegend=False,
        xaxis_rangeslider_visible=False
    )
    
    fig.update_xaxes(title_text="Temps", row=2, col=1)
    
    return fig

@st.fragment(run_every="3s")
def show_candlestick_chart(data_source: str, collection_name: str, symbol: str, interval: str, start_date: date, end_date: date, max_points: int):
    with st.spinner("Chargement des données..."):
        try:
            if data_source == "API PostgreSQL":
                df = load_candles_data_api(start_date, end_date, max_points, interval, symbol)
            else:  # MongoDB
                df = load_klines_data_mongodb(collection_name, start_date, end_date, max_points)
        except Exception as e:
            st.error(f"Erreur lors du chargement: {e}")
            df = pd.DataFrame()
      
    if df.empty:
        st.warning(f"Aucune donnée trouvée dans la source {data_source} ou erreur de connexion.")
        

    source_info = "table PostgreSQL 'candles' (via API)" if data_source == "API PostgreSQL" else f"collection MongoDB: {collection_name}"
    st.subheader(f"Données de la {source_info}")
    name_display = "candles" if data_source == "API PostgreSQL" else collection_name
    st.info(f"Source active: **{data_source}** | Nom: **{name_display}** | Nombre de points chargés: **{len(df)}**")
    st.subheader("Graphique Candlestick")
    
    fig = create_candlestick_chart(df, symbol, interval)
    if fig:
        st.plotly_chart(fig, width="stretch")    

def main():
    """Fonction principale de l'application Streamlit"""

    # Titre principal
    st.header("Visualisation Candles", divider="gray")
    
    # Sidebar pour les contrôles
    st.sidebar.title("Paramètres")
    
    # Sélection de la source de données
    data_source = st.sidebar.selectbox(
        "Source de données",
        ["API PostgreSQL", "MongoDB"],
        index=0,
        help="Choisir entre l'API FastAPI (données PostgreSQL) ou MongoDB directement"
    )
    
    # Test de connexion API si sélectionnée
    if data_source == "API PostgreSQL":
        api_url = get_api_base_url()
        with st.sidebar.expander("ℹ️ Info API", expanded=False):
            st.write(f"**URL API:** {api_url}")
            if st.button("Tester la connexion API"):
                try:
                    response = requests.get(f"{api_url}/health", timeout=5)
                    if response.status_code == 200:
                        st.success("✅ API disponible")
                    else:
                        st.error(f"❌ API erreur: {response.status_code}")
                except Exception as e:
                    st.error(f"❌ API inaccessible: {e}")
    
    # Configuration selon la source
    collection_name = None
    symbol = "BTCUSDT"
    interval = "1m"
    if data_source == "API PostgreSQL":
        symbol = st.sidebar.selectbox(
            "Symbole",
            options=["BTCUSDT", "ETHUSDT"],
            index=0,
            help="Symbole de trading à afficher"
        )
        interval = st.sidebar.selectbox(
            "Intervalle",
            options=["1m", "5m", "1h", "1d", "1w", "1M"],
            index=0,
            help="Intervalle des candles"
        )
        st.sidebar.info("Source : table **candles** (PostgreSQL via API)")
    else:  # MongoDB
        collection_name = st.sidebar.text_input(
            "Nom de la collection MongoDB",
            value=f"klines_{symbol}_1m_ws",
            help="Nom de la collection MongoDB contenant les données klines"
        )
    
    # Limite du nombre de points
    max_points = st.sidebar.slider(
        "Nombre max de points", 
        min_value=1,
        max_value=1000,
        value=10,
        step=1,
        help="Limite le nombre de points affichés pour optimiser les performances"
    )
    
    # Bouton de rafraîchissement
    # st.sidebar.button("Rafraîchir les données", type="primary")

    # Filtres temporels
    use_date_filter = st.sidebar.checkbox("Activer le filtre par date", value=False)
    start_date = None
    end_date = None
    
    if use_date_filter:
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input(
                "Date début",
                value=datetime.now().date() - timedelta(days=1)
            )
        with col2:
            end_date = st.date_input(
                "Date fin",
                value=datetime.now().date()
            )
        # Conversion en datetime
        start_date = datetime.combine(start_date, datetime.min.time())
        end_date = datetime.combine(end_date, datetime.max.time())

    
    # Affichage du graphique des candles
    show_candlestick_chart(data_source, collection_name, symbol, interval, start_date, end_date, max_points)

if __name__ == "__main__":
    main()