
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import pandas as pd
import sys
import os
from datetime import datetime, timedelta
import logging

import streamlit as st
from streamlit_autorefresh import st_autorefresh
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Ajouter le répertoire src au path pour importer config
script_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.dirname(script_dir)  # src/data
src_dir = os.path.dirname(data_dir)     # src
sys.path.insert(0, src_dir)

try:
    from config import (DB_NAME, DB_BOT_USER, DB_BOT_PASSWORD, MONGO_DB_PORT, MONGO_HOST)
except ImportError as e:
    logging.error(f"Impossible d'importer config: {e}")
    logging.error("Assurez-vous que le fichier src/config.py existe et est accessible")
    sys.exit(1)


# Configuration de la page
st.set_page_config(
    page_title="Klines BTCUSDT Viewer", 
    page_icon="📈", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@st.cache_resource
def get_mongodb_connection():

    try:
        connection_string = f"mongodb://{DB_BOT_USER}:{DB_BOT_PASSWORD}@{MONGO_HOST}:{MONGO_DB_PORT}/{DB_NAME}"
        client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        return client
    except PyMongoError as e:
        st.error(f"Erreur de connexion MongoDB: {e}")
        return None
    except Exception as e:
        st.error(f"Erreur inattendue lors de la connexion: {e}")
        return None


@st.cache_data(ttl=2)
def load_klines_data(collection_name: str, start_date=None, end_date=None, limit=1000):
    
    st.session_state.loading_data = True
    
    try:
        if client is None:
            return pd.DataFrame()
        
        db = client[DB_NAME]
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
    
        return df
        
    except PyMongoError as e:
        st.error(f"Erreur lors de la récupération des données: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erreur inattendue: {e}")
        return pd.DataFrame()
    finally:
        st.session_state.loading_data = False


def create_candlestick_chart(df):

    if df.empty:
        return None
    
    # Graphique principal avec sous-graphique pour le volume
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=('Prix BTCUSDT', 'Volume'),
        row_width=[0.7, 0.3]
    )
    
    # Graphique candlestick
    fig.add_trace(
        go.Candlestick(
            x=df['open_time'],
            open=df['open_price'],
            high=df['high_price'],
            low=df['low_price'],
            close=df['close_price'],
            name="BTCUSDT",
            increasing_line_color='#00ff88',
            decreasing_line_color='#ff4444'
        ),
        row=1, col=1
    )
    
    # Graphique du volume
    colors = ['#00ff88' if close >= open else '#ff4444' 
              for close, open in zip(df['close_price'], df['open_price'])]
    
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
        title="Graphique BTCUSDT (1 minute)",
        yaxis_title="Prix (USDT)",
        yaxis2_title="Volume",
        template="plotly_dark",
        height=600,
        showlegend=False,
        xaxis_rangeslider_visible=False
    )
    
    fig.update_xaxes(title_text="Temps", row=2, col=1)
    
    return fig



# Initialisation de la connexion MongoDB : 1 seule connexion partagée
client = get_mongodb_connection()
# Paramètrage de l'auto-refresh
count = st_autorefresh(interval=10000, limit=100, key="fizzbuzzcounter")

def main():
    """Fonction principale de l'application Streamlit."""
    collection_name_default = "klines_BTCUSDT_1h"

    # Titre principal
    st.title("Visualisation Klines BTCUSDT")
    st.markdown("---")
    
    # Sidebar pour les contrôles
    st.sidebar.title("Paramètres")
    
    # Sélection de la collection
    collection_name = st.sidebar.text_input(
        "Collection MongoDB", 
        value=collection_name_default,
        help="Nom de la collection MongoDB contenant les données klines"
    )
    
    # Limite du nombre de points
    max_points = st.sidebar.slider(
        "Nombre max de points", 
        min_value=100, 
        max_value=5000, 
        value=1000,
        step=100,
        help="Limite le nombre de points affichés pour optimiser les performances"
    )
    
    # Bouton de rafraîchissement
    st.sidebar.button("Rafraîchir les données", type="primary")

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

    
    with st.spinner("Chargement des données..."):
        try:
            
            df = load_klines_data(collection_name, start_date, end_date, max_points)
        except Exception as e:
            st.error(f"Erreur lors du chargement: {e}")
            df = pd.DataFrame()
      
    if df.empty:
        st.warning("Aucune donnée trouvée dans la collection ou erreur de connexion.")
        

    st.subheader(f"Données de la collection: {collection_name}")
    st.subheader("Graphique Candlestick")
    
    fig = create_candlestick_chart(df)
    if fig:
        st.plotly_chart(fig, use_container_width=True)
    

if __name__ == "__main__":
    main()