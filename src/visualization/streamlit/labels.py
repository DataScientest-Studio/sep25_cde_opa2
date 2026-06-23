from datetime import datetime, timedelta

import pandas as pd
import requests
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from src.common.api import get_api_base_url
from src.common.custom_logger import logger

# Configuration de la page
st.set_page_config(
    page_title="Visualisation des labels",
    page_icon="🏷️",
    layout="wide",
    initial_sidebar_state="expanded"
)

LABEL_COLORS = {-1: "#ff4444", 0: "#aaaaaa", 1: "#00ff88"}
LABEL_NAMES  = {-1: "SELL (-1)", 0: "HOLD (0)", 1: "BUY (+1)"}


@st.cache_data(ttl=60)
def get_available_params(symbol: str, interval: str):
    """Retourne les combinaisons (horizon, threshold) disponibles via l'API pour ce symbol/interval."""
    try:
        api_base_url = get_api_base_url()
        response = requests.get(
            f"{api_base_url}/labels/params",
            params={"symbol": symbol, "interval": interval},
            timeout=10,
        )
        if response.status_code != 200:
            logger.error(f"Erreur API /labels/params (status {response.status_code}): {response.text}")
            return []
        data = response.json()
        return [(item["horizon"], item["threshold"]) for item in data]
    except Exception as e:
        logger.error(f"Erreur get_available_params: {e}")
        return []


@st.cache_data(ttl=60)
def load_labels(symbol: str, interval: str, horizon: int, threshold: float, start_date=None, end_date=None, limit: int = 2000):
    try:
        api_base_url = get_api_base_url()
        params = {
            "symbol": symbol,
            "interval": interval,
            "horizon": horizon,
            "threshold": threshold,
            "limit": limit,
        }
        if start_date:
            params["start_date"] = start_date.strftime("%Y-%m-%d %H:%M:%S") if isinstance(start_date, datetime) else str(start_date)
        if end_date:
            params["end_date"] = end_date.strftime("%Y-%m-%d %H:%M:%S") if isinstance(end_date, datetime) else str(end_date)

        response = requests.get(f"{api_base_url}/labels", params=params, timeout=30)
        if response.status_code != 200:
            st.error(f"Erreur API /labels (status {response.status_code}): {response.text}")
            return pd.DataFrame()

        data = response.json()
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["label_return"] = df["label_return"].astype(float)
        df["label_up_down"] = df["label_up_down"].astype(int)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

        df = df.sort_values("timestamp").reset_index(drop=True)
        logger.info(f"{len(df)} labels chargés pour {symbol}")
        return df

    except requests.exceptions.RequestException as e:
        st.error(f"Erreur de connexion à l'API: {e}")
        logger.error(f"Erreur load_labels (connexion): {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erreur lors du chargement des labels: {e}")
        logger.error(f"Erreur load_labels: {e}")
        return pd.DataFrame()


def chart_distribution(df: pd.DataFrame):
    counts = df["label_up_down"].value_counts().sort_index()
    labels = [LABEL_NAMES[k] for k in counts.index]
    colors = [LABEL_COLORS[k] for k in counts.index]

    fig = go.Figure(go.Pie(
        labels=labels,
        values=counts.values,
        marker=dict(colors=colors),
        hole=0.4,
        textinfo="label+percent+value",
        textposition="inside",
        insidetextorientation="horizontal"
    ))
    fig.update_layout(
        title="Distribution des classes",
        template="plotly_dark",
        height=550
    )
    return fig


def chart_labels_over_time(df: pd.DataFrame, symbol: str):
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(f"Prix de clôture + labels — {symbol}", "Rendement futur r(t)"),
        row_width=[0.35, 0.65]
    )

    # Courbe du prix
    if df["close"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["close"],
                mode="lines",
                line=dict(color="#888888", width=1),
                name="Close",
                showlegend=True
            ),
            row=1, col=1
        )

    # Points colorés par label
    for label_val, label_name in LABEL_NAMES.items():
        subset = df[df["label_up_down"] == label_val]
        if subset.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=subset["timestamp"],
                y=subset["close"] if df["close"].notna().any() else subset["label_return"],
                mode="markers",
                marker=dict(color=LABEL_COLORS[label_val], size=5, opacity=0.8),
                name=label_name
            ),
            row=1, col=1
        )

    # Rendement brut
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df["label_return"] * 100,
            mode="lines",
            line=dict(color="#5599ff", width=1),
            name="Rendement (%)",
            showlegend=True
        ),
        row=2, col=1
    )
    fig.add_hline(y=0,  line_dash="dash", line_color="white",  opacity=0.3, row=2, col=1)

    fig.update_layout(
        template="plotly_dark",
        height=600,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    fig.update_yaxes(title_text="Prix (USDT)", row=1, col=1)
    fig.update_yaxes(title_text="r(t) (%)",    row=2, col=1)
    return fig


def main():
    st.header("Visualisation des labels", divider="gray")

    with st.expander("ℹ️ Comment sont calculés les labels ?", expanded=False):
        st.markdown("""
        Les labels représentent la **vérité terrain** des bonnes décisions sur les données historiques.
        Ils sont calculés par la méthode du **Forward Return à horizon fixe**.

        **Étape 1 — Rendement futur `label_return`**

        Pour chaque candle à l'instant *t*, on calcule le rendement réalisé *N* candles plus tard :

        > `r(t) = ( close(t+N) - close(t) ) / close(t)`

        **Étape 2 — Classe `label_up_down`**

        Le rendement est ensuite converti en décision de trading en le comparant à un seuil symétrique θ :

        | Condition | Classe | Décision |
        |---|---|---|
        | `r(t) > +θ` | **+1** | BUY — hausse significative |
        | `r(t) < −θ` | **−1** | SELL — baisse significative |
        | `\|r(t)\| ≤ θ` | **0** | HOLD — mouvement trop faible |

        > ⚠️ Les *N* dernières candles de l'historique n'ont pas de label car leur rendement futur n'est pas encore connu.

        **Jeux de paramètres disponibles** (sélectionnables dans la sidebar) :

        | Intervalle | Horizon | Fenêtre réelle | Seuil θ |
        |---|---|---|---|
        | 1d | 5 candles | 5 jours | 2%, 3% |
        | 1d | 10 candles | 10 jours | 2%, 3% |
        | 1h | 12 candles | 12 heures | 1%, 2% |
        | 1h | 24 candles | 1 journée | 1%, 2% |
        | 5m | 12 candles | 1 heure | 0.3%, 0.5% |
        | 5m | 24 candles | 2 heures | 0.3%, 0.5% |
        | 1m | 30 candles | 30 minutes | 0.1%, 0.3% |
        | 1m | 60 candles | 1 heure | 0.1%, 0.3% |

        Les seuils sont calibrés selon la volatilité typique de chaque granularité :
        plus l'intervalle est court, plus les mouvements significatifs sont petits.
        """)

    # --- Sidebar ---
    st.sidebar.title("Paramètres")

    symbol = st.sidebar.selectbox(
        "Symbole",
        options=["BTCUSDT", "ETHUSDT"],
        index=0
    )

    interval = st.sidebar.selectbox(
        "Intervalle",
        options=["1h", "1d", "5m", "1m"],
        index=0
    )

    # Chargement des paramètres disponibles en base pour ce symbol/interval
    available_params = get_available_params(symbol, interval)
    if not available_params:
        st.warning("Aucun label trouvé pour ce symbole/intervalle. Lancez d'abord `compute_labels.sh`.") 
        return

    param_options = {f"horizon={h}, seuil={t*100:.2f}%": (h, t) for h, t in available_params}
    selected_label = st.sidebar.selectbox(
        "Version des labels (horizon / seuil)",
        options=list(param_options.keys())
    )
    horizon, threshold = param_options[selected_label]

    max_points = st.sidebar.slider(
        "Nombre max de labels",
        min_value=100,
        max_value=5000,
        value=1000,
        step=100
    )

    use_date_filter = st.sidebar.checkbox("Filtrer par date", value=False)
    start_date, end_date = None, None
    if use_date_filter:
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input("Date début", value=datetime.now().date() - timedelta(days=30))
        with col2:
            end_date = st.date_input("Date fin", value=datetime.now().date())
        start_date = datetime.combine(start_date, datetime.min.time())
        end_date   = datetime.combine(end_date,   datetime.max.time())

    # --- Chargement ---
    with st.spinner("Chargement des labels..."):
        df = load_labels(symbol, interval, horizon, threshold, start_date, end_date, max_points)

    if df.empty:
        st.warning("Aucune donnée pour ces paramètres.")
        return

    # --- Métriques ---
    counts = df["label_up_down"].value_counts()
    total  = len(df)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total labels", total)
    c2.metric("BUY (+1)",  f"{counts.get(1, 0)} ({counts.get(1, 0)/total*100:.1f}%)",  delta=None)
    c3.metric("HOLD (0)",  f"{counts.get(0, 0)} ({counts.get(0, 0)/total*100:.1f}%)",  delta=None)
    c4.metric("SELL (-1)", f"{counts.get(-1, 0)} ({counts.get(-1, 0)/total*100:.1f}%)", delta=None)

    st.info(
        f"Intervalle : **{interval}** | Horizon : **{horizon} candles** | Seuil : **{threshold*100:.2f}%** | "
        f"Période : **{df['timestamp'].min().strftime('%Y-%m-%d %H:%M')}** → "
        f"**{df['timestamp'].max().strftime('%Y-%m-%d %H:%M')}** | "
        f"Rendement moyen : **{df['label_return'].mean()*100:.3f}%** | "
        f"Rendement std : **{df['label_return'].std()*100:.3f}%**"
    )

    # --- Graphiques ---
    col_left, col_right = st.columns([1, 2])
    with col_left:
        st.plotly_chart(chart_distribution(df), use_container_width=True)
    with col_right:
        fig_hist = px.histogram(
            df, x="label_return",
            nbins=80,
            color_discrete_sequence=["#5599ff"],
            title="Distribution des rendements r(t)",
            labels={"label_return": "Rendement r(t)"},
            template="plotly_dark"
        )
        fig_hist.add_vline(x=threshold,  line_dash="dash", line_color="#00ff88", annotation_text=f"+{threshold*100:.2f}%")
        fig_hist.add_vline(x=-threshold, line_dash="dash", line_color="#ff4444", annotation_text=f"-{threshold*100:.2f}%")
        fig_hist.update_layout(height=550)
        st.plotly_chart(fig_hist, use_container_width=True)

    st.plotly_chart(chart_labels_over_time(df, symbol), use_container_width=True)

if __name__ == "__main__":
    main()
