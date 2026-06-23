import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from src.common.connectors import PostgreSQLConnector
from src.common.custom_logger import logger

st.set_page_config(
    page_title="Visualisation des prédictions",
    page_icon="🎲",
    layout="wide",
    initial_sidebar_state="expanded",
)

CLASS_META = {
    -1: {"name": "SELL (-1)", "short": "SELL", "color": "#ff4444", "symbol": "triangle-down"},
    0: {"name": "HOLD (0)", "short": "HOLD", "color": "#aaaaaa", "symbol": "circle"},
    1: {"name": "BUY (+1)", "short": "BUY", "color": "#00ff88", "symbol": "triangle-up"},
}


@st.cache_resource
def get_pg_connection():
    try:
        return PostgreSQLConnector().connect()
    except SystemExit:
        st.error("Erreur de connexion PostgreSQL")
        return None


@st.cache_data(ttl=60)
def get_available_symbols() -> list[str]:
    try:
        connector = get_pg_connection()
        if connector is None:
            return []

        with connector.conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT s.symbol
                FROM predictions p
                JOIN symbols s ON s.id = p.id_symbol
                ORDER BY s.symbol;
                """
            )
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Erreur get_available_symbols: {e}")
        return []


@st.cache_data(ttl=60)
def get_available_intervals(symbol: str) -> list[str]:
    try:
        connector = get_pg_connection()
        if connector is None:
            return []

        with connector.conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT p.interval
                FROM predictions p
                JOIN symbols s ON s.id = p.id_symbol
                WHERE s.symbol = %s
                ORDER BY p.interval;
                """,
                (symbol,),
            )
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Erreur get_available_intervals: {e}")
        return []


@st.cache_data(ttl=60)
def get_available_versions(symbol: str, interval: str) -> list[tuple[int, float]]:
    """Version de modele/labels disponible, representee par (horizon, threshold)."""
    try:
        connector = get_pg_connection()
        if connector is None:
            return []

        with connector.conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT p.horizon, p.threshold
                FROM predictions p
                JOIN symbols s ON s.id = p.id_symbol
                WHERE s.symbol = %s
                  AND p.interval = %s
                ORDER BY p.horizon, p.threshold;
                """,
                (symbol, interval),
            )
            return [(int(row[0]), float(row[1])) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Erreur get_available_versions: {e}")
        return []


@st.cache_data(ttl=5)
def load_predictions_history(
    symbol: str,
    interval: str,
    horizon: int,
    threshold: float,
    limit: int = 1000,
) -> pd.DataFrame:
    try:
        connector = get_pg_connection()
        if connector is None:
            return pd.DataFrame()

        with connector.conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    p.timestamp,
                    p.predicted_up_down,
                    p.created_at,
                    c.close,
                    l.label_up_down
                FROM predictions p
                JOIN symbols s ON s.id = p.id_symbol
                LEFT JOIN candles c
                    ON c.id_symbol = p.id_symbol
                    AND c.interval = p.interval
                    AND c.open_time = p.timestamp
                LEFT JOIN labels l
                    ON l.id_symbol = p.id_symbol
                    AND l.interval = p.interval
                    AND l.horizon = p.horizon
                    AND l.threshold = p.threshold
                    AND l.timestamp = p.timestamp
                WHERE s.symbol = %s
                  AND p.interval = %s
                  AND p.horizon = %s
                  AND p.threshold = %s
                ORDER BY p.timestamp DESC
                LIMIT %s;
                """,
                (symbol, interval, horizon, threshold, limit),
            )
            rows = cur.fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(
            rows,
            columns=["timestamp", "predicted_up_down", "created_at", "close", "label_up_down"],
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["created_at"] = pd.to_datetime(df["created_at"])
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["predicted_up_down"] = pd.to_numeric(df["predicted_up_down"], errors="coerce").astype("Int64")
        df["label_up_down"] = pd.to_numeric(df["label_up_down"], errors="coerce").astype("Int64")

        return df.sort_values("timestamp").reset_index(drop=True)
    except Exception as e:
        logger.error(f"Erreur load_predictions_history: {e}")
        st.error(f"Erreur lors du chargement des prédictions: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_latest_prediction(symbol: str, interval: str, horizon: int, threshold: float) -> pd.DataFrame:
    return load_predictions_history(symbol, interval, horizon, threshold, limit=1)

def chart_predictions(df: pd.DataFrame, symbol: str) -> go.Figure:
    has_labels = df["label_up_down"].notna().any()
    has_close = df["close"].notna().any()

    if has_labels:
        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.06,
            subplot_titles=(
                f"Prédictions du modèle — {symbol}",
                f"Labels réels — {symbol}",
            ),
            row_width=[0.5, 0.5],
        )
    else:
        fig = make_subplots(rows=1, cols=1)

    # --- Sous-graphe 1 : prix + prédictions ---
    if has_close:
        fig.add_trace(
            go.Scatter(
                x=df["timestamp"],
                y=df["close"],
                mode="lines",
                line=dict(color="#888888", width=1),
                name="Close",
                showlegend=True,
            ),
            row=1, col=1,
        )

    for value, meta in CLASS_META.items():
        pred_subset = df[df["predicted_up_down"] == value]
        if not pred_subset.empty:
            fig.add_trace(
                go.Scatter(
                    x=pred_subset["timestamp"],
                    y=pred_subset["close"],
                    mode="markers",
                    marker=dict(
                        color=meta["color"],
                        size=8,
                        symbol=meta["symbol"],
                        opacity=0.9,
                    ),
                    name=f"Pred {meta['name']}",
                    showlegend=True,
                ),
                row=1, col=1,
            )

    # --- Sous-graphe 2 : prix + labels (uniquement si labels disponibles) ---
    if has_labels:
        if has_close:
            fig.add_trace(
                go.Scatter(
                    x=df["timestamp"],
                    y=df["close"],
                    mode="lines",
                    line=dict(color="#888888", width=1),
                    name="Close (labels)",
                    showlegend=False,
                ),
                row=2, col=1,
            )

        for value, meta in CLASS_META.items():
            label_subset = df[df["label_up_down"] == value]
            if not label_subset.empty:
                fig.add_trace(
                    go.Scatter(
                        x=label_subset["timestamp"],
                        y=label_subset["close"],
                        mode="markers",
                        marker=dict(
                            color=meta["color"],
                            size=9,
                            symbol="x",
                            line=dict(width=1.5, color=meta["color"]),
                        ),
                        name=f"Label {meta['name']}",
                        showlegend=True,
                    ),
                    row=2, col=1,
                )

    fig.update_layout(
        template="plotly_dark",
        height=750 if has_labels else 450,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_yaxes(title_text="Prix (USDT)", row=1, col=1)
    if has_labels:
        fig.update_yaxes(title_text="Prix (USDT)", row=2, col=1)
    return fig


@st.fragment(run_every="30s")
def render_latest_prediction(symbol: str, interval: str, horizon: int, threshold: float):
    latest_df = load_latest_prediction(symbol, interval, horizon, threshold)
    if latest_df.empty:
        st.warning("Aucune prédiction récente disponible pour ces paramètres.")
        return

    latest = latest_df.iloc[-1]
    pred_value = int(latest["predicted_up_down"])
    pred_meta = CLASS_META.get(pred_value)

    if pred_meta is None:
        pred_label = f"Classe inconnue ({pred_value})"
    else:
        pred_label = pred_meta["short"]

    label_text = "N/A"
    if pd.notna(latest["label_up_down"]):
        label_value = int(latest["label_up_down"])
        label_meta = CLASS_META.get(label_value)
        label_text = label_meta["short"] if label_meta else str(label_value)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Prediction courante", pred_label)
    c2.metric("Timestamp candle", latest["timestamp"].strftime("%Y-%m-%d %H:%M"))
    c3.metric("Close", f"{latest['close']:.4f}" if pd.notna(latest["close"]) else "N/A")
    c4.metric("Label disponible", label_text)

    st.caption(
        "Le label peut manquer sur les candles les plus récentes (horizon futur non encore observé)."
    )


@st.fragment(run_every="30s")
def render_predictions_history(symbol: str, interval: str, horizon: int, threshold: float, max_points: int):
    df = load_predictions_history(symbol, interval, horizon, threshold, max_points)

    if df.empty:
        st.warning("Aucune donnée historique disponible pour ces paramètres.")
        return

    st.info(
        f"Symbole : **{symbol}** | Intervalle : **{interval}** | "
        f"Horizon : **{horizon} candles** | Seuil : **{threshold * 100:.2f}%** | "
        f"Période : **{df['timestamp'].min().strftime('%Y-%m-%d %H:%M')}** -> "
        f"**{df['timestamp'].max().strftime('%Y-%m-%d %H:%M')}**"
    )

    st.plotly_chart(chart_predictions(df, symbol), use_container_width=True)
    st.caption(
        "Légende: triangles/cercle = prédictions, croix = labels réels."
    )


def main():
    st.header("Visualisation des prédictions", divider="gray")

    st.sidebar.title("Paramètres")

    symbols = get_available_symbols()
    if not symbols:
        st.warning("Aucune prédiction en base. Lancez d'abord le pipeline predict_model.")
        return

    symbol = st.sidebar.selectbox("Symbole", options=symbols, index=0)

    intervals = get_available_intervals(symbol)
    if not intervals:
        st.warning("Aucun intervalle disponible pour ce symbole.")
        return

    interval = st.sidebar.selectbox("Intervalle", options=intervals, index=0)

    versions = get_available_versions(symbol, interval)
    if not versions:
        st.warning("Aucune version horizon/seuil disponible pour ce symbole/intervalle.")
        return

    version_options = {
        f"horizon={h}, seuil={t * 100:.2f}%": (h, t)
        for h, t in versions
    }

    selected_version = st.sidebar.selectbox(
        "Version labels / modele",
        options=list(version_options.keys()),
    )
    horizon, threshold = version_options[selected_version]

    max_points = st.sidebar.slider(
        "Nombre max de prédictions",
        min_value=100,
        max_value=5000,
        value=1000,
        step=100,
    )

    st.subheader("Prédiction en cours (dernière candle)")
    render_latest_prediction(symbol, interval, horizon, threshold)
    render_predictions_history(symbol, interval, horizon, threshold, max_points)


if __name__ == "__main__":
    main()
