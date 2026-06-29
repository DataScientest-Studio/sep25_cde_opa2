import streamlit as st
import streamlit.components.v1 as components
import requests

from src.common.api import get_api_base_url
from src.config import API_PORT

st.set_page_config(
    page_title="Documentation API",
    page_icon="🔌",
    layout="wide",
    initial_sidebar_state="expanded",
)


def check_health(api_base_url: str) -> dict | None:
    try:
        response = requests.get(f"{api_base_url}/health", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


def fetch_openapi(api_base_url: str) -> dict | None:
    try:
        response = requests.get(f"{api_base_url}/openapi.json", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


def main():
    api_base_url = get_api_base_url()
    # URL accessible par le navigateur : le port est exposé sur localhost
    browser_api_url = f"http://localhost:{API_PORT}"

    st.header("API — Documentation & Explorateur", divider="gray")

    # --- Statut ---
    health = check_health(api_base_url)
    col_status, col_url = st.columns([1, 3])
    with col_status:
        if health and health.get("status") == "healthy":
            st.success("API en ligne ✅")
        else:
            st.error("API inaccessible ❌")
    with col_url:
        st.info(f"URL de base : **{browser_api_url}**")

    if health:
        db_status = health.get("database", "unknown")
        ts = health.get("timestamp", "")
        st.caption(f"Base de données : **{db_status}** | Dernière vérification : {ts}")

    st.divider()

    # --- Onglets ---
    tab_swagger, tab_redoc = st.tabs(
        ["📖 Swagger UI", "📚 ReDoc"]
    )

    with tab_swagger:
        st.markdown(
            f"Interface interactive Swagger UI — [ouvrir dans un nouvel onglet]({browser_api_url}/docs)"
        )
        components.iframe(f"{browser_api_url}/docs", height=700, scrolling=True)

    with tab_redoc:
        st.markdown(
            f"Documentation ReDoc — [ouvrir dans un nouvel onglet]({browser_api_url}/redoc)"
        )
        components.iframe(f"{browser_api_url}/redoc", height=700, scrolling=True)


if __name__ == "__main__":
    main()
