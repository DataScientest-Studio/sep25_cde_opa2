import streamlit as st
import streamlit.components.v1 as components
import requests

from src.config import GRAFANA_HOST, GRAFANA_PORT

BROWSER_GRAFANA_URL = f"http://localhost:{GRAFANA_PORT}/dashboards"
SERVER_GRAFANA_URL = f"http://{GRAFANA_HOST}:{GRAFANA_PORT}"

st.set_page_config(
    page_title="Monitoring Grafana",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded",
)

def check_health(grafana_url: str) -> dict | None:
    try:
        response = requests.get(f"{grafana_url}/api/health", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception:
        return None


def main():
    st.header("Grafana — Monitoring", divider="gray")

    health = check_health(SERVER_GRAFANA_URL)
    col_status, col_url = st.columns([1, 3])
    with col_status:
        if health and health.get("database") == "ok":
            st.success("Grafana en ligne ✅")
        else:
            st.error("Grafana inaccessible ❌")
    with col_url:
        st.info(f"URL de base : **{BROWSER_GRAFANA_URL}**")

    if health:
        version = health.get("version", "unknown")
        st.caption(f"Version : **{version}**")

    st.divider()

    components.iframe(BROWSER_GRAFANA_URL, height=1000, scrolling=True)


if __name__ == "__main__":
    main()
