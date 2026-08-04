import streamlit as st
import streamlit.components.v1 as components
import requests
from pathlib import Path


URL = f"http://localhost:8082"
DOCKER_URL = f"http://airflow-apiserver:8080"
ASSETS_DIR = Path(__file__).parent / "assets" / "slides"

st.set_page_config(
    page_title="Orchestration Airflow",
    page_icon="🌬️",
    layout="wide",
    initial_sidebar_state="expanded",
)

def check_health(url: str) -> dict | None:
    try:
        response = requests.get(f"{url}/api/v2/monitor/health", timeout=5)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        return {"error": str(e)}


def main():
    st.header("Airflow — Orchestrateur", divider="gray")

    health = check_health(DOCKER_URL)
    col_status, col_url = st.columns([1, 3])
    with col_status:
        if "error" in health:
            st.error("Airflow inaccessible ❌")
        else:
            metadatabase = health.get("metadatabase", {}).get("status")
            scheduler = health.get("scheduler", {}).get("status")
            dag_processor = health.get("dag_processor", {}).get("status")
            triggerer = health.get("triggerer", {}).get("status")

            if metadatabase == "healthy" and scheduler == "healthy" and dag_processor == "healthy":
                st.success("Airflow opérationnel ✅")
            else:
                st.warning("Airflow partiellement indisponible")
    with col_url:
        st.info(f"URL de base : **{URL}**")

    st.subheader(f"Captures Airflow")
    st.image(ASSETS_DIR / "DAGs.png", caption="Capture Airflow 1", use_container_width=True)
    st.image(ASSETS_DIR / "DAG1.png", caption="Capture Airflow 2", use_container_width=True)

if __name__ == "__main__":
    main()
