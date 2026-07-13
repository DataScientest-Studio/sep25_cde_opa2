import os
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, literal
from airflow.providers.docker.operators.docker import DockerOperator


LOCAL_TZ = pendulum.timezone("Europe/Paris")

DOCKER_URL = os.getenv("AIRFLOW_DOCKER_URL", "unix://var/run/docker.sock")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "sep25_cde_opa2_default")
PROJECT_ENV_FILE = os.getenv("AIRFLOW_PROJECT_ENV_FILE", "/opt/airflow/project.env")
DOCKER_REGISTRY = os.getenv("DOCKER_REGISTRY", "")

IMG_SCRAPER = os.getenv("IMG_SCRAPER")
IMG_SCRAPER_SENTIMENT = os.getenv("IMG_SCRAPER_SENTIMENT")
IMG_MODELS_PREDICT_SENTIMENT = os.getenv("IMG_MODELS_PREDICT_SENTIMENT")

SCRAPER_IMAGE = f"{DOCKER_REGISTRY}{IMG_SCRAPER}"
SCRAPER_SENTIMENT_IMAGE = f"{DOCKER_REGISTRY}{IMG_SCRAPER_SENTIMENT}"
SCRAPER_PREDICT_IMAGE = f"{DOCKER_REGISTRY}{IMG_MODELS_PREDICT_SENTIMENT}"

def load_env_file(path: str) -> dict[str, str]:
    env_vars = {}

    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            env_vars[key.strip()] = value.strip().strip('"').strip("'")

    return env_vars


PROJECT_ENV = load_env_file(PROJECT_ENV_FILE)
# Suppression de variables d'env propres à airflow
for key in list(PROJECT_ENV.keys()):
    if key.startswith("AIRFLOW_"):
        PROJECT_ENV.pop(key)

PROJECT_ENV["ENV"] = "docker"

default_args = {
    "owner": "cryptobot",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def docker_task(
    task_id: str,
    image: str,
    command: str,
    timeout: int = 60 * 60,
) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=literal(command),
        docker_url=DOCKER_URL,
        network_mode=DOCKER_NETWORK,
        environment=PROJECT_ENV,
        working_dir="/app",
        auto_remove="success",
        mount_tmp_dir=False,
        tty=False,
        timeout=timeout,
    )


with DAG(
    dag_id="scraping_sentiment_daily",
    description=(
        "Pipeline quotidien de scraping crypto : indexation des articles, "
        "enrichissement, détection des symboles, features sentiment, "
        "entraînement et prédiction."
    ),
    default_args=default_args,
    start_date=pendulum.datetime(2026, 7, 13, tz=LOCAL_TZ),
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["cryptobot", "scraping", "sentiment"],
) as dag:

    index_articles = docker_task(
        task_id="index_articles",
        image=SCRAPER_IMAGE,
        command="python -m src.data.scraping.index_articles",
        timeout=60 * 60,
    )

    enrich_articles = docker_task(
        task_id="enrich_articles",
        image=SCRAPER_IMAGE,
        command="python -m src.data.scraping.enrich_articles",
        timeout=2 * 60 * 60,
    )

    detect_symbols = docker_task(
        task_id="detect_symbols",
        image=SCRAPER_IMAGE,
        command="python -m src.data.scraping.detect_symbols",
        timeout=60 * 60,
    )

    compute_sentiment_features = docker_task(
        task_id="compute_sentiment_features",
        image=SCRAPER_SENTIMENT_IMAGE,
        command="bash src/features/scraping/entrypoint.sh",
        timeout=2 * 60 * 60,
    )

    predict_sentiment = docker_task(
        task_id="predict_sentiment",
        image=SCRAPER_PREDICT_IMAGE,
        command="bash src/models/predict_sentiment_model.sh",
        timeout=60 * 60,
    )

    (
        index_articles
        >> enrich_articles
        >> detect_symbols
        >> compute_sentiment_features
        >> predict_sentiment
    )