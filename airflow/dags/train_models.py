import os
from datetime import timedelta

import pendulum
from airflow.sdk import DAG, literal
from airflow.providers.docker.operators.docker import DockerOperator


LOCAL_TZ = pendulum.timezone("Europe/Paris")

PROJECT_ENV_FILE = os.getenv("AIRFLOW_PROJECT_ENV_FILE", "/opt/airflow/project.env")

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

DOCKER_URL = PROJECT_ENV.get("AIRFLOW_DOCKER_URL", "unix://var/run/docker.sock")
DOCKER_NETWORK = PROJECT_ENV.get("DOCKER_NETWORK", "sep25_cde_opa2_default")
DOCKER_REGISTRY = PROJECT_ENV.get("DOCKER_REGISTRY", "")

IMG_MODELS_TRAIN = PROJECT_ENV.get("IMG_MODELS_TRAIN")
IMG_MODELS_TRAIN_SENTIMENT = PROJECT_ENV.get("IMG_MODELS_TRAIN_SENTIMENT")

TRAIN_IMAGE = f"{DOCKER_REGISTRY}{IMG_MODELS_TRAIN}"
TRAIN_SENTIMENT_IMAGE = f"{DOCKER_REGISTRY}{IMG_MODELS_TRAIN_SENTIMENT}"

default_args = {
    "owner": "cryptobot",
    "retries": 0,
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
    dag_id="train_models",
    description=(
        "Pipeline hebdomadaire d'entrainement de models liés aux données marché et de sentiments"
    ),
    default_args=default_args,
    start_date=pendulum.datetime(2026, 7, 20, tz=LOCAL_TZ),
    schedule="0 8 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["cryptobot", "train", "market", "sentiment"],
) as dag:

    train_market_model = docker_task(
        task_id="train_market_model",
        image=TRAIN_IMAGE,
        command="bash src/models/train_model.sh",
        timeout=60 * 60,
    )

    train_sentiment_model = docker_task(
        task_id="train_sentiment_model",
        image=TRAIN_SENTIMENT_IMAGE,
        command="bash src/models/train_sentiment_model.sh",
        timeout=60 * 60,
    )
