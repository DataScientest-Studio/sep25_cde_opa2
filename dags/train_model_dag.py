"""
DAG Airflow — entraînement quotidien du modèle de prédiction BTCUSDT.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.models.train_model import run as run_training

SYMBOL    = "BTCUSDT"
INTERVAL  = "1h"
HORIZON   = 12
THRESHOLD = 0.01

default_args = {
    "owner": "cryptobot",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="train_model_btcusdt",
    description="Entraînement quotidien du modèle sur BTCUSDT",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ml", "training", "cryptobot"],
) as dag:

    train_task = PythonOperator(
        task_id="train_and_save_model",
        python_callable=run_training,
        op_kwargs={
            "symbol": SYMBOL,
            "interval": INTERVAL,
            "horizon": HORIZON,
            "threshold": THRESHOLD,
        },
    )
