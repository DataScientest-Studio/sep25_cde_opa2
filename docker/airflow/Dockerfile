FROM apache/airflow:3.3.0

WORKDIR /opt/airflow

COPY --chown=airflow:root requirements/airflow.txt ./requirements/airflow.txt

USER airflow

RUN pip install --no-cache-dir -r ./requirements/airflow.txt