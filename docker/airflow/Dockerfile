FROM apache/airflow:2.10.4-python3.12

WORKDIR /opt/airflow

COPY requirements/models.txt requirements/models.txt
RUN pip install --no-cache-dir -r requirements/models.txt

COPY src/ src/
COPY models/ models/
