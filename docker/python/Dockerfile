FROM python:3.14.4-slim

ARG REQUIREMENTS_FILE=requirements/db.txt

WORKDIR /app

COPY requirements/ requirements/
RUN pip install --no-cache-dir -r $REQUIREMENTS_FILE

COPY src/ src/