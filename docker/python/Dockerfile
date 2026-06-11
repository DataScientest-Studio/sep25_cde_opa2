FROM python:3.12.13-slim
ARG REQUIREMENTS_FILE=requirements/db.txt
WORKDIR /app
COPY requirements/ requirements/
RUN pip install --no-cache-dir -r $REQUIREMENTS_FILE
COPY src/ src/