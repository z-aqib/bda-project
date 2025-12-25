FROM apache/airflow:2.9.3-python3.10

USER root
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*
USER airflow

RUN pip install --no-cache-dir pymongo==4.6.3
