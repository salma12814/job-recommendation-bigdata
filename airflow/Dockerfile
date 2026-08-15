FROM apache/airflow:2.9.3

USER root

# Java (necessaire pour PySpark) + utilitaires
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Librairies Python pour le pipeline
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    kafka-python-ng==2.2.3 \
    numpy==1.26.4 \
    psycopg2-binary==2.9.9 \
    pandas==2.2.2