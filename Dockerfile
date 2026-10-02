# Dockerfile
#
# Extends the official Airflow image with:
# - Java (required by PySpark, which runs on the JVM even when called
#   from Python)
# - Every Python package our pipeline code needs (pyspark, dbt,
#   pyarrow, requests, etc.)
#
# Project code (src/, dbt/, dags/) is NOT copied into this image --
# it's mounted as a volume in docker-compose.yml instead, so editing
# code doesn't require rebuilding the image during development.

FROM apache/airflow:2.10.3-python3.11

USER root

# Java is needed for PySpark (it runs on the JVM under the hood)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt
RUN pip install --no-cache-dir -r /requirements-airflow.txt
