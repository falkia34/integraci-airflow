FROM apache/airflow:3.0.2

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" polars-lts-cpu