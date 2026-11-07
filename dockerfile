FROM apache/airflow:3.4.0
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-google \
    pandas \
    dbt-bigquery 