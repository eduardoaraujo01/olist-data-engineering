FROM apache/airflow:3.3.2-python3.12

USER root
RUN mkdir -p /opt/airflow/auth \
    && chown airflow:root /opt/airflow/auth \
    && chmod 700 /opt/airflow/auth

USER airflow
RUN pip install --no-cache-dir "apache-airflow-providers-docker==4.5.9" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt"