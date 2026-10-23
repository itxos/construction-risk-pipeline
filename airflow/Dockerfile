FROM apache/airflow:2.10.5-python3.12
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres==1.9.0
