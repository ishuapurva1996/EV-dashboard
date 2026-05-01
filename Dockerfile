FROM apache/airflow:2.10.1-python3.12

USER airflow

RUN pip install --no-cache-dir \
      "apache-airflow-providers-snowflake==5.7.0" \
      "apache-airflow-providers-docker" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.1/constraints-3.12.txt"

RUN pip install --no-cache-dir "dbt-snowflake==1.8.4"
