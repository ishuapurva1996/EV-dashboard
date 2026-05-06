from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# Templated from the Airflow `snowflake_default` connection (the same one the
# ingest DAGs use via SnowflakeHook). Variable names match dbt/profiles.yml.
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "env": {
        "SNOWFLAKE_USER":      "{{ conn.snowflake_default.login }}",
        "SNOWFLAKE_PASSWORD":  "{{ conn.snowflake_default.password }}",
        "SNOWFLAKE_ACCOUNT":   "{{ conn.snowflake_default.extra_dejson.account }}",
        "SNOWFLAKE_ROLE":      "{{ conn.snowflake_default.extra_dejson.role }}",
        "SNOWFLAKE_DATABASE":  "{{ conn.snowflake_default.extra_dejson.database }}",
        "SNOWFLAKE_WAREHOUSE": "{{ conn.snowflake_default.extra_dejson.warehouse }}",
        "SNOWFLAKE_SCHEMA":    "ANALYTICS_EV",
    },
}

DBT_DIR = "/opt/airflow/dbt"


with DAG(
    dag_id="ev_dbt_pipeline",
    default_args=default_args,
    description="Run dbt seed → run → test for the EV pipeline (staging → curated → analytics).",
    schedule=None,
    start_date=datetime(2026, 4, 24),
    tags=["ELT", "dbt", "ev"],
    catchup=False,
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"dbt seed --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        append_env=True,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        append_env=True,
    )

    dbt_seed >> dbt_run >> dbt_test
