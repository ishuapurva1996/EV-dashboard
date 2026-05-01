from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


with DAG(
    dag_id="test_snowflake_connection",
    description="Smoke test: verifies Airflow can run SQL through snowflake_default.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["smoke-test"],
) as dag:

    check_identity = SnowflakeOperator(
        task_id="check_identity",
        snowflake_conn_id="snowflake_default",
        sql="""
            SELECT
                CURRENT_USER()      AS user,
                CURRENT_ROLE()      AS role,
                CURRENT_WAREHOUSE() AS warehouse,
                CURRENT_DATABASE()  AS database,
                CURRENT_SCHEMA()    AS schema,
                CURRENT_VERSION()   AS snowflake_version;
        """,
    )
