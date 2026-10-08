from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from ev_publication import run_dbt, export_and_publish, dispatch_dashboard

with DAG('ev_dbt_pipeline', description='Build/test EV models and publish only a validated successful export.',
         schedule=None, start_date=datetime(2026,4,24), catchup=False, max_active_runs=1,
         default_args={'owner':'airflow','retries':1,'retry_delay':timedelta(minutes=5)}, tags=['ELT','dbt','ev']) as dag:
    dbt_seed = PythonOperator(task_id='dbt_seed',python_callable=run_dbt,op_args=['seed'],do_xcom_push=True,show_return_value_in_logs=False)
    dbt_run = PythonOperator(task_id='dbt_run',python_callable=run_dbt,op_args=['run'],do_xcom_push=True,show_return_value_in_logs=False)
    dbt_test = PythonOperator(task_id='dbt_test',python_callable=run_dbt,op_args=['test'],do_xcom_push=True,show_return_value_in_logs=False)
    export = PythonOperator(task_id='export_dashboard_bundle',python_callable=export_and_publish,show_return_value_in_logs=False)
    publish = PythonOperator(task_id='trigger_dashboard_deploy',python_callable=dispatch_dashboard,show_return_value_in_logs=False)
    dbt_seed >> dbt_run >> dbt_test >> export >> publish
