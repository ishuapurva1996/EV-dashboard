from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException

from dotenv import load_dotenv, find_dotenv
from datetime import datetime, timezone
import snowflake.connector
import requests
import os


load_dotenv(find_dotenv())

CENSUS_BASE = "https://api.census.gov/data/2024/acs/acs5" 
SNOWFLAKE_CONN_ID = "snowflake_default"

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
      
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn, conn.cursor()


@task
def extract_census_population():
    api_key = os.environ["CENSUS_API_KEY"]
    params = {
            "key": api_key,
            "get": "NAME,B01003_001E",
            "for": "state:*",
        }
    r = requests.get(CENSUS_BASE, params=params, timeout=30)

    if r.status_code != 200:
        raise RuntimeError(f"API request failed: {r.status_code} {r.text}")

    data = r.json()
    return data


@task
def transform_census_population_data(raw_data):
    records = []
    loaded_at = datetime.now(timezone.utc)
    for state in raw_data[1:]:
        records.append({'state_fips': state[2],
                        'state_name': state[0],
                        'population': int(state[1]),
                        'acs_year': 2024, 
                        'loaded_at': loaded_at 
                        })
    
    return records

@task
def load_population_data_into_snowflake(records, target_table):
    connec, con = return_snowflake_conn()
    try:
        con.execute("BEGIN")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table}(
                    state_fips    VARCHAR(2),                                                                                                                                   
                    state_name    VARCHAR,
                    population    BIGINT,                                                                                                                                       
                    acs_year      NUMBER(4),                                              
                    loaded_at     TIMESTAMP_NTZ                                                                                                                                 
                );""")
        
        con.execute(f"""DELETE FROM {target_table}""")
        sql = f"""INSERT INTO {target_table} (state_fips, state_name, population, acs_year, loaded_at)
                  VALUES (%s, %s, %s, %s, %s)"""
        
        rows = []
        for rec in records:
            rows.append(
                (
                    rec["state_fips"],                                                                                                                                  
                    rec["state_name"],
                    rec["population"],                                                                                                                                  
                    rec["acs_year"],                                              
                    rec["loaded_at"],
                )
            )
        con.executemany(sql, rows)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:                                                                                                                                                    
        con.close()   
        connec.close()

            



with DAG(
    dag_id='census_population_data_ingest',
    description="populating data into RAW_EV.CENSUS_POPULATION",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    tags=["ETL", "census", "population"],
    schedule="@yearly",
) as dag:
    
    target_table = 'RAW_EV.CENSUS_POPULATION'

    raw_data = extract_census_population()
    transform_data = transform_census_population_data(raw_data)
    load_population_data_into_snowflake(transform_data, target_table)
    