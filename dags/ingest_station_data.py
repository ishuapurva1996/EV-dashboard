from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowSkipException
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dotenv import load_dotenv, find_dotenv
from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests
import os

load_dotenv(find_dotenv())

NREL_BASE = "https://developer.nrel.gov/api/alt-fuel-stations/v1"
TARGET_TABLE = "RAW_EV.NREL_STATIONS"
SNOWFLAKE_CONN_ID = "snowflake_default"
HIGH_WATER_MARK_VAR = "nrel_last_updated"


def return_snowflake_conn():

      # Initialize the SnowflakeHook
      hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
      
      # Execute the query and fetch results
      conn = hook.get_conn()
      return conn.cursor()

def extract_ev_raw_data(NREL_BASE):
    api_key = os.environ["NREL_API_KEY"]
    params = {
            "api_key": api_key,
            "fuel_type": "ELEC",
            "country": "US",
            "status": "E,P,T",
            "access": "public",
            "limit": "all",
        }
    r = requests.get(NREL_BASE, params=params)

    if r.status_code != 200:
        raise RuntimeError(f"API request failed: {r.status_code} {r.text}")

    data = r.json()
    return data

def _clean(v):
    """Normalize NREL's empty strings to None so Snowflake DATE/NUMBER columns don't choke."""
    return None if v in ("", None) else v


def transform_ev_data(raw_data):
    stations = raw_data["fuel_stations"]

    if len(stations) != raw_data["total_results"]:
        raise RuntimeError(
            f"NREL count mismatch: header says {raw_data['total_results']}, got {len(stations)}"
        )

    records = []
    for s in stations:
        connectors = s.get("ev_connector_types") or []
        records.append({
            "id": s["id"],
            "station_name": _clean(s["station_name"]),
            "updated_at": _clean(s["updated_at"]),
            "date_last_confirmed": _clean(s["date_last_confirmed"]),
            "latitude": s["latitude"],
            "longitude": s["longitude"],
            "street_address": _clean(s["street_address"]),
            "city": _clean(s["city"]),
            "state": _clean(s["state"]),
            "zip": _clean(s["zip"]),
            "ev_level1_evse_num": s["ev_level1_evse_num"],
            "ev_level2_evse_num": s["ev_level2_evse_num"],
            "ev_dc_fast_num": s["ev_dc_fast_num"],
            "ev_connector_types": ",".join(connectors) if connectors else None,
            "status_code": _clean(s["status_code"]),
            "access_code": _clean(s["access_code"]),
            "ev_network": _clean(s["ev_network"]),
            "ev_pricing": _clean(s["ev_pricing"]),
            "facility_type": _clean(s["facility_type"]),
            "open_date": _clean(s["open_date"])
        })

    return records


@task
def check_last_updated(nrel_base):
    api_key = os.environ["NREL_API_KEY"]
    r = requests.get(f"{nrel_base}/last-updated.json", params={"api_key": api_key})

    if r.status_code != 200:
        raise RuntimeError(f"NREL last-updated HTTP {r.status_code}: {r.text}")

    body = r.json()
    if "last_updated" not in body:
        raise RuntimeError(f"NREL last-updated response missing field; got: {body}")

    current = body["last_updated"]   # e.g. "2026-05-02T04:14:44Z"
    last_known = Variable.get(HIGH_WATER_MARK_VAR, default_var=None)

    if current == last_known:
        raise AirflowSkipException(f"NREL unchanged since {last_known} — skipping pull")

    return current


@task
def update_high_water_mark(current_ts):
    Variable.set(HIGH_WATER_MARK_VAR, current_ts)


@task
def extract_transform_load(nrel_base, target_table):
    """Single task to keep the 280MB raw dict and 81k-record list in memory,
    instead of round-tripping them through XCom (which is JSON-into-Postgres,
    capped at ~1GB per row)."""
    raw_data = extract_ev_raw_data(nrel_base)
    records = transform_ev_data(raw_data)
    load_data_to_snowflake(records, target_table)


def load_data_to_snowflake(records, target_table):
    con = return_snowflake_conn()
    try:
        con.execute("BEGIN")
        con.execute(f"""CREATE TABLE IF NOT EXISTS {target_table}(
                    id                  NUMBER PRIMARY KEY,
                    station_name        VARCHAR,
                    updated_at          TIMESTAMP_TZ,
                    date_last_confirmed DATE,
                    latitude            FLOAT,
                    longitude           FLOAT,
                    street_address      VARCHAR,
                    city                VARCHAR,
                    state               VARCHAR,
                    zip                 VARCHAR,
                    ev_level1_evse_num  NUMBER,
                    ev_level2_evse_num  NUMBER,
                    ev_dc_fast_num      NUMBER,
                    ev_connector_types  VARCHAR,
                    status_code         VARCHAR,
                    access_code         VARCHAR,
                    ev_network          VARCHAR,
                    ev_pricing          VARCHAR,
                    facility_type       VARCHAR,
                    open_date           DATE
                    );""")
        
        con.execute(f"""DELETE FROM {target_table}""")

        sql = f"""
        INSERT INTO {target_table}
        (id, station_name, updated_at, date_last_confirmed,
         latitude, longitude, street_address, city, state, zip,
         ev_level1_evse_num, ev_level2_evse_num, ev_dc_fast_num,
         ev_connector_types, status_code, access_code,
         ev_network, ev_pricing, facility_type, open_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        rows = [(
            val['id'], val['station_name'], val['updated_at'], val['date_last_confirmed'],
            val['latitude'], val['longitude'], val['street_address'], val['city'],
            val['state'], val['zip'], val['ev_level1_evse_num'], val['ev_level2_evse_num'],
            val['ev_dc_fast_num'], val['ev_connector_types'], val['status_code'],
            val['access_code'], val['ev_network'], val['ev_pricing'], val['facility_type'],
            val['open_date'],
        ) for val in records]
        con.executemany(sql, rows)
        con.execute("COMMIT")
        print(f'loaded {len(records)} records in the {target_table}')

    except Exception as e:
        con.execute("ROLLBACK")
        print(e)
        raise


with DAG(
    dag_id='nrel_stations_data_ingest',
    description="Daily refresh of NREL EV charging stations into RAW_EV.NREL_STATIONS.",
    start_date=datetime(2026, 4, 24),
    catchup=False,
    tags=["ETL", "nrel", "ev"],
    schedule='30 02 * * *',
) as dag:
    
    current_ts = check_last_updated(NREL_BASE)
    loaded = extract_transform_load(NREL_BASE, TARGET_TABLE)
    final = update_high_water_mark(current_ts)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt",
        trigger_dag_id="ev_dbt_pipeline",
        wait_for_completion=False,
    )

    current_ts >> loaded >> final >> trigger_dbt   # gate → ETL → watermark → dbt

    





