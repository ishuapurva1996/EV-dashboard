"""
Daily ingest of NREL Alternative Fuels Station data into RAW_EV.NREL_STATIONS.

Pattern:
  1. Cheap call to /v1/last-updated.json
  2. Compare with the high-water-mark in Airflow Variable `nrel_last_updated`
  3. If unchanged -> skip the expensive full pull (saves quota + time)
  4. Otherwise -> pull all stations, TRUNCATE + INSERT into RAW_EV.NREL_STATIONS,
     then update the variable

Source docs: https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/
"""
from datetime import datetime
import logging
import os

import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

NREL_BASE = "https://developer.nrel.gov/api/alt-fuel-stations/v1"
SNOWFLAKE_CONN_ID = "snowflake_default"
HIGH_WATER_MARK_VAR = "nrel_last_updated"

TARGET_FIELDS = [
    "id", "station_name", "city", "state", "zip",
    "latitude", "longitude",
    "ev_level1_evse_num", "ev_level2_evse_num", "ev_dc_fast_num",
    "ev_connector_types", "ev_network", "ev_pricing",
    "status_code", "access_code",
    "open_date", "date_last_confirmed",
    "facility_type", "owner_type_code",
]

CREATE_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS RAW_EV;

CREATE TABLE IF NOT EXISTS RAW_EV.NREL_STATIONS (
    id                  NUMBER PRIMARY KEY,
    station_name        VARCHAR,
    city                VARCHAR,
    state               VARCHAR,
    zip                 VARCHAR,
    latitude            FLOAT,
    longitude           FLOAT,
    ev_level1_evse_num  NUMBER,
    ev_level2_evse_num  NUMBER,
    ev_dc_fast_num      NUMBER,
    ev_connector_types  VARCHAR,
    ev_network          VARCHAR,
    ev_pricing          VARCHAR,
    status_code         VARCHAR,
    access_code         VARCHAR,
    open_date           DATE,
    date_last_confirmed DATE,
    facility_type       VARCHAR,
    owner_type_code     VARCHAR,
    ingested_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""


def _nullify_blank(value):
    """Snowflake DATE columns reject empty strings — normalize to None."""
    return value if value else None


def _row_from_station(s: dict) -> tuple:
    return (
        s["id"],
        s.get("station_name"),
        s.get("city"),
        s.get("state"),
        s.get("zip"),
        s.get("latitude"),
        s.get("longitude"),
        s.get("ev_level1_evse_num"),
        s.get("ev_level2_evse_num"),
        s.get("ev_dc_fast_num"),
        ",".join(s.get("ev_connector_types") or []) or None,
        s.get("ev_network"),
        s.get("ev_pricing"),
        s.get("status_code"),
        s.get("access_code"),
        _nullify_blank(s.get("open_date")),
        _nullify_blank(s.get("date_last_confirmed")),
        s.get("facility_type"),
        s.get("owner_type_code"),
    )


@dag(
    dag_id="nrel_stations_ingest",
    description="Daily refresh of NREL EV charging stations into RAW_EV.NREL_STATIONS.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ingest", "nrel", "ev"],
)
def nrel_stations_ingest():

    @task
    def fetch_remote_last_updated() -> str:
        api_key = os.environ["NREL_API_KEY"]
        r = requests.get(
            f"{NREL_BASE}/last-updated.json",
            params={"api_key": api_key},
            timeout=30,
        )
        r.raise_for_status()
        ts = r.json()["last_updated"]
        logger.info("Remote last_updated: %s", ts)
        return ts

    @task
    def load_if_changed(remote_ts: str) -> int:
        local_ts = Variable.get(HIGH_WATER_MARK_VAR, default_var="")
        logger.info("Local: %s | Remote: %s", local_ts or "(none)", remote_ts)

        if local_ts == remote_ts:
            raise AirflowSkipException("NREL data unchanged — skipping full pull.")

        api_key = os.environ["NREL_API_KEY"]
        logger.info("Fetching full station list...")
        r = requests.get(
            f"{NREL_BASE}.json",
            params={
                "api_key": api_key,
                "fuel_type": "ELEC",
                "country": "US",
                "status": "E,P,T",
                "access": "public",
                "limit": "all",
            },
            timeout=300,
        )
        r.raise_for_status()
        stations = r.json()["fuel_stations"]
        logger.info("Fetched %d stations.", len(stations))

        rows = [_row_from_station(s) for s in stations]

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        for stmt in CREATE_TABLE_SQL.strip().split(";"):
            if stmt.strip():
                hook.run(stmt)
        hook.run("TRUNCATE TABLE RAW_EV.NREL_STATIONS")
        hook.insert_rows(
            table="RAW_EV.NREL_STATIONS",
            rows=rows,
            target_fields=TARGET_FIELDS,
            commit_every=1000,
        )

        Variable.set(HIGH_WATER_MARK_VAR, remote_ts)
        logger.info("Inserted %d rows; high-water-mark = %s.", len(rows), remote_ts)
        return len(rows)

    load_if_changed(fetch_remote_last_updated())


nrel_stations_ingest()
