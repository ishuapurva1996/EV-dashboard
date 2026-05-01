# EV Charging Infrastructure & Adoption Analytics

End-to-end data pipeline analyzing US EV charging station coverage and cross-referencing it against state-level EV adoption to surface infrastructure gaps. Built for DATA 226 (Group 5).

**Pipeline:** NREL + DOE/AFDC + Census APIs → Airflow → Snowflake (RAW) → dbt (CURATED → ANALYTICS) → Preset.io dashboard.

**Team:** Pragya Apurva, Pragya Chourasia, Pinal Pawar, Sanjana Reddy Khatam.

---

## Architecture

```
┌────────────────────┐   daily
│ NREL AFDC API      │ ────────┐
└────────────────────┘         │
┌────────────────────┐ yearly  │   ┌──────────┐    ┌────────────────────┐    ┌──────────────────────┐    ┌────────┐
│ DOE/AFDC CSVs      │ ────────┼─→ │ Airflow  │ →  │ Snowflake RAW      │ →  │ dbt CURATED →        │ →  │ Preset │
└────────────────────┘         │   │ (Local-  │    │ schema             │    │ ANALYTICS schemas    │    │ Cloud  │
┌────────────────────┐ yearly  │   │ Executor)│    └────────────────────┘    └──────────────────────┘    └────────┘
│ Census ACS5 API    │ ────────┘   └──────────┘
└────────────────────┘
```

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Docker Desktop | 4.20+ | <https://docker.com/products/docker-desktop> |
| Python | 3.12+ | macOS: `brew install python@3.12` |
| git | any modern | already on macOS / Windows installer |

You also need:
- Your own **Snowflake training login** (user + password from your instructor) — each teammate has a personal account in the shared `SFEDU02-EAB27764` org.
- A free **NREL API key** — <https://developer.nrel.gov/signup/> (instant).
- A free **Census API key** — <https://api.census.gov/data/key_signup.html> (instant).

> **Shared resources:** the project's three Snowflake schemas (`RAW`, `CURATED`, `ANALYTICS`) live in **one shared database — `USER_DB_BADGER`**. Each teammate logs in with their *own* user/password but reads and writes to that same shared DB. The `TRAINING_ROLE` already has cross-DB access in this class's setup, so no extra Snowflake grants are needed.

---

## First-time setup

Once. ~10 minutes total. Most of the time is spent waiting for `docker compose build`.

### 1. Clone

```bash
git clone <repo-url> EV-pipeline
cd EV-pipeline
```

### 2. Create your personal `.env`

```bash
cp .env.example .env
```

The file already has the **shared values** filled in (account, database, schema, role). You only need to fill in the **personal** fields:

| Variable | Where to get it |
|---|---|
| `SNOWFLAKE_USER` | your training username (e.g., `EAGLE`) — given by instructor |
| `SNOWFLAKE_PASSWORD` | given by instructor |
| `SNOWFLAKE_WAREHOUSE` | your own warehouse (e.g., `EAGLE_QUERY_WH`) |
| `NREL_API_KEY` | from <https://developer.nrel.gov/signup/> |
| `CENSUS_API_KEY` | from <https://api.census.gov/data/key_signup.html> |

> **Never commit `.env`.** It's already in `.gitignore`. Each teammate has their own.

### 3. Create the local Python venv (for dbt CLI)

```bash
python3 -m venv ev_env
source ev_env/bin/activate
pip install --upgrade pip
pip install dbt-snowflake==1.8.4
```

The Airflow container has its own dbt install. The local `ev_env` is for fast iteration: `dbt debug`, `dbt run`, `dbt test` from your terminal.

### 4. Verify dbt connects to Snowflake

```bash
set -a && . .env && set +a       # load env vars into shell
cd dbt
dbt debug
cd ..
```

Expected output ends with:

```
Connection test: OK connection ok
All checks passed!
```

If it fails, see [Troubleshooting](#troubleshooting).

### 5. Build and start Airflow

```bash
docker compose build             # 3–5 min the first time
docker compose up -d             # ~30 sec
docker compose ps                # wait for "Up X min (healthy)"
```

Subsequent restarts are quick — only `up -d` is needed unless you change the `Dockerfile`.

### 6. Register the Snowflake connection in Airflow

1. Open <http://localhost:8081> — login `airflow` / `airflow`.
2. **Admin → Connections → `+`** (top-left).
3. Fill in:

   | Field | Value |
   |---|---|
   | Connection Id | `snowflake_default` |
   | Connection Type | `Snowflake` |
   | Login | your `SNOWFLAKE_USER` |
   | Password | your `SNOWFLAKE_PASSWORD` |
   | Schema | your `SNOWFLAKE_SCHEMA` |
   | Account | your `SNOWFLAKE_ACCOUNT` |
   | Warehouse | your `SNOWFLAKE_WAREHOUSE` |
   | Database | your `SNOWFLAKE_DATABASE` |
   | Role | `TRAINING_ROLE` |

4. Click **Test** → wait for green "Connection successfully tested" → **Save**.

Setup complete.

---

## Daily workflow

### Working on dbt models

```bash
source ev_env/bin/activate
set -a && . .env && set +a       # forget this and dbt errors with "Env var required"
cd dbt

dbt run                          # build all models
dbt run --select staging         # build only staging layer
dbt test                         # run all tests
dbt run --select +my_model       # build my_model and its dependencies
dbt docs generate && dbt docs serve   # browse model lineage in browser
```

### Working on Airflow DAGs

DAGs live in `dags/`. Drop a `.py` file there and Airflow auto-detects within ~30s — no restart needed.

```bash
docker compose up -d              # start
docker compose logs -f airflow    # tail live logs
docker compose down               # stop (preserves Postgres metadata)
```

### Adding a new Python dependency

Don't `pip install` inside the container — it won't persist after a rebuild.

1. Edit `Dockerfile` to add the package.
2. `docker compose build && docker compose up -d`.

### Pulling teammate changes

```bash
git pull origin main
docker compose build              # only if Dockerfile or requirements changed
docker compose up -d
```

---

## Project structure

```
EV-pipeline/
├── .env                    # YOUR creds — gitignored, per-person
├── .env.example            # template — committed
├── .gitignore
├── Dockerfile              # custom Airflow image, pinned deps
├── docker-compose.yaml     # Airflow + Postgres
├── README.md               # this file
├── ev_env/                 # local Python venv — gitignored
├── dags/                   # Airflow DAGs (.py files)
├── plugins/                # custom Airflow operators
├── config/                 # custom airflow.cfg overrides
├── logs/                   # Airflow runtime logs — gitignored
└── dbt/
    ├── dbt_project.yml     # dbt config (profile: ev_pipeline)
    ├── profiles.yml        # env-var-driven Snowflake connection (committed, no secrets)
    ├── models/
    │   ├── staging/        # raw → cleaned (views)
    │   ├── curated/        # standardized (tables)
    │   └── analytics/      # final dimensional models (tables)
    ├── seeds/              # static reference CSVs
    ├── macros/             # reusable Jinja
    ├── tests/              # custom data tests
    └── snapshots/          # SCD tracking
```

---

## How credentials flow

| Process | Runs where | Reads creds from |
|---|---|---|
| `dbt` CLI on your Mac | local `ev_env` venv | env vars (`set -a && . .env && set +a`) |
| Airflow scheduler + webserver | Docker container | `.env` auto-loaded via `env_file:` in compose |
| `dbt` triggered by an Airflow DAG | inside Airflow container | same `.env` env vars + `DBT_PROFILES_DIR=/opt/airflow/dbt` |
| Native Snowflake operators (`SnowflakeOperator`, `SnowflakeHook`) | inside Airflow container | the `snowflake_default` Airflow connection (per-machine, registered manually) |

`dbt/profiles.yml` is **committed** but contains no secrets — only `{{ env_var(...) }}` references that resolve at runtime.

---

## Snowflake schema layout

All four teammates read/write to the **same** three schemas in `USER_DB_BADGER`:

| Schema | Purpose | Populated by |
|---|---|---|
| `RAW` | Untransformed JSON / CSV from APIs | Airflow ingestion DAGs |
| `CURATED` | Cleaned, standardized, joined | dbt `models/staging` + `models/curated` |
| `ANALYTICS` | Final aggregates for dashboards | dbt `models/analytics` |

These were created once via `snowflake/setup.sql` (run by Pragya in the Snowflake worksheet UI). You don't need to re-run it.

> **Heads up on collaboration:** since we share schemas, two people running `dbt run` simultaneously can overwrite the same table mid-build. Easy mitigation — give a heads-up in chat ("running dbt now, hold off ~5 min") before kicking off a build.

---

## Data sources

| Dataset | URL | Cadence |
|---|---|---|
| NREL Alternative Fuels Stations | <https://developer.nrel.gov/docs/transportation/alt-fuel-stations-v1/> | Daily |
| DOE/AFDC State EV Registrations | <https://afdc.energy.gov/vehicle-registration> | Annual |
| US Census Bureau ACS5 | <https://api.census.gov/data/2024/acs/acs5> | Annual |

Original project proposal: see `Data 226 EV Dashboard Project Proposal (group 5).pdf`.

---

## Troubleshooting

| Symptom | Cause / Fix |
|---|---|
| `dbt debug`: `Env var required but not provided: 'SNOWFLAKE_USER'` | You skipped `set -a && . .env && set +a`. Run it from the project root. |
| `docker compose up` → `Bind for 0.0.0.0:8081 failed: port is already allocated` | Another container is using 8081. `docker ps` to find it, then `docker stop <name>`. Or change `8081:8080` to `8083:8080` in `docker-compose.yaml`. |
| Browser at `localhost:8081` shows `ERR_CONNECTION_RESET` | Webserver still booting. Wait 60–90s, confirm `docker compose ps` shows `(healthy)`. |
| `docker compose build` enters a long pip backtracking loop | You probably edited `Dockerfile` with incompatible versions. Restore the committed pins (`apache-airflow-providers-snowflake==5.7.0` + `dbt-snowflake==1.8.4`). |
| Airflow UI works but `snowflake_default` is missing | The connection lives in the local Postgres metadata volume — it's per-machine, not synced via git. Re-add through Admin → Connections (or run `docker compose down -v` recreates volume — you'll lose all connections). |
| dbt model errors: `Object 'X' does not exist` | Schema not yet created in Snowflake. `dbt run --full-refresh` once with privilege to create schemas, or pre-create the schema manually. |

---

## Cheat sheet

```bash
# stack
docker compose up -d              # start
docker compose down               # stop
docker compose logs -f airflow    # tail logs
docker compose build              # rebuild after Dockerfile change
docker compose ps                 # status

# dbt (with venv active + .env loaded)
dbt debug                         # check connection
dbt deps                          # install packages from packages.yml
dbt run                           # build all models
dbt test                          # run tests
dbt run --select staging          # only staging layer
dbt docs generate && dbt docs serve   # browse model docs
```

---

## Branching conventions

- Branch from `main`: `git checkout -b <name>/<feature>` (e.g. `pragya/nrel-ingestion-dag`).
- Open a PR for review before merging.
- If you add a new env var, update **`.env.example`** so others know to set it.
- Coordinate `dbt run` timing in the team chat to avoid two people overwriting each other's tables.
