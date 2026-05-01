# Skills & Project Summary — EV Charging Infrastructure & Adoption Analytics

DATA 226 group project (Group 5). End-to-end data pipeline analyzing US EV charging station coverage and cross-referencing it against state-level EV adoption to surface infrastructure gaps.

**Stack:** Apache Airflow • dbt-snowflake • Snowflake • Docker Compose • Python • GitHub.

**Repo:** https://github.com/ishuapurva1996/EV-dashboard

---

## What's been built so far

### 1. Local development environment
- Created a Python 3.12 virtual environment (`ev_env/`) with `dbt-core 1.11.8` and `dbt-snowflake 1.11.4` for hands-on dbt development outside the container.
- Wired environment variables through a `.env` file (gitignored) so credentials never enter source control.

### 2. Containerized Airflow stack
- Custom **Dockerfile** extending `apache/airflow:2.10.1-python3.12`, pre-installing `apache-airflow-providers-snowflake==5.7.0`, `apache-airflow-providers-docker`, and `dbt-snowflake==1.8.4` against Airflow's official constraints file. Solves the pip dependency-resolution loop that broke the default `_PIP_ADDITIONAL_REQUIREMENTS` runtime install.
- **`docker-compose.yaml`** running:
  - `postgres:13` for Airflow metadata
  - `airflow-init` (one-shot DB migrations + admin user creation)
  - `airflow` running scheduler + webserver under `LocalExecutor`
- Compose injects `.env` into the container via `env_file:`, mounts `dbt/`, `dags/`, `plugins/`, `config/`, `logs/` as volumes, and exposes the UI on `localhost:8081`.

### 3. dbt project
- Scaffolded with `dbt init ev_pipeline`, then **flattened** the auto-generated `dbt/ev_pipeline/` subfolder up to `dbt/` directly (one fewer layer of nesting since this is a single-project repo).
- **`dbt/profiles.yml`** uses `{{ env_var(...) }}` references for every credential — committable to a public repo with zero secrets.
- Same profile works in two contexts:
  - Local Mac venv: env vars sourced via `set -a && . .env && set +a`
  - Airflow container: env vars auto-loaded from `.env` + `DBT_PROFILES_DIR=/opt/airflow/dbt`

### 4. Snowflake setup
- **`snowflake/setup.sql`** — one-shot schema creation script (`RAW_EV`, `CURATED_EV`, `ANALYTICS_EV`) inside the shared `USER_DB_BADGER` database. Run once in the Snowflake worksheet UI.
- Decided on a **shared-DB collaboration model**: all 4 teammates log in with their *own* training users but read/write the same three schemas in `USER_DB_BADGER`. `TRAINING_ROLE` already has cross-DB access, so no explicit grants needed (commented-out grant block included as a fallback).
- Registered the **`snowflake_default`** Airflow connection through Admin → Connections so DAGs can reach Snowflake via `SnowflakeOperator`/`SnowflakeHook`.

### 5. Verification DAGs
- **`dags/test_snowflake_connection.py`** — smoke-test DAG that runs `SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_VERSION()`. Used to verify end-to-end Airflow → Snowflake plumbing. Confirmed working.
- `dbt debug` from the local venv passes against Snowflake — connection healthy in both code paths.

### 6. First ingestion DAG
- **`dags/nrel_stations_ingest.py`** — daily NREL Alternative Fuels Stations ingest:
  1. Cheap call to `/v1/last-updated.json` (one request)
  2. Compares against the `nrel_last_updated` Airflow Variable (high-water-mark stored across runs)
  3. If unchanged → skip via `AirflowSkipException`
  4. Otherwise → full pull (`fuel_type=ELEC`, `country=US`, `status=E,P,T`, `access=public`, `limit=all`) → `TRUNCATE TABLE RAW_EV.NREL_STATIONS` → batched `INSERT` ~65k rows in chunks of 1k → update high-water-mark
- Extracts 19 fields (id, station_name, location, port counts, connector types, network, pricing, status, access, dates, facility metadata).

### 7. GitHub & collaboration
- Initialized git, sanity-checked `.env` exclusion, made first commit, pushed to public repo.
- README covers prerequisites, 6-step first-time setup, daily workflow, troubleshooting matrix, and branching conventions.
- `.env.example` pre-fills shared values and leaves only personal fields blank.
- WhatsApp onboarding message drafted for teammates.

---

## Skills demonstrated

### Data engineering
- Designing a layered analytics warehouse pattern (RAW → CURATED → ANALYTICS).
- Authoring an idempotent ingestion DAG with high-water-mark freshness checks (saves API quota + compute on no-op runs).
- Choosing TRUNCATE+INSERT vs. MERGE based on data volume and refresh cadence.
- Extracting structured columns from a JSON API while preserving option to add a `VARIANT` column later.

### Apache Airflow
- TaskFlow API (`@dag` + `@task` decorators) for readable DAGs over the older `BashOperator`/`PythonOperator` style.
- Using `AirflowSkipException` to short-circuit downstream tasks when no work is needed.
- Persisting state across runs via Airflow `Variable.get` / `Variable.set`.
- Wiring the Snowflake provider's `SnowflakeHook.insert_rows` for batched loading.
- Managing the metadata DB through Postgres + the `airflow-init` bootstrap pattern.

### dbt
- Project initialization with `dbt init`, configuring `dbt_project.yml`, and the standard staging/curated/analytics layout.
- Env-var-driven `profiles.yml` so the same profile works locally and inside Docker without leaking creds.
- Verifying connectivity with `dbt debug`.

### Snowflake
- Schema design across three logical tiers in a single database.
- Role-based access with `TRAINING_ROLE`.
- Authoring idempotent DDL (`CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`).
- Using `CURRENT_USER()` / `CURRENT_ROLE()` / `CURRENT_WAREHOUSE()` for plumbing diagnostics.

### Docker & DevOps
- Extending an official base image (`apache/airflow:2.10.1-python3.12`) with a project-specific Dockerfile.
- Using **Airflow's official constraint file** (`constraints-3.12.txt`) to pin transitive dependencies and avoid pip resolver loops.
- Multi-service compose orchestration with health checks, volumes, and `restart: always` policies.
- Diagnosing port conflicts (8081 collision) and container crash-loops via `docker compose ps` + `logs`.

### Python
- `requests` for REST API ingestion.
- Defensive null handling for inconsistent third-party data (e.g., normalizing empty-string `open_date` to `None`).
- Modular helpers (`_row_from_station`, `_nullify_blank`) to keep DAG task bodies readable.

### Git & collaboration
- Public GitHub repository with clean `.gitignore` excluding secrets and runtime artifacts.
- README-driven onboarding so a teammate can clone → run in 15 minutes.
- `.env.example` template + private secret distribution via DM (rather than committing).
- Schema isolation strategy and `dbt run` coordination etiquette for parallel development.

### Architectural decisions worth noting
- **Why a Dockerfile instead of `_PIP_ADDITIONAL_REQUIREMENTS`?** The runtime install hit pip's exhaustive backtracking resolver on conflicting `snowflake-connector-python` requirements between `apache-airflow-providers-snowflake` and `dbt-snowflake`. Build-time install with the official constraint file converges in seconds.
- **Why pin `dbt-snowflake==1.8.4` in the container but `1.11.4` locally?** Compatibility with the Airflow Snowflake provider's `snowflake-connector-python<4` requirement. Schema/config files are forward-compatible across these minor versions for our use case.
- **Why store the high-water-mark as an Airflow Variable rather than a Snowflake table?** Lower complexity, no extra DDL, persists across container restarts (lives in the Postgres metadata DB), and resets cleanly with `docker compose down -v` if we ever want to force a re-pull.
- **Why flatten `dbt/ev_pipeline/` to `dbt/`?** Single-project repo — the inner folder is convention-driven noise. The parent `dbt/` is already self-documenting.

---

## What's next

| # | Task | Status |
|---|---|---|
| 1 | DOE/AFDC EV registration ingest DAG | Not started |
| 2 | US Census ACS5 population ingest DAG | Not started |
| 3 | dbt staging models (`stg_nrel_stations`, `stg_afdc_registrations`, `stg_census_population`) | Not started |
| 4 | dbt curated layer (deduplicated, normalized fact + dimension tables) | Not started |
| 5 | dbt analytics layer (per-state station density, top-20 cities, gap ranking, stations-per-100k) | Not started |
| 6 | 12-month time-series forecast (EV adoption + infrastructure expansion) | Not started |
| 7 | Preset.io dashboard | Not started |

---

*Last updated: 2026-05-01.*
