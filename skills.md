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
  - Local Mac venv: env vars sourced via `set -a && . .env && set +a` (with `--profiles-dir .` to override the container path)
  - Airflow container: env vars auto-loaded from `.env` + `DBT_PROFILES_DIR=/opt/airflow/dbt`
- Folder structure follows a **layered architecture** (see section 8 below):
  ```
  dbt/
  ├── seeds/         — CSV files loaded into RAW_EV
  ├── macros/        — custom Jinja macros (generate_schema_name override)
  ├── models/
  │   ├── staging/   → STAGING_EV  (views; one-to-one with raw)
  │   ├── curated/   → CURATED_EV  (tables; joined facts/dims)
  │   └── analytics/ → ANALYTICS_EV (tables; dashboard-ready aggregates)
  ```

### 4. Snowflake setup
- **`snowflake/setup.sql`** — one-shot schema creation script for **four** schemas inside the shared `USER_DB_BADGER` database: `RAW_EV`, `STAGING_EV`, `CURATED_EV`, `ANALYTICS_EV`. Run once in the Snowflake worksheet UI.
- Decided on a **shared-DB collaboration model**: all 4 teammates log in with their *own* training users but read/write the same four schemas in `USER_DB_BADGER`. `TRAINING_ROLE` already has cross-DB access, so no explicit grants needed (commented-out grant block included as a fallback).
- Registered the **`snowflake_default`** Airflow connection through Admin → Connections so DAGs can reach Snowflake via `SnowflakeOperator`/`SnowflakeHook`.

### 5. Verification DAGs
- **`dags/test_snowflake_connection.py`** — smoke-test DAG that runs `SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_VERSION()`. Used to verify end-to-end Airflow → Snowflake plumbing. Confirmed working.
- `dbt debug` from the local venv passes against Snowflake — connection healthy in both code paths.

### 6. Ingestion DAGs (RAW layer)

#### 6.1 NREL Alternative Fuels Stations — daily, real-time API
- **`dags/ingest_station_data.py`** (formerly `nrel_stations_ingest.py`):
  1. Cheap call to `/v1/last-updated.json` (one request)
  2. Compares against the `nrel_last_updated` Airflow Variable (high-water-mark stored across runs)
  3. If unchanged → skip via `AirflowSkipException`
  4. Otherwise → full pull (`fuel_type=ELEC`, `country=US`, `status=E,P,T`, `access=public`, `limit=all`) → `TRUNCATE TABLE RAW_EV.NREL_STATIONS` → batched `INSERT` ~65k rows in chunks of 1k → update high-water-mark
- Extracts 19 fields (id, station_name, location, port counts, connector types, network, pricing, status, access, dates, facility metadata).

#### 6.2 US Census ACS5 state population — annual, REST API
- **`dags/ingest_state_population_data.py`** — built today using the TaskFlow API:
  - `extract_census_population` task pulls `NAME` + `B01003_001E` (total population) for `for=state:*` from the 2024 ACS5 vintage. Uses `CENSUS_API_KEY` from `.env`.
  - `transform_census_population_data` task converts the 2D-array JSON response (header row + 52 data rows) into clean dicts, casting `population` to `int` and stamping `acs_year` + `loaded_at`.
  - `load_population_data_into_snowflake` task wraps the load in `BEGIN`/`COMMIT`/`ROLLBACK` for atomic full-refresh: `CREATE TABLE IF NOT EXISTS` → `DELETE FROM` → `executemany` INSERT 52 rows.
- Schedule: `@yearly` (Census ACS5 publishes once a year in December).
- Schema:
  ```sql
  RAW_EV.CENSUS_POPULATION (
    state_fips    VARCHAR(2),     -- '01' = Alabama, '11' = DC, '72' = Puerto Rico
    state_name    VARCHAR,
    population    BIGINT,
    acs_year      NUMBER(4),
    loaded_at     TIMESTAMP_NTZ
  )
  ```
- Verified end-to-end: 52 rows loaded, populations match Census's published figures.

### 7. AFDC EV registrations as a dbt seed (RAW layer)
- **`dbt/seeds/afdc_registrations.csv`** — manually curated 5-year CSV (2020–2024) with 255 rows (51 states/DC × 5 years). Built today.
- Three columns from the source: `state_name`, `bev_count` (Electric EV), `phev_count` (Plug-In Hybrid Electric). HEV (non-plug-in hybrid) intentionally excluded — those don't use charging stations.
- Long-format with year as a column rather than 5 separate files: easier annual update (append 51 rows), single source of truth, time-series analysis is trivial without a UNION ALL staging model.
- Loaded via `dbt seed --select afdc_registrations`. Lands at `USER_DB_BADGER.RAW_EV.AFDC_REGISTRATIONS` thanks to the schema-routing macro (section 8).
- A teammate also loaded a parallel set of 5 yearly tables (`RAW_EV.AFDC_2020 ... AFDC_2024`) directly to Snowflake. Those are kept as a backup — the canonical source for downstream models is the unified seed table.

### 8. dbt project structure & schema routing
- **`dbt/macros/generate_schema_name.sql`** — custom Jinja override of dbt's default schema-naming logic. By default dbt builds the schema as `<target.schema>_<custom_schema>`, which would route everything into `ANALYTICS_EV_RAW_EV`, `ANALYTICS_EV_STAGING_EV`, etc. The override uses the custom schema as-is when set, falling back to `target.schema` otherwise. Six lines of Jinja:
  ```sql
  {% macro generate_schema_name(custom_schema_name, node) -%}
      {%- if custom_schema_name is none -%}
          {{ target.schema }}
      {%- else -%}
          {{ custom_schema_name | trim }}
      {%- endif -%}
  {%- endmacro %}
  ```
- **`dbt/dbt_project.yml`** — configured for the four-schema layered architecture:
  ```yaml
  models:
    ev_pipeline:
      staging:   { +materialized: view,  +schema: STAGING_EV }
      curated:   { +materialized: table, +schema: CURATED_EV }
      analytics: { +materialized: table, +schema: ANALYTICS_EV }
  seeds:
    ev_pipeline:
      afdc_registrations: { +schema: RAW_EV }
  ```
- Default `models/example/` folder (from `dbt init`) deleted; replaced with the three-folder layered structure.

### 9. GitHub & collaboration
- Initialized git, sanity-checked `.env` exclusion, made first commit, pushed to public repo.
- README covers prerequisites, 6-step first-time setup, daily workflow, troubleshooting matrix, and branching conventions.
- `.env.example` pre-fills shared values and leaves only personal fields blank.
- WhatsApp onboarding message drafted for teammates.
- Two clean commits to date:
  - `b77001a` Switch to shared-DB collaboration model
  - `ae3dce3` Rename schemas to *_EV; add NREL ingest DAG + skills.md
  - `e25d681` Add Census ACS5 population ingest; rename NREL DAG for consistency
- Pending uncommitted work: AFDC seed CSV, generate_schema_name macro, dbt_project.yml updates, model folder scaffolding, STAGING_EV in setup.sql.

---

## Skills demonstrated

### Data engineering
- Designing a layered analytics warehouse pattern (RAW → STAGING → CURATED → ANALYTICS).
- **Choosing the right ingestion pattern per source**: DAG with high-water-mark for real-time API (NREL), TaskFlow DAG with annual schedule for clean REST API (Census), dbt seed with manually curated CSV for an HTML-only annual source (AFDC). Three patterns, three sources, each justified by source mutability and cadence.
- Authoring idempotent ingestion: high-water-mark Variables, `TRUNCATE+INSERT` full refresh, transactional load with explicit `BEGIN`/`COMMIT`/`ROLLBACK`.
- Schema design with explicit type casting at ingest boundary (string → BIGINT for Census population strings, removed-comma int parsing for AFDC values).

### Apache Airflow
- TaskFlow API (`@dag` + `@task` decorators) for readable DAGs over the older `BashOperator`/`PythonOperator` style.
- Hybrid pattern: `with DAG(...) as dag:` context manager combined with `@task`-decorated functions — fully supported, dependencies wire automatically.
- Using `AirflowSkipException` to short-circuit downstream tasks when no work is needed (NREL DAG only).
- Persisting state across runs via Airflow `Variable.get` / `Variable.set`.
- Wiring the Snowflake provider's `SnowflakeHook.get_conn().cursor()` for transactional batched loading.
- Managing the metadata DB through Postgres + the `airflow-init` bootstrap pattern.

### dbt
- Project initialization with `dbt init`, configuring `dbt_project.yml`, and a four-schema staging/curated/analytics layout.
- Env-var-driven `profiles.yml` so the same profile works locally and inside Docker without leaking creds.
- **Custom `generate_schema_name` macro** to override dbt's default `<target>_<custom>` concatenation — required for clean schema names in shared training environments.
- **dbt seeds** for slow-changing reference data: schema routing via `+schema:`, explicit column types via `+column_types`, type inference fallback for clean CSVs.
- Verifying connectivity with `dbt debug`; loading data with `dbt seed --select <name>`.

### Snowflake
- Schema design across four logical tiers in a single shared database.
- Role-based access with `TRAINING_ROLE`.
- Authoring idempotent DDL (`CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`).
- Using `CURRENT_USER()` / `CURRENT_ROLE()` / `CURRENT_WAREHOUSE()` for plumbing diagnostics.
- Understanding FIPS state codes (non-sequential 01–56 for states, 11 for DC, 72 for PR) as the canonical state join key.

### Docker & DevOps
- Extending an official base image (`apache/airflow:2.10.1-python3.12`) with a project-specific Dockerfile.
- Using **Airflow's official constraint file** (`constraints-3.12.txt`) to pin transitive dependencies and avoid pip resolver loops.
- Multi-service compose orchestration with health checks, volumes, and `restart: always` policies.
- Diagnosing port conflicts (8081 collision) and container crash-loops via `docker compose ps` + `logs`.

### Python
- `requests` for REST API ingestion; parameterized URL building via `params=` dict (URL-safe encoding, no string-concat bugs).
- Defensive null handling for inconsistent third-party data (e.g., normalizing empty-string `open_date` to `None`, casting `"5108468"` strings to `int`).
- List comprehension for dict→tuple transformation at the SQL parameter-binding boundary.
- Modular helpers (`_row_from_station`, `_nullify_blank`) to keep DAG task bodies readable.

### Git & collaboration
- Public GitHub repository with clean `.gitignore` excluding secrets and runtime artifacts.
- README-driven onboarding so a teammate can clone → run in 15 minutes.
- `.env.example` template + private secret distribution via DM (rather than committing).
- Schema isolation strategy and `dbt run` coordination etiquette for parallel development.
- Logical commits: each commit captures a coherent unit (rename + new ingest, or schema migration), with descriptive multi-line messages.

### Architectural decisions worth noting
- **Why a Dockerfile instead of `_PIP_ADDITIONAL_REQUIREMENTS`?** The runtime install hit pip's exhaustive backtracking resolver on conflicting `snowflake-connector-python` requirements between `apache-airflow-providers-snowflake` and `dbt-snowflake`. Build-time install with the official constraint file converges in seconds.
- **Why pin `dbt-snowflake==1.8.4` in the container but `1.11.4` locally?** Compatibility with the Airflow Snowflake provider's `snowflake-connector-python<4` requirement. Schema/config files are forward-compatible across these minor versions for our use case.
- **Why store the high-water-mark as an Airflow Variable rather than a Snowflake table?** Lower complexity, no extra DDL, persists across container restarts (lives in the Postgres metadata DB), and resets cleanly with `docker compose down -v` if we ever want to force a re-pull.
- **Why flatten `dbt/ev_pipeline/` to `dbt/`?** Single-project repo — the inner folder is convention-driven noise.
- **Why dbt seed for AFDC instead of an Airflow DAG?** No reliable download URL or API exists; the data is a static HTML table updated once per year. Scraping in production is brittle, while a manually maintained CSV in git is simple, audited via PRs, and trivially refreshable. dbt seed is purpose-built for exactly this kind of slow, small reference data.
- **Why one combined CSV vs five files (one per year)?** The data has stable schema, is updated annually as a batch, and is used together for time-series analysis. Splitting would create five Snowflake tables, force a `UNION ALL` in staging, and require five PRs/year to update. A single 255-row CSV with a `registration_year` column avoids all of that.
- **Why a custom `generate_schema_name` macro?** dbt's default behavior concatenates `<target.schema>_<custom>`, which would map our intended `RAW_EV` to `ANALYTICS_EV_RAW_EV`. Standard dbt override pattern; six lines of Jinja eliminate the schema-sprawl problem.
- **Why the `_EV` suffix on every schema (RAW_EV, STAGING_EV, CURATED_EV, ANALYTICS_EV)?** Visual symmetry when scanning Snowflake's schema list, and clear separation from any non-EV schemas teammates might use in the same shared training database.
- **Why include Puerto Rico (FIPS 72) in raw, but plan to filter in analytics?** Census ACS5 returns it; NREL has stations there; AFDC excludes it. Keeping it in RAW preserves source fidelity; filtering in the analytics layer keeps "US states" comparisons clean while preserving the option to add territories later.

---

## What's next

| # | Task | Status |
|---|---|---|
| 1 | NREL Alternative Fuels Stations ingest DAG | ✅ Done |
| 2 | US Census ACS5 population ingest DAG | ✅ Done |
| 3 | DOE/AFDC EV registration ingest (dbt seed) | ✅ Done |
| 4 | dbt sources YAML (`models/staging/_sources.yml`) declaring the 3 raw tables | Not started |
| 5 | `dim_states` dbt model — FIPS ↔ state name ↔ 2-letter abbreviation lookup | Not started |
| 6 | dbt staging models (`stg_nrel_stations`, `stg_census_population`, `stg_afdc_registrations`) | Not started |
| 7 | dbt curated layer (deduplicated, normalized fact + dimension tables) | Not started |
| 8 | dbt analytics layer (per-state station density, top-20 cities, gap ranking, stations-per-100k) | Not started |
| 9 | 12-month time-series forecast (EV adoption + infrastructure expansion) | Not started |
| 10 | Preset.io dashboard | Not started |

---

*Last updated: 2026-05-04.*
