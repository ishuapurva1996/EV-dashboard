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

### 9. dbt sources, schema tests, and staging models

The first end-to-end dbt-only layer. Four components: source declarations, a separated test file, the `dim_states` conformed dimension, and the three staging models that produce key-conformed views.

#### 9.1 Source declarations — `dbt/models/staging/_sources.yml`
- Declares all three raw tables under `source: raw_ev` so models can reference them via `{{ source('raw_ev', 'nrel_stations') }}` instead of hardcoding `RAW_EV.NREL_STATIONS`. Enables lineage tracking, refactor safety, and source freshness checks.
- `identifier:` field on each table makes the dbt-alias → Snowflake-table mapping explicit (e.g., `nrel_stations` → `NREL_STATIONS`).
- Source `freshness:` blocks: NREL warns at 2 days / errors at 7 days; Census warns at 400 days / errors at 730 days (annual cadence). AFDC has no freshness check (it's a static seed).
- Full column-level descriptions on every table for auto-generated docs (`dbt docs generate`).
- All data-quality tests intentionally moved to a separate `schema.yml` for readability.

#### 9.2 Schema tests — `dbt/models/staging/schema.yml`
- All staging-layer tests in one file, attached to the staging *models* (not the raw sources). Same coverage as source tests would provide, since staging views are 1:1 with raw.
- Test inventory:
  - `stg_nrel_stations`: `not_null` + `unique` on `station_id`; `not_null` on `updated_at`/`latitude`/`longitude`/`state_abbr`; `accepted_values` on `status_code` (E/P/T)
  - `stg_census_population`: `not_null` + `unique` on `state_fips`; `not_null` on `state_name`/`acs_year`; `population > 0`
  - `stg_afdc_registrations`: composite-key uniqueness on `(state_name, registration_year)`; `not_null` on all four columns; `bev_count >= 0`, `phev_count >= 0`
- Uses `dbt_utils.expression_is_true` and `dbt_utils.unique_combination_of_columns` — required adding `dbt-labs/dbt_utils` to `dbt/packages.yml` and running `dbt deps`.

#### 9.3 `dim_states` — the conformed dimension
- `dbt/seeds/dim_states.csv` — 52 rows: 50 states + DC (FIPS `11`) + Puerto Rico (FIPS `72`).
- Three columns: `state_fips`, `state_name`, `state_abbr`. Each row is the same state expressed in all three "languages."
- Solves the three-way key mismatch: Census uses FIPS (`'06'`), NREL uses 2-letter abbreviations (`'CA'`), AFDC uses full names (`'California'`). Without `dim_states`, the three feeds cannot be joined.
- `+column_types: state_fips: varchar(2)` in `dbt_project.yml` is critical — without it, dbt's seed loader infers `state_fips` as NUMBER and silently strips the leading zero from `'01'`, `'06'`, etc., breaking the join to Census which transmits FIPS as zero-padded strings.
- Loaded via `dbt seed --select dim_states` → lands at `RAW_EV.DIM_STATES`.

#### 9.4 Three staging models — `dbt/models/staging/`
- `stg_census_population.sql` — joins on `state_fips`, gains `state_abbr` + `state_name`.
- `stg_nrel_stations.sql` — `UPPER(state)` defensively, joins on `state_abbr`, gains `state_fips` + `state_name`. Renames `id` → `station_id` for consistent FK naming downstream.
- `stg_afdc_registrations.sql` — joins on `state_name`, gains `state_fips` + `state_abbr`.
- All three follow the same CTE pattern: `source → states → renamed → select`.
- Materialized as views (per `dbt_project.yml`) — zero storage cost, always reflect latest raw data.
- **Key conformance**: after staging, every model exposes the *same three state keys* (`state_fips`, `state_abbr`, `state_name`). Downstream curated/analytics models can join on whichever key is most convenient.
- **INNER JOIN, not LEFT JOIN**: filters territory rows (Guam, USVI, AS, MP) at the staging boundary. The contract is "this view contains only US states + DC + PR." Anything outside that scope is dropped here, not silently propagated to analytics where it would skew per-state aggregates.

#### 9.5 Build + validate
- `dbt build --select staging --profiles-dir .` runs seeds + models + tests in dependency order. One command, full pipeline validation.
- The `--profiles-dir .` flag overrides `DBT_PROFILES_DIR=/opt/airflow/dbt` from `.env` (which is the correct container path but not the local Mac path).

### 10. Curated layer — conformed dimension + three fact tables

End-to-end build complete: dim_states promoted to CURATED_EV, three fact tables materialized, 41 curated-layer tests passing.

#### 10.1 Promoted `dim_states` from RAW_EV → CURATED_EV
- Changed `+schema:` for the dim_states seed in `dbt_project.yml`. Seed CSV still lives at `dbt/seeds/dim_states.csv`; only the destination schema moved.
- Rationale: dim_states is hand-curated reference data, not external feed output. CURATED_EV is its semantically correct home.
- Old `RAW_EV.DIM_STATES` dropped manually with `DROP TABLE IF EXISTS`.
- **Lazy-resolution gotcha caught:** Snowflake views resolve table references at query time, not creation time. Existing staging views were compiled to read `RAW_EV.DIM_STATES`; after the drop, querying any staging view failed with "object does not exist" until we ran `dbt run --select staging` to recompile against the new location. Lesson: after relocating any upstream table, rebuild downstream views before assuming the pipeline still works.

#### 10.2 `fct_stations_by_state` — current snapshot
- Grain: one row per US state (52 rows: 50 + DC + PR).
- Source: `stg_nrel_stations`. No filter on `open_date` — includes the ~5% of stations with NULL open dates (we want the most accurate "what exists today" count).
- Aggregations: total stations, status breakdown (open/planned/temporary), per-tier port counts (L1/L2/DCFast), grand-total ports, distinct network operators, earliest/latest open dates.
- Materialized as a table (per `dbt_project.yml` curated config).
- **Defensive arithmetic on `total_ports`:** `SUM(coalesce(level1, 0) + coalesce(level2, 0) + coalesce(dcfast, 0))`. Without COALESCE, any NULL tier inside the addition would propagate (NULL + 6 = NULL) and corrupt the sum. SUM-ignoring-nulls only protects you when each tier is summed independently — not inside an arithmetic expression.

#### 10.3 `fct_ev_adoption_by_state_year` — adoption time-series
- Grain: one row per state per year (255 rows: 51 states × 5 AFDC years 2020–2024).
- Source: `stg_afdc_registrations`.
- Derived metrics: `total_ev_count = bev + phev`, `bev_share` and `phev_share` (forced float division via `* 1.0`), `prior_year_total` (via LAG), `yoy_growth_pct = (current - prior) / prior * 100`.
- **Window function pattern:** `LAG(total_ev_count) OVER (PARTITION BY state_fips ORDER BY registration_year)`. Partition resets the running view at each new state; ORDER BY defines what "previous row" means within the partition. Without partitioning, NY's 2021 LAG would pull CA's 2020 value.
- NULL handling: `bev_share`/`phev_share` are NULL when `total_ev_count = 0` (defensive guard; doesn't actually trigger for AFDC data); `prior_year_total` and `yoy_growth_pct` are NULL for the earliest year per state (no prior to compare).

#### 10.4 `fct_stations_by_state_year` — infrastructure time-series
- Grain: one row per state per year, 1995–current year (1,664 rows: 52 × 32).
- Source: `stg_nrel_stations` filtered to `open_date IS NOT NULL AND status_code IN ('E', 'T')`. Drops the ~5% of undated stations (can't place on timeline) and excludes Planned status (not yet operational).
- **Two metric families per row:**
  - *Flow* (`new_*`): stations and ports that opened *in* that year. `GROUP BY year(open_date)` produces sparse per-year aggregates.
  - *Stock* (`cumulative_*`): running sums via `SUM(...) OVER (PARTITION BY state_fips ORDER BY vintage_year)` — the default frame `RANGE UNBOUNDED PRECEDING TO CURRENT ROW` makes this a true running cumulative.
- **Complete (state × year) grid construction** — the standard pattern for any time-series fact:
  1. `year_spine` CTE: 1995 → current year using `TABLE(GENERATOR(rowcount => 100))` + `SEQ4()` + `WHERE vintage_year <= year(current_date())`. Generator margin (100 vs ~32 needed) avoids annual maintenance.
  2. `state_year_grid` = `dim_states CROSS JOIN year_spine` → dense 52 × 32 = 1,664-row frame
  3. `LEFT JOIN` sparse `new_per_year` aggregates onto the dense grid; `COALESCE(..., 0)` zero-fills gaps
  4. Window function computes cumulative SUMs across the now-contiguous time series
- **Why the grid matters:** forecasting models require contiguous time. Without it, Wyoming's 2019 row (no new stations) would be missing entirely → charts gap → LAG/window functions either error or interpolate falsely.
- **Snowflake gotcha caught:** initial year_spine used `QUALIFY vintage_year <= year(current_date())`. Snowflake rejects QUALIFY without a window function. Refactored to wrap in a subquery and use `WHERE`.

#### 10.5 Curated schema tests — `dbt/models/curated/schema.yml`
- 41 tests across the three facts. Coverage:
  - `unique` + `not_null` on PKs (state_fips for snapshot facts, composite for time-series)
  - `relationships` from every fact's `state_fips` → `dim_states.state_fips` — proves no orphan keys (the conformed dimension contract)
  - `dbt_utils.unique_combination_of_columns` for composite PKs `(state_fips, year)`
  - `dbt_utils.expression_is_true` for arithmetic consistency:
    - `total_ports = total_level1_ports + total_level2_ports + total_dcfast_ports`
    - `total_ev_count = bev_count + phev_count`
    - `cumulative_total_ports = cumulative_level1 + cumulative_level2 + cumulative_dcfast`
    - `cumulative_stations >= new_stations` (monotonic running sum)
    - `bev_share between 0 and 1`, `phev_share between 0 and 1`
    - All count columns `>= 0`

### 11. GitHub & collaboration
- Initialized git, sanity-checked `.env` exclusion, made first commit, pushed to public repo.
- README covers prerequisites, 6-step first-time setup, daily workflow, troubleshooting matrix, and branching conventions.
- `.env.example` pre-fills shared values and leaves only personal fields blank.
- WhatsApp onboarding message drafted for teammates.
- Clean commit history to date:
  - `b77001a` Switch to shared-DB collaboration model
  - `ae3dce3` Rename schemas to *_EV; add NREL ingest DAG + skills.md
  - `e25d681` Add Census ACS5 population ingest; rename NREL DAG for consistency
  - `c30fa9c` Add AFDC seed, schema-routing macro, dbt model scaffolding
  - `ec83940` Build dbt staging layer: sources, schema tests, dim_states seed, three stg_ models
  - (next commit) Build curated layer: promote dim_states + 3 fact tables + 41 tests

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
- **`{{ source(...) }}` references** instead of hardcoded `schema.table` names — enables lineage tracking, source freshness checks, and one-line refactor when raw locations change.
- **`identifier:`** on source tables to make the dbt-alias → Snowflake-table mapping explicit and protect against case-sensitivity surprises.
- **Source freshness** with warn/error windows tied to source `loaded_at_field` timestamps.
- **Splitting source declarations from tests** — `_sources.yml` for what exists, `schema.yml` for what must be true.
- **`dbt_utils` package** for tests beyond dbt-core (`expression_is_true`, `unique_combination_of_columns`).
- **Conformed dimension pattern** — `dim_states` as a Rosetta Stone joining sources that use different keys (FIPS vs abbreviation vs full name).
- **`+column_types` on seeds** to preserve string formatting (zero-padded FIPS codes) that dbt's type inference would otherwise destroy.
- **CTE pattern for staging models** — `source → ref → renamed → select` for readability and easy debugging.
- **Defensive normalization at the staging boundary** (`UPPER()` on third-party string keys) to absorb upstream casing variance.
- `dbt build --select <selector>` for one-shot run+test in dependency order; `--profiles-dir .` to override container env vars on the local Mac.
- Verifying connectivity with `dbt debug`; loading data with `dbt seed --select <name>`.
- **Star-schema fact tables** — current-snapshot fact (`fct_stations_by_state`), per-period flow + stock fact (`fct_stations_by_state_year`, `fct_ev_adoption_by_state_year`). Two facts for the same entity coexist when they answer different questions ("what exists now?" vs "how did it change?").
- **`relationships` tests** as the conformed-dimension contract — every fact's foreign key must resolve to a row in `dim_states`.
- **Window functions for time-series**:
  - `LAG()` for prior-period lookups (year-over-year growth)
  - `SUM(...) OVER (PARTITION BY ... ORDER BY ...)` for running cumulative sums
  - Default frame `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` produces true cumulative behavior
- **Date-spine pattern** — `TABLE(GENERATOR(rowcount => N))` + `SEQ4()` to build a contiguous year/date series, then `CROSS JOIN dim_states` for a complete grid that fills LEFT-JOIN gaps with `COALESCE(..., 0)`. Required for any forecasting model that assumes contiguous time.
- **Defensive `COALESCE` inside arithmetic** — `coalesce(a, 0) + coalesce(b, 0)` prevents one NULL from poisoning the whole expression. SUM-ignoring-nulls only protects per-column aggregation, not in-expression addition.
- **Monotonic-cumulative consistency tests** (`cumulative_X >= new_X`) and **per-tier sum tests** (`total = level1 + level2 + level3`) — catch silent bugs in window-function arithmetic.

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
- **Why split `_sources.yml` and `schema.yml`?** Source declarations are reference material (what columns exist, what they mean); tests are operational contracts (what must be true). Splitting keeps each file scannable for its own purpose. dbt requires source-column tests to live inside the source declaration, so we instead attach the tests to the staging models — same coverage since staging is 1:1 with raw.
- **Why INNER JOIN to `dim_states` instead of LEFT JOIN at staging?** INNER JOIN treats the `dim_states` membership as a filter: any row whose state isn't in our recognized set (50 + DC + PR) gets dropped at the staging boundary. The contract becomes explicit ("this view is US states only"), unrecognized states surface immediately rather than skewing downstream aggregates, and adding a territory means one new line in `dim_states.csv` — no model changes.
- **Why `UPPER(s.state)` on NREL?** Snowflake string comparisons are case-sensitive. NREL *should* always send uppercase 2-letter codes, but it's a third-party API we don't control. `UPPER()` normalizes at the join boundary so a future bad batch (`'ca'` instead of `'CA'`) doesn't silently drop rows from the join.
- **Why preserve leading zeros on `state_fips`?** FIPS codes are zero-padded *strings*, not numbers. Census transmits them as `'01'`, `'06'`, etc. If `dim_states.state_fips` becomes the integer `1`, the join `'01' = 1` fails. The `+column_types: state_fips: varchar(2)` config in `dbt_project.yml` forces dbt to load the seed column as VARCHAR.
- **Why `dbt seed` for `dim_states` rather than a hand-written SQL model with UNION ALLs?** 52 rows of static reference data, version-controlled via PR review. A SQL `UNION ALL` would be 52 lines of repetition with no upside. Same reasoning as AFDC: dbt seed is purpose-built for slow-moving small reference data.
- **Why two station fact tables (`fct_stations_by_state` snapshot and `fct_stations_by_state_year` time-series)?** They answer different questions and have different filtering needs. The snapshot includes ALL stations (the most accurate "what exists today" count); the time-series excludes ~5% of stations with NULL `open_date` (can't place undated rows on a timeline). Forcing one table to do both jobs would mean either undercounting the current state or fabricating fake open dates. Both tables coexist; downstream queries pick the one that fits.
- **Why filter NULL `open_date` in the time-series fact instead of including them?** Two bad alternatives: include in latest year (creates a false 5% spike on the last data point that corrupts forecasts), or distribute across all years (fabricates history that didn't happen). Dropping them is the only honest choice. The 5% gap is documented in the model description and bounded by `fct_stations_by_state` (which keeps them).
- **Why a complete (state × year) grid via `dim_states CROSS JOIN year_spine` instead of just aggregating where data exists?** Sparse aggregations break time-series analysis. If Wyoming had no station openings in 2019, a `GROUP BY year` produces no row → cumulative SUM has nothing to anchor → forecasting models error or interpolate wildly. The grid forces all 32 years to exist for every state; LEFT JOIN + COALESCE zero-fills empty cells. ~1,600 rows, trivially small, eliminates an entire class of downstream bugs.
- **Why include both flow (`new_*`) and stock (`cumulative_*`) columns in the same time-series fact?** Both are needed for analytics — flow for "growth rate per year," stock for "stations per 100k people in 2022." Computing one from the other on demand requires a window function in every query. Materializing both pays the compute cost once at build time and serves all reads instantly. Fact tables are caches of pre-computed analytics — that's their job.
- **Why promote `dim_states` from RAW_EV to CURATED_EV (and not leave it where the seed initially landed)?** Conformed dimensions are *derived* artifacts even when the source is a hand-curated CSV — they encode the business decision of "which states do we recognize?" That decision belongs in the curated tier alongside facts, not in the raw tier where external feeds land. Caused a one-time stale-view bug (Snowflake views resolve table references lazily) — fixed by re-running staging models after the move.

---

## What's next

| # | Task | Status |
|---|---|---|
| 1 | NREL Alternative Fuels Stations ingest DAG | ✅ Done |
| 2 | US Census ACS5 population ingest DAG | ✅ Done |
| 3 | DOE/AFDC EV registration ingest (dbt seed) | ✅ Done |
| 4 | dbt sources YAML (`models/staging/_sources.yml`) declaring the 3 raw tables | ✅ Done |
| 5 | `dim_states` dbt seed — FIPS ↔ state name ↔ 2-letter abbreviation lookup | ✅ Done |
| 6 | dbt staging models (`stg_nrel_stations`, `stg_census_population`, `stg_afdc_registrations`) | ✅ Done |
| 7 | dbt curated layer (deduplicated, normalized fact + dimension tables) | ✅ Done |
| 8 | dbt analytics layer (per-state station density, top-20 cities, gap ranking, stations-per-100k) | Not started |
| 9 | 12-month time-series forecast (EV adoption + infrastructure expansion) | Not started |
| 10 | Preset.io dashboard | Not started |

---

*Last updated: 2026-05-04.*
