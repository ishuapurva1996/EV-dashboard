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

### 11. Analytics layer — dashboard-ready marts

End-to-end analytics layer complete: 4 marts spanning state snapshot, time-series, regional rollup, and city-level ranking, plus 1 supporting curated fact and a `census_region` column added to `dim_states`. 132 tests passing across the curated + analytics layers.

#### 11.1 First-pass design — two marts
- **`mart_state_ev_overview`** — one row per state (latest snapshot). Joins `fct_stations_by_state` (current snapshot) with the most recent year of `fct_ev_adoption_by_state_year` and `stg_census_population` per state. Powers KPI tiles, choropleths, scatter plots, and the leaderboard.
- **`mart_ev_growth_trends`** — one row per (state, year). Joins `fct_stations_by_state_year` with `fct_ev_adoption_by_state_year` on `(state_fips, year)`, with population held at the latest ACS year. Powers all time-series charts.
- **Initially scoped to 3 marts; collapsed to 2.** A separate "station density" mart would have re-joined the same tables to produce the same ratios already in the overview mart — a violation of DRY without operational benefit. Density columns live in `mart_state_ev_overview`.
- **"Latest per state" via `qualify row_number()`** — `qualify row_number() over (partition by state_fips order by registration_year desc) = 1` lets each state pick up its own most-recent AFDC year and ACS year independently. Robust to per-state data gaps; cleaner than a self-join on a max() subquery.

#### 11.2 Reference-driven expansion
- Surveyed [singhpriyanshu5/us-ev-charging-stations-dashboard](https://github.com/singhpriyanshu5/us-ev-charging-stations-dashboard) — same stack (Airflow + dbt + Snowflake), same three sources (NREL, Census ACS5, EV registrations). Their dashboard exposed five chart angles missing from the initial design; all five added in this phase.
- **Added `census_region` to `dim_states.csv`** — 4 official US Census regions (Northeast / Midwest / South / West) plus a `Territory` bucket for Puerto Rico. Standard, defensible, single column on the conformed dimension. Required:
  - Updating `+column_types` in `dbt_project.yml` to type the new column as `varchar`.
  - Re-seeding via `dbt seed --select dim_states --full-refresh` (52 rows reloaded).
- **Added `stations_with_dcfast` to `fct_stations_by_state`** — count of stations with at least one DC fast port (`count(case when coalesce(ev_dc_fast_num, 0) > 0 then 1 end)`). Distinct from `total_dcfast_ports` (port count, not station count) — a station with 4 DC fast ports counts once toward `stations_with_dcfast`, four times toward `total_dcfast_ports`.

#### 11.3 New curated fact — `fct_stations_by_city`
- Grain: `(state_fips, city)`. ~9k rows from ~85k stations.
- Same column shape as `fct_stations_by_state` (counts + per-tier ports), one level deeper.
- **City-name normalization gotcha**: NREL transmits city names with inconsistent case and whitespace (`"san francisco"`, `"San Francisco "`). `initcap(trim(city))` collapses variants without losing display readability. Empty/null cities are dropped at this layer — the contract is "named cities only."
- Tests: composite uniqueness on `(state_fips, city)`, FK to `dim_states`, identity test on `total_ports = level1 + level2 + dcfast`, monotonic test on `stations_with_dcfast <= total_stations`.

#### 11.4 Two new analytics marts
- **`mart_stations_by_region`** — one row per Census region (5 rows). Aggregates state-level metrics from `mart_state_ev_overview` rather than re-querying the curated facts. **Mart-of-marts pattern**: when a rollup is just sums + ratios over an existing mart's columns, the cheaper path is to layer the new mart on the existing one. Single source of truth for the per-state inputs; no risk of the two marts disagreeing on, say, `total_ev_count` due to subtle filter drift.
- **`mart_top_cities`** — pass-through of `fct_stations_by_city` with `national_rank` and `state_rank` columns precomputed via `row_number() over (...)`. Dashboard query is `where national_rank <= 20` instead of an order-by-limit at read time. National rank is unique (tie-break alphabetically by city); state rank resets per state.

#### 11.5 Expanded `mart_state_ev_overview`
- Added 3 columns:
  - `census_region` (joined from `dim_states`)
  - `dcfast_penetration_pct` = `stations_with_dcfast × 100 / total_stations` — share of stations offering any DC fast charging
  - `avg_l2_per_open_station` = `total_level2_ports / stations_open` — distinguishes hub-style deployments (many ports per site) from single-port installs
- Passed through `stations_with_dcfast` for downstream rollup use.
- Joins to `dim_states` via INNER JOIN (region must be present); to AFDC and Census via LEFT JOIN (states with no AFDC row still get a station snapshot).

#### 11.6 Final layer counts
- **8 tables built**, **124 tests passing**, **0 errors** via `dbt build --select curated analytics`.
- Row-count sanity check via `dbt show`:
  - `fct_stations_by_city`: 8,993 rows
  - `mart_state_ev_overview`: 52 rows (50 states + DC + PR)
  - `mart_stations_by_region`: 5 rows (Northeast / Midwest / South / West / Territory)
  - `mart_top_cities`: 8,993 rows
- **Snowflake reserved-word gotcha caught**: initial inline `dbt show` query used `count(*) as rows` — Snowflake rejected `rows` as a column alias (reserved). Renamed to `row_count`.

### 12. Airflow ↔ dbt orchestration

End-to-end pipeline closure: NREL ingest now triggers a downstream dbt DAG so the analytics marts refresh automatically when new station data lands.

#### 12.1 `dags/ev_dbt_dag.py` — the dbt orchestration DAG
- Three sequential `BashOperator` tasks: `dbt_seed → dbt_run → dbt_test`. Mirrors the structure of the cross-project reference at `weather-forecasting-pipeline/dags/weather_dbt_dag.py`.
- `schedule=None` — runs only when triggered by upstream DAGs (or manually). No autonomous schedule.
- **Snowflake creds templated via `default_args.env`** — Jinja-templated from the existing `snowflake_default` Airflow connection that the ingest DAGs already use:
  ```python
  "env": {
      "SNOWFLAKE_USER":      "{{ conn.snowflake_default.login }}",
      "SNOWFLAKE_PASSWORD":  "{{ conn.snowflake_default.password }}",
      "SNOWFLAKE_ACCOUNT":   "{{ conn.snowflake_default.extra_dejson.account }}",
      ...
  }
  ```
  Variable names match the existing `dbt/profiles.yml` (which uses `SNOWFLAKE_*`). Single source of truth for Snowflake credentials — change it in one place (Airflow Connections), every DAG picks it up.
- **`append_env=True`** on each `BashOperator` — keeps the container's `PATH` so plain `dbt` resolves through the airflow user's pip-installed binary. Without it, `BashOperator.env` *replaces* the inherited environment.
- **`--project-dir` and `--profiles-dir` flags**, both pointing at `/opt/airflow/dbt` (the mounted dbt project root). Avoids `cd`-ing in the bash command and makes the working dir explicit.

#### 12.2 NREL → dbt chain
- Added `TriggerDagRunOperator(task_id="trigger_dbt", trigger_dag_id="ev_dbt_pipeline")` to `dags/ingest_station_data.py`. The import for `TriggerDagRunOperator` was already present at the top of the file (added speculatively in an earlier commit but unused) — now actually wired up.
- Final task chain: `current_ts >> loaded >> final >> trigger_dbt`.
- **Default trigger rule (`all_success`) — skip propagates**: when `check_last_updated` raises `AirflowSkipException` (NREL data unchanged since last run), the skip cascades through `loaded`, `final`, and `trigger_dbt`. Result: dbt only re-runs on days when NREL actually published new data, not every day at 02:30 regardless. This is the correct economic behavior — re-materializing 8 marts on stale source data is pure waste.
- **Why a downstream chain instead of a separate schedule?** Coupling dbt to the upstream ingest cadence is correct: dbt's purpose is to refresh marts *because* new data arrived. A separate cron schedule would either fire too early (before NREL completes) or too late (marts stale until the next tick). The trigger-on-success pattern is exactly what we want.

### 13. GitHub & collaboration
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
  - `4a0183e` Build curated layer: promote dim_states + 3 fact tables + 41 tests
  - (next commit) Build analytics layer + Airflow↔dbt orchestration: 4 marts, region column, city fact, ev_dbt_pipeline DAG, NREL trigger

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
- **Orchestrating dbt from Airflow** via `BashOperator` running `dbt seed`/`dbt run`/`dbt test` against the mounted project. Lighter-weight than `cosmos` for a small project; matches the established cross-project pattern.
- **Templating Snowflake creds from an Airflow connection** via `default_args.env` with Jinja `{{ conn.snowflake_default.login }}` etc. Single source of truth: the Airflow Connections UI. dbt picks the same creds the ingest DAGs use without a parallel `.env` path.
- **`append_env=True` on `BashOperator`** — `BashOperator.env` *replaces* the inherited environment by default. Setting `append_env=True` merges templated env vars onto the container's existing env so `PATH`, `DBT_PROFILES_DIR`, etc. survive.
- **`TriggerDagRunOperator` for cross-DAG chaining** — at the end of the NREL ingest DAG, fires `ev_dbt_pipeline` so the marts refresh on the same trigger as the data they depend on.
- **Skip propagation as economic guardrail** — default trigger rule (`all_success`) means the dbt trigger inherits the upstream skip when NREL reports unchanged data. dbt only re-builds when there's actually new source data; running `dbt build` daily on stale data is pure compute waste.

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
- **Analytics / mart layer design** — wide, dashboard-shaped tables that pre-join facts to conformed dims so each chart hits one model. Two grains in this project: per-state snapshot (`mart_state_ev_overview`) and state × year time-series (`mart_ev_growth_trends`).
- **`qualify row_number() over (...) = 1` for "latest per group"** — picks the most-recent row per partition without a self-join on a max() subquery. Per-state robust to gaps: each state independently gets its own latest available year.
- **Mart-of-marts pattern** — `mart_stations_by_region` aggregates `mart_state_ev_overview` rather than re-querying the curated facts. Single source of truth for per-state inputs; impossible for the two marts to disagree on a measure due to filter drift.
- **String-key normalization with `initcap(trim(...))`** — collapses inconsistent third-party text keys (city names with case + whitespace variants) into a single canonical form, without losing display-friendly capitalization.
- **Pre-computed ranks via `row_number() over (order by metric desc)`** — `national_rank` and `state_rank` baked into the mart so the dashboard query is `where rank <= N` instead of a runtime sort+limit. Tie-break alphabetically by name to keep ranks stable across builds.
- **Per-capita and infrastructure-density ratios** — `stations_per_100k_pop`, `evs_per_1k_pop`, `dcfast_penetration_pct`, `evs_per_open_station`, `avg_l2_per_open_station`. Pattern: `nullif`-protected division, multiplied by a unit-friendly scale (100k or 1k) so the resulting numbers fit chart axes without further transformation.
- **Snowflake reserved-word awareness** — `rows` cannot be used as a column alias; `count(*) as row_count` instead.

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
- **Why two marts instead of three (collapsed station_density into overview)?** A separate "station density" mart would have re-joined the same upstream tables to produce ratios (stations-per-100k, evs-per-station) already available from `fct_stations_by_state` + Census + AFDC. Materializing those ratios as columns inside `mart_state_ev_overview` keeps one source of truth per state and avoids the risk that the two marts disagree on, say, the `total_ev_count` baseline. DRY at the mart layer is the same principle as at the model layer.
- **Why hold population constant at the latest ACS year inside `mart_ev_growth_trends`?** The dashboard is a current-state view, not a longitudinal demographics study. Joining ACS by year would require either (a) a population-by-year table we don't have (single ACS vintage in `RAW_EV.CENSUS_POPULATION`) or (b) a fallback chain when a year has no match — both add complexity for a metric that barely changes year-over-year. Documenting the constraint in the model description is more honest than fabricating "varying" population.
- **Why `qualify row_number() = 1` instead of a max-year subquery?** Both work; `qualify` is one CTE and one expression. The subquery alternative requires a self-join on `(state_fips, max_year)` plus another on the original table — three table references to do what `qualify` does in one. Snowflake-specific syntax; cost is portability if we ever migrate.
- **Why surface five reference-repo chart angles when our 10-chart plan already covered the basics?** The reference (`singhpriyanshu5/us-ev-charging-stations-dashboard`) had been refined past our first pass — five concrete angles (DC fast vs L2 stack, DC fast penetration %, L2-per-station, top cities, regional breakdown) added qualitatively distinct insights without much marginal SQL. Cheap to incorporate; expensive to discover later mid-build.
- **Why a `Territory` bucket on `census_region` instead of grouping Puerto Rico into the South or excluding it?** Census doesn't classify PR into a region (it's a territory, not a state). Forcing it into `South` would falsely inflate that region's totals; excluding it would silently drop a whole jurisdiction from rollup math. A separate bucket is honest, the `accepted_values` test enumerates it explicitly, and the dashboard can choose to filter it out for "mainland US" comparisons without losing the row.
- **Why expose `stations_with_dcfast` (count) when `total_dcfast_ports` (sum) already exists?** They answer different questions. Penetration ("what share of stations offer fast charging?") needs station-level boolean rollups; capacity ("how much fast-charging exists?") needs port sums. Computing one from the other inside the mart would require going back to the staging level. Adding the column to `fct_stations_by_state` once pays off in two places (penetration in the overview mart, regional rollup in `mart_stations_by_region`).
- **Why a mart-of-marts (`mart_stations_by_region` aggregates `mart_state_ev_overview`)?** The state-level mart already encodes filter decisions ("latest AFDC year per state," "latest ACS year per state," "regional bucket from `dim_states`"). A region-level mart that re-queried the curated facts could subtly differ on those choices and produce a different `total_ev_count` than what shows up in the state-level dashboard. Layering keeps the inputs identical by construction.
- **Why `initcap(trim(city))` for city normalization?** NREL ships inconsistent strings (`"san francisco"`, `"San Francisco "`) — without normalization, two rows for the same city. `lower()` would lose display readability ("san francisco" on a chart looks broken); `trim()` alone wouldn't fix case variance. `initcap(trim())` collapses both axes and produces a chart-ready label. Empty/blank cities are dropped at this layer (the contract is "named cities only"); state-level aggregates already exist for "everything else."
- **Why pre-compute `national_rank` + `state_rank` instead of letting the dashboard sort+limit?** Two reasons. (1) Stable ranks across deploys — tie-break by city name baked into the SQL means `national_rank=1` doesn't flip between two same-count cities just because a viz library reordered them. (2) Read-time simplicity — `where national_rank <= 20` is one predicate the dashboard never has to think about; an `order by ... limit 20` re-sorts 9k rows on every chart render. Materializing the rank trades a few KB of storage for repeatable, fast reads.
- **Why a standalone `ev_dbt_pipeline` DAG instead of inlining `dbt build` tasks at the end of each ingest DAG?** Three ingest DAGs (NREL daily, Census yearly, AFDC seed) all need to converge on the same dbt build. Inlining means three copies of the dbt task block to maintain. A separate DAG with `schedule=None` is the single point of orchestration; any upstream DAG triggers it via `TriggerDagRunOperator`. Adds one DAG file, removes future drift.
- **Why `schedule=None` on the dbt DAG instead of a daily cron?** The marts only need to refresh when source data changes. A cron schedule would either fire too early (before NREL completes) or too late (waiting for the next tick after a successful ingest). Triggering downstream from the ingest gives "new data → marts refresh" without a calendar dependency. Census and AFDC change annually/manually; their cadence is too rare to dictate a daily cron, but the trigger pattern still applies if/when they fire.
- **Why default skip propagation rather than `trigger_rule="none_failed"` on the dbt trigger?** Dbt rebuilding identical marts from unchanged source data is waste. The NREL DAG's `check_last_updated` task already short-circuits the day's run when the source hasn't changed; that skip should cascade to the dbt trigger by default. If a future scenario calls for "rebuild marts even when NREL didn't change" (e.g., a model change without a data change), that's better handled by a manual trigger or a separate cron, not by changing the chain's normal economics.
- **Why template Snowflake creds from `conn.snowflake_default` rather than rely on `.env`?** The `.env` file works inside the container (loaded via `env_file:` in compose), but the `.env` and the Airflow Connection are two parallel sources of truth that can drift. The ingest DAGs already use the connection (`SnowflakeHook(snowflake_conn_id="snowflake_default")`); the dbt DAG should too. Templating closes the loop: change the connection in the UI, every DAG (ingest *and* dbt) picks it up. `.env` becomes a developer-convenience file for local CLI work, not a runtime dependency.
- **Why `append_env=True` on the dbt `BashOperator`s?** `BashOperator.env` *replaces* the inherited environment by default — setting it without `append_env=True` would strip the container's `PATH`, breaking plain `dbt` invocations. Setting `append_env=True` merges the templated Snowflake creds onto the container env. The cross-project reference DAG sidesteps the issue by hardcoding `/opt/dbt_venv/bin/dbt` (absolute path); we keep the more portable plain-`dbt` form by appending instead.

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
| 8 | dbt analytics layer — 4 marts (state overview, growth trends, regional rollup, top cities) | ✅ Done |
| 9 | Airflow → dbt orchestration (`ev_dbt_pipeline` DAG + NREL trigger) | ✅ Done |
| 10 | Dashboard build (Streamlit / Plotly / Preset.io) on the analytics marts | Not started |
| 11 | 12-month time-series forecast (EV adoption + infrastructure expansion) | Not started |

---

*Last updated: 2026-05-05.*
