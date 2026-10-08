# EV dashboard publication and refresh

## Current verification status

The owner-authorized current Snowflake connection is verified, with separate EV schemas in the existing `WEATHER_FORECASTING` database. Real station and Census source loads, dbt seed, all 11 models, and all 145 warehouse tests succeeded on October 8, 2026. The first immutable private export was stored and its exact bytes verified against its SHA-256. It includes 52 jurisdictions, five regions, 1,664 growth rows, and 1,017 city rows. The checked-in snapshot preserves that real export and its original source/build dates. Pages publication and automatic dispatch verification are still pending; synthetic browser fixtures are never a production fallback.

The station provider retired `developer.nrel.gov` on May 29, 2026. Ingestion uses the documented `developer.nlr.gov` replacement. Existing keys remain valid according to the [official transition notice](https://developer.nlr.gov/docs/nlr-domain-transition/). The owner authorized the saved EV key at this hostname. Bounded timestamp and one-station checks returned HTTP 200 on October 8, 2026, with the expected ingestion fields; full ingestion has not yet run. The Census 2024 API connection was also verified successfully.

## Data path

Changed public station source → Airflow ingestion → dbt seed/run/test → completion-receipt and warehouse-fingerprint validation → bounded public JSON → private immutable S3 bundle → conditional latest-success pointer → GitHub Actions → Pages artifact → public checksum verification.

Both source loads and all three dbt tasks must have actual successful execution receipts. Manually marking a failed task successful cannot make a bundle eligible. Daily station and annual population captures are selected independently. Competing runs or changed warehouse tables reject the export. dbt-run, test and export fingerprints must agree. External/manual writes during a dbt build are not coordinated by Airflow; avoid concurrent manual writes to these EV schemas. A fingerprint detects subsequent changes; it is not a database-wide lock.

The default station schedule remains **02:30 UTC daily** (7:30 PM the previous day in Los Angeles during daylight saving time, 6:30 PM during standard time). Unchanged station data skips ingestion and its downstream build. Census ingestion keeps its annual `@yearly` schedule and triggers a model rebuild after a successful load. AFDC registrations are a manually maintained annual seed: an automated seed run does not download a new registration year. Airflow and Docker must remain running on this computer for schedules to execute.

## Runtime configuration

Use the EV checkout's ignored `.env`; do not reuse another project's credentials without explicit authorization. Supply a working EV Snowflake connection as `snowflake_default` and the existing EV source keys. The companion listens at port **8083** by default (`EV_AIRFLOW_PORT`), alongside weather at 8081 and movies at 8080.

| Setting | Storage | Purpose / consumer |
| --- | --- | --- |
| `SNOWFLAKE_*` | Local ignored `.env` and Airflow `snowflake_default` connection | Existing EV database, role, warehouse and login. dbt/export read the Airflow connection. |
| `NREL_API_KEY`, `CENSUS_API_KEY` | Local ignored `.env` | EV source ingestion. Never exported to Pages. |
| `DASHBOARD_S3_BUCKET` | Local ignored `.env`; GitHub repository Actions **secret** | Private export bucket for Airflow and Pages assembly. |
| `DASHBOARD_S3_PREFIX` | Same locations | Dedicated `dashboard/ev` prefix. |
| `AWS_CREDENTIALS_DIR` | Local ignored `.env` | Existing AWS profile directory, mounted read-only as in weather and movie. The owner explicitly approved this access for EV. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_SESSION_TOKEN` | Local ignored `.env` or trusted short-lived writer environment | Optional alternative to the mounted profile. A new upload user/key is unnecessary when the existing profile can write the EV prefix. |
| `AWS_DEFAULT_REGION` | Local ignored `.env` | EV writer region. |
| `DASHBOARD_AIRFLOW_API_URL` | Local ignored `.env` | `http://localhost:8080` inside the EV container, or HTTPS for a remote host. |
| `DASHBOARD_AIRFLOW_USERNAME`, `DASHBOARD_AIRFLOW_PASSWORD` | Local ignored `.env` | Existing reader able to read DAG runs, task instances and XCom completion receipts through Airflow 2.10's `/api/v1`. |
| `DASHBOARD_GITHUB_TOKEN` | Local ignored `.env` | Fine-grained, expiring token limited to `ishuapurva1996/EV-dashboard`, Actions read/write; dispatches `deploy-dashboard.yml` on main. |
| `DASHBOARD_AWS_ROLE_ARN` | GitHub repository Actions **secret** | Separate read-only OIDC role for this EV repository's `github-pages` environment. |
| `DASHBOARD_AWS_REGION` | GitHub repository Actions **variable** | Region for Actions OIDC/S3 access. Must match the bucket. |
| `DASHBOARD_PUBLICATION_MODE` | GitHub repository Actions **variable** | `airflow` activates the private handoff route and disables snapshot publishing; `snapshot` activates only reviewed real snapshot publishing. |

Settings links: [Actions secrets](https://github.com/ishuapurva1996/EV-dashboard/settings/secrets/actions), [Actions variables](https://github.com/ishuapurva1996/EV-dashboard/settings/variables/actions), [Pages source](https://github.com/ishuapurva1996/EV-dashboard/settings/pages), [deployment environments](https://github.com/ishuapurva1996/EV-dashboard/settings/environments).

Keep credentials private. Token creation, credential expansion or IAM changes require owner approval. Private runtime credentials remain in the ignored local `.env` and authorized AWS profile. The owner created the separate `EVDashboardPagesReader` role; actual GitHub OIDC authentication still needs deployment verification. Recreate the EV Airflow container after `.env` changes so it receives the new values. Preserve the existing metadata volume.

## AWS scope

Keep Block Public Access enabled. The writer needs scoped `s3:GetObject` and `s3:PutObject` for `dashboard/ev/latest-success.json` and `dashboard/ev/bundles/*`; give the writer `s3:ListBucket` on the bucket with `StringEquals` condition `s3:prefix = dashboard/ev/latest-success.json`. A first missing pointer can return 403 without unrestricted listing rights. The exporter then checks only this exact prefix with `MaxKeys=1`; only a confirmed empty, non-truncated listing permits initialization. An unreadable existing pointer or failed/uncertain listing stops publication. S3 writes use `IfNoneMatch` for immutable objects and `IfMatch` or `IfNoneMatch` for the success pointer. An older build/export cannot replace a newer pointer. Verify the stored immutable bytes before advancing the pointer. Never expire the currently referenced bundle.

The GitHub reader needs only `s3:GetObject` on those same keys. Restrict AWS OIDC audience to `sts.amazonaws.com` and subject to this exact repository/environment. Verify the repository's actual OIDC subject configuration before applying trust: newer repositories may include immutable owner/repository IDs. Restrict `github-pages` deployments to main and retain any existing protection rules. Use a distinct reader role; do not expand the movie role or share its dispatch token.

## First real publication

1. Resolve the working EV Snowflake account and source endpoint authorization, then create the EV source tables/schemas as appropriate for that account. Do not rerun the old hard-coded training-account `snowflake/setup.sql` in another account without adapting it.
2. Configure the authorized existing AWS profile, private EV S3 prefix and Airflow metadata reader. Build the EV image, initialize its own Airflow/Postgres stack, and register `snowflake_default`. Existing services must not be stopped or replaced.
3. Run Census ingestion once, then a fresh complete station-ingestion run. Its chained dbt pipeline must finish the real seed/run/test and validated private export. Initial Census-triggered publication may fail until the first station source exists; then the subsequent complete station chain is the eligible build.
4. Inspect the immutable real export and contract validation. A snapshot-only first publication is available by placing that real export and SHA-256 file together in `web_dashboard/snapshot/`; original capture/build dates remain unchanged.
5. Enable GitHub Pages **GitHub Actions** source, configure the EV OIDC reader and repository settings, restrict the environment to main, and merge the reviewed implementation.
6. Switch `DASHBOARD_PUBLICATION_MODE` to `airflow` only after the first real private bundle and read access are verified. Complete a fresh eligible Airflow run to test its automatic dispatch. An accepted dispatch is only a queued request.
7. Confirm the [deployment workflow](https://github.com/ishuapurva1996/EV-dashboard/actions/workflows/deploy-dashboard.yml) actually deployed, use its returned Pages URL, compare the public JSON checksum to the selected export, and check charts/controls on desktop/mobile. Add the verified live link and a real README preview afterward.

## Local preview and validation

Install `requirements-dashboard.txt` plus `requests` in an isolated environment. Run `python -m unittest discover -s tests -v`. Tests cover public fields, non-finite values, coverage, totals, ratios, row bounds, checksums, allowlisted packaging, receipts, independent source cadences, competing runs, failed uploads and conditional pointer conflicts.

For a **real** local bundle: `python scripts/build_dashboard_site.py --bundle /absolute/path/dashboard.json --output /absolute/path/public-site`, then serve that directory over HTTP. For a real checked-in snapshot: `python scripts/build_dashboard_site.py --snapshot --state /absolute/path/selection.json --output /absolute/path/public-site`.

Synthetic fixtures are only for local tests; `--allow-synthetic` requires local `--bundle` and is rejected inside Actions. Pull-request validation has no warehouse/AWS/export credentials and never deploys. Assets and JSON use relative repository-base paths; the USA map data and Plotly are vendored.

## Failure recovery

- Source, seed, model or test failure: fix the first failed task, then execute a fresh complete build. Empty source responses do not delete existing RAW data.
- Export evidence/fingerprint failure: do not mark tasks successful or clear only the export to bypass validation. Run a new coherent build.
- S3 upload/pointer failure: check the exact bucket, prefix, object permissions and encryption. A failed export never advances the success pointer.
- Successful export but failed dispatch: fix token scope/expiry, reload runtime configuration, then retry only dispatch while its verified private handoff remains valid.
- Skipped GitHub job: check main/repository and `DASHBOARD_PUBLICATION_MODE`. An alternative mode deliberately disables that publisher.
- Failed GitHub step: inspect the first failed step's logs. A failed assembly preserves the old public site. A failure after deployment does not automatically roll back.
- OIDC succeeds but S3 read fails: distinguish an absent first pointer from a region/prefix/policy problem; do not grant S3 administrator access.
- Pages checksum differs: inspect the actual deployed URL and selected bundle. Do not call the dashboard refreshed until the public bytes match.

All site publishers share the `ev-dashboard-pages` concurrency group. Assembly resolves current main after acquiring the publication lock, selects the latest validated pointer, and rechecks code and data before deployment. A bounded reassembly handles changed inputs; repeated changes fail rather than publishing stale code/data.
