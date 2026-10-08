"""Success-only EV pipeline evidence and immutable private export handoff."""
import ast
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from urllib.parse import quote, urlsplit

import requests

STATION_DAG = 'nrel_stations_data_ingest'
CENSUS_DAG = 'census_population_data_ingest'
DBT_DAG = 'ev_dbt_pipeline'
PREDECESSORS = {STATION_DAG: ('check_last_updated', 'extract_transform_load'),
                CENSUS_DAG: ('extract_census_population', 'transform_census_population_data', 'load_population_data_into_snowflake'),
                DBT_DAG: ('dbt_seed', 'dbt_run', 'dbt_test')}
MUTATORS = {STATION_DAG: {'extract_transform_load'}, CENSUS_DAG: {'load_population_data_into_snowflake'},
            DBT_DAG: {'dbt_seed','dbt_run'}}
ACTIVE = {'running', 'queued', 'scheduled', 'up_for_retry', 'restarting', 'deferred'}


def required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f'Missing private configuration: {name}.')
    return value


def source_request(url, **kwargs):
    try:
        return requests.get(url, **kwargs)
    except requests.RequestException:
        # Requests exceptions can include the full URL with the source API key.
        raise RuntimeError('EV source request failed; check connectivity and the configured endpoint.') from None


def now():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise RuntimeError('Missing timezone in pipeline evidence.')
    return result


def success_receipt(started_at, **extra):
    from airflow.operators.python import get_current_context
    ti = get_current_context()['ti']
    return dict(dag_id=ti.dag_id, run_id=ti.run_id, task_id=ti.task_id,
                try_number=ti.try_number, started_at=started_at, completed_at=now(), **extra)


def run_dbt(command):
    """Emit a receipt only when the real dbt subprocess has exited successfully."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    connection = SnowflakeHook(snowflake_conn_id='snowflake_default').get_connection('snowflake_default')
    extra = connection.extra_dejson
    env = os.environ.copy()
    dbt_env = dict(SNOWFLAKE_USER=connection.login, SNOWFLAKE_PASSWORD=connection.password,
                   SNOWFLAKE_ACCOUNT=extra['account'], SNOWFLAKE_SCHEMA='ANALYTICS_EV',
                   SNOWFLAKE_ROLE=extra['role'], SNOWFLAKE_DATABASE=extra['database'],
                   SNOWFLAKE_WAREHOUSE=extra['warehouse'])
    missing = [name for name, value in dbt_env.items() if not isinstance(value, str) or not value]
    if missing:
        raise RuntimeError('EV Snowflake connection needs explicit dbt settings: ' + ', '.join(missing))
    env.update(dbt_env)
    if '/opt/airflow/scripts' not in sys.path:
        sys.path.insert(0, '/opt/airflow/scripts')
    started = now()
    if command == 'seed':
        subprocess.run(['dbt', 'deps', '--project-dir', '/opt/airflow/dbt', '--profiles-dir', '/opt/airflow/dbt'], env=env, check=True, timeout=300)
    subprocess.run(['dbt', command, '--project-dir', '/opt/airflow/dbt',
                    '--profiles-dir', '/opt/airflow/dbt'], env=env, check=True, timeout=600)
    if command in ('run','test'):
        from export_dashboard import warehouse_fingerprint
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        conn = hook.get_conn()
        try:
            fingerprint = warehouse_fingerprint(conn, extra['database'])
        finally:
            conn.close()
        if command == 'test':
            from airflow.operators.python import get_current_context
            previous = get_current_context()['ti'].xcom_pull(task_ids='dbt_run')
            if not previous or previous.get('warehouse_fingerprint') != fingerprint:
                raise RuntimeError('EV tables changed after the selected dbt run; execute a fresh build.')
        return success_receipt(started,warehouse_fingerprint=fingerprint)
    return success_receipt(started)


class MetadataAPI:
    def __init__(self, base=None, auth=None):
        base = base or required('DASHBOARD_AIRFLOW_API_URL')
        parsed = urlsplit(base)
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise RuntimeError('Airflow reader URL cannot contain credentials or query parameters.')
        if parsed.scheme != 'https' and not (parsed.scheme == 'http' and parsed.hostname in {'localhost','127.0.0.1','airflow'}):
            raise RuntimeError('A remote Airflow reader must use HTTPS.')
        self.base = base.rstrip('/') + '/api/v1'
        self.auth = auth or (required('DASHBOARD_AIRFLOW_USERNAME'), required('DASHBOARD_AIRFLOW_PASSWORD'))

    def get(self, path, params=None):
        response = requests.get(self.base + path, auth=self.auth, params=params, timeout=(10, 30))
        if response.status_code != 200:
            raise RuntimeError(f'Cannot verify EV pipeline metadata (HTTP {response.status_code}).')
        return response.json()

    def tasks(self, dag, run='~'):
        path = f'/dags/{dag}/dagRuns/{quote(run, safe="")}/taskInstances'
        rows, total = [], None
        for offset in range(0, 10000, 100):
            page = self.get(path, {'limit': 100, 'offset': offset})
            batch, count = page['task_instances'], page['total_entries']
            if not isinstance(batch,list) or not isinstance(count,int) or count < 0 or (total is not None and total != count):
                raise RuntimeError('EV task metadata changed during its bounded scan.')
            total = count
            rows.extend(batch)
            if len(rows) == total:
                ids = [(r['dag_id'],r['dag_run_id'],r['task_id'],r.get('map_index',-1)) for r in rows]
                if len(ids) != len(set(ids)):
                    raise RuntimeError('Duplicate EV metadata task identities.')
                return rows
            if not batch or len(rows) > total:
                raise RuntimeError('EV task metadata pagination is incomplete.')
        raise RuntimeError('EV metadata exceeds the scan limit; retain publication evidence and review cleanup.')

    def run(self, dag, run):
        return self.get(f'/dags/{dag}/dagRuns/{quote(run, safe="")}')

    def receipt(self, dag, run, task):
        result = self.get(f'/dags/{dag}/dagRuns/{quote(run, safe="")}/taskInstances/{task}/xcomEntries/return_value')['value']
        if isinstance(result, str):
            if len(result) > 8192:
                raise RuntimeError('Success receipt exceeds its evidence size limit.')
            try:
                result = json.loads(result)
            except json.JSONDecodeError:
                # Airflow 2.10's API String field emits Python dictionary repr.
                # Parse literals only; this never evaluates executable code.
                result = ast.literal_eval(result)
        if not isinstance(result, dict):
            raise RuntimeError('Success receipt must contain task-attempt evidence.')
        return result


def check_receipt(receipt, task):
    for field in ('dag_id', 'task_id', 'try_number'):
        if receipt.get(field) != task[field]:
            raise RuntimeError('Success receipt does not match this task attempt; execute a fresh full pipeline.')
    if receipt.get('run_id') != task['dag_run_id']:
        raise RuntimeError('Success receipt belongs to another pipeline run.')
    if not timestamp(task['start_date']) <= timestamp(receipt['started_at']) <= timestamp(receipt['completed_at']) <= timestamp(task['end_date']):
        raise RuntimeError('Success receipt is outside the real successful task interval.')


def latest_success(api, dag, task, cutoff):
    rows = [r for r in api.tasks(dag) if r['task_id'] == task and r['state'] == 'success'
            and r.get('end_date') and timestamp(r['end_date']) <= cutoff]
    if not rows:
        raise RuntimeError('Complete both EV source ingestion DAGs before publishing.')
    return max(rows, key=lambda r: timestamp(r['end_date']))['dag_run_id']


def check_eligibility(api, dbt_run, expected=None):
    dbt_rows = {r['task_id']:r for r in api.tasks(DBT_DAG, dbt_run)}
    seed = dbt_rows.get('dbt_seed')
    if not seed or not seed.get('start_date'):
        raise RuntimeError('No completed EV seed/build is available.')
    cutoff = timestamp(seed['start_date'])
    conf = api.run(DBT_DAG, dbt_run).get('conf') or {}
    if set(conf) - {'nrel_run_id', 'census_run_id'}:
        raise RuntimeError('Unexpected EV pipeline lineage fields.')
    runs = {DBT_DAG: dbt_run,
            STATION_DAG: conf.get('nrel_run_id') or latest_success(api, STATION_DAG, 'extract_transform_load', cutoff),
            CENSUS_DAG: conf.get('census_run_id') or latest_success(api, CENSUS_DAG, 'load_population_data_into_snowflake', cutoff)}
    receipts, evidence = {}, []
    for dag, run in runs.items():
        task_rows = api.tasks(dag, run)
        rows = {r['task_id']:r for r in task_rows}
        if len(rows) != len(task_rows):
            raise RuntimeError('Unexpected mapped or duplicate EV tasks.')
        for name in PREDECESSORS[dag]:
            task = rows.get(name)
            if not task or task['state'] != 'success' or not task.get('start_date') or not task.get('end_date'):
                raise RuntimeError('A required EV predecessor has not succeeded.')
            if task['dag_id'] != dag or task['dag_run_id'] != run or task.get('map_index',-1) != -1:
                raise RuntimeError('EV predecessor identity changed.')
            if timestamp(task['start_date']) > timestamp(task['end_date']):
                raise RuntimeError('EV predecessor interval is invalid.')
            evidence.append({k:task[k] for k in ('dag_id','dag_run_id','task_id','state','try_number','start_date','end_date')})
            if name in MUTATORS[dag] or (dag == DBT_DAG and name == 'dbt_test'):
                receipt = api.receipt(dag, run, name)
                check_receipt(receipt, task)
                receipts[(dag,name)] = receipt
        for parent, child in zip(PREDECESSORS[dag], PREDECESSORS[dag][1:]):
            if timestamp(rows[parent]['end_date']) > timestamp(rows[child]['start_date']):
                raise RuntimeError('EV producer sequence is inconsistent.')
        if dag != DBT_DAG and timestamp(rows[PREDECESSORS[dag][-1]]['end_date']) > cutoff:
            raise RuntimeError('An EV source load completed after the selected build started.')
    # Annual population and daily station snapshots have independent capture times.
    # Check competing writes against each producer's selected attempt, not the
    # oldest annual source (which would incorrectly reject every later daily run).
    boundaries = {dag:min(timestamp(receipts[(dag,name)]['started_at']) for name in MUTATORS[dag]) for dag in runs}
    for dag, run in runs.items():
        for task in api.tasks(dag):
            if task['task_id'] not in MUTATORS[dag] or task['dag_run_id'] == run:
                continue
            if task['state'] in ACTIVE or (task.get('start_date') and not task.get('end_date')) or (task.get('end_date') and timestamp(task['end_date']) >= boundaries[dag]):
                raise RuntimeError('Another EV run may have modified these tables; execute a fresh complete build.')
    nrel = receipts[(STATION_DAG,'extract_transform_load')]
    census = receipts[(CENSUS_DAG,'load_population_data_into_snowflake')]
    result = dict(fingerprint=hashlib.sha256(json.dumps(evidence,sort_keys=True).encode()).hexdigest(),
                  stations_captured_at=nrel['source_captured_at'], stations_source_updated_at=nrel['source_last_updated_at'],
                  population_captured_at=census['source_captured_at'],
                  warehouse_completed_at=receipts[(DBT_DAG,'dbt_test')]['completed_at'],
                  warehouse_fingerprint=receipts[(DBT_DAG,'dbt_test')]['warehouse_fingerprint'])
    if expected is not None and result != expected:
        raise RuntimeError('EV task attempts changed during export; no pointer was published.')
    return result


def prefix(value):
    if not re.fullmatch(r'dashboard/[A-Za-z0-9/_-]+', value) or '..' in value or value.endswith('/'):
        raise RuntimeError('Use a dedicated private dashboard/<name> prefix.')
    return value


def read_object(s3, bucket, key, limit):
    result = s3.get_object(Bucket=bucket, Key=key)
    stream = result['Body']
    try:
        body = stream.read(limit + 1)
    finally:
        stream.close()
    if len(body) > limit:
        raise RuntimeError('Private export object exceeds its size limit.')
    return body, result.get('ETag')


def publish_bundle(s3, bucket, path, body, metadata, run_id, guard):
    path = prefix(path)
    digest = hashlib.sha256(body).hexdigest()
    key = f'{path}/bundles/{digest}.json'
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json', IfNoneMatch='*')
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in {'PreconditionFailed', '412'}:
            raise
    if read_object(s3, bucket, key, 2 * 1024 * 1024)[0] != body:
        raise RuntimeError('Stored immutable bundle failed byte verification.')
    pointer_key = path + '/latest-success.json'
    try:
        old_body, etag = read_object(s3, bucket, pointer_key, 8192)
        old = json.loads(old_body)
        if (timestamp(old['warehouse_completed_at']), timestamp(old['exported_at'])) > (timestamp(metadata['warehouse_completed_at']), timestamp(metadata['exported_at'])):
            raise RuntimeError('A newer successful EV export already exists.')
        if not etag:
            raise RuntimeError('No conditional pointer identity available.')
        condition = {'IfMatch': etag}
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in {'NoSuchKey', '404'}:
            raise
        condition = {'IfNoneMatch': '*'}
    pointer = dict(schema_version=1, bundle_id=metadata['bundle_id'], sha256=digest, bundle_key=key, run_id=run_id,
                   warehouse_completed_at=metadata['warehouse_completed_at'], exported_at=metadata['exported_at'])
    guard()
    pointer_bytes = json.dumps(pointer, sort_keys=True).encode()
    try:
        s3.put_object(Bucket=bucket, Key=pointer_key, Body=pointer_bytes, ContentType='application/json', **condition)
    except Exception:
        if read_object(s3, bucket, pointer_key, 8192)[0] != pointer_bytes:
            raise RuntimeError('Pointer update was not confirmed; inspect its identity before retrying.') from None
    return {'sha256': digest}


def export_and_publish(**context):
    import boto3
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    sys.path.insert(0, '/opt/airflow/scripts')
    from export_dashboard import extract_bundle, serialize, warehouse_fingerprint
    api = MetadataAPI()
    run = context['run_id']
    evidence = check_eligibility(api, run)
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    database = hook.get_connection('snowflake_default').extra_dejson['database']
    conn = hook.get_conn()
    try:
        conn.cursor().execute('ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 120')
        fingerprint = warehouse_fingerprint(conn, database)
        if fingerprint != evidence['warehouse_fingerprint']:
            raise RuntimeError('EV tables changed since the tested build; execute a fresh complete build.')
        def guard():
            check_eligibility(api, run, expected=evidence)
            if warehouse_fingerprint(conn, database) != fingerprint:
                raise RuntimeError('EV warehouse tables changed during export; run a fresh complete build.')
        bundle = extract_bundle(conn, evidence, database=database)
        body = serialize(bundle)
        guard()
        return publish_bundle(boto3.client('s3'), required('DASHBOARD_S3_BUCKET'), required('DASHBOARD_S3_PREFIX'),
                              body, bundle['metadata'], run, guard)
    finally:
        conn.close()


def dispatch_dashboard(**context):
    publication = context['ti'].xcom_pull(task_ids='export_dashboard_bundle')
    if not publication or not re.fullmatch(r'[0-9a-f]{64}', publication.get('sha256', '')):
        raise RuntimeError('A verified private export is required before dispatch.')
    response = requests.post('https://api.github.com/repos/ishuapurva1996/EV-dashboard/actions/workflows/deploy-dashboard.yml/dispatches',
        headers={'Authorization': 'Bearer ' + required('DASHBOARD_GITHUB_TOKEN'),
                 'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28'},
        json={'ref': 'main'}, timeout=(10, 30))
    if response.status_code != 204:
        raise RuntimeError(f'EV Pages dispatch rejected (HTTP {response.status_code}); check token scope and expiry.')
    return {'sha256': publication['sha256'], 'submitted': True}
