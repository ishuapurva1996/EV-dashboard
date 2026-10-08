"""Export only the bounded, public EV analytics contract; no raw station rows."""
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
import hashlib
import json
import math
import os
import re
import tempfile
import uuid

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
MAX_BUNDLE_BYTES = 2 * 1024 * 1024
SCHEMA = ROOT / 'web_dashboard/data-contract.schema.json'
SOURCES = [
    {'name': 'NLR / AFDC public charging stations', 'url': 'https://developer.nlr.gov/docs/transportation/alt-fuel-stations-v1/'},
    {'name': 'DOE / AFDC registrations (project seed)', 'url': 'https://afdc.energy.gov/vehicle-registration'},
    {'name': 'Census ACS 5-year population estimates', 'url': 'https://api.census.gov/data/2024/acs/acs5'},
]
FIELDS = {
    'states': ('state_fips state_abbr state_name census_region total_stations stations_open stations_planned stations_with_dcfast '
               'total_level1_ports total_level2_ports total_dcfast_ports total_ports ev_data_year bev_count phev_count total_ev_count '
               'population_year population stations_per_100k_pop dcfast_ports_per_100k_pop evs_per_1k_pop evs_per_open_station '
               'ports_per_ev dcfast_penetration_pct avg_l2_per_open_station').split(),
    'regions': ('census_region state_count total_stations stations_open stations_with_dcfast total_level1_ports total_level2_ports '
                'total_dcfast_ports total_ports total_ev_count bev_count phev_count population stations_per_100k_pop '
                'evs_per_1k_pop evs_per_open_station dcfast_penetration_pct').split(),
    'growth': ('state_fips state_abbr state_name year new_stations new_total_ports cumulative_stations cumulative_total_ports '
               'bev_count phev_count total_ev_count ev_yoy_growth_pct population_latest population_year stations_yoy_growth_pct').split(),
    'cities': ('state_fips state_abbr state_name city total_stations stations_open stations_with_dcfast total_level2_ports '
               'total_dcfast_ports total_ports national_rank state_rank').split(),
}
RELATIONS = dict(states='MART_STATE_EV_OVERVIEW', regions='MART_STATIONS_BY_REGION',
                 growth='MART_EV_GROWTH_TRENDS', cities='MART_TOP_CITIES')
LIMITS = dict(states=60, regions=5, growth=6000, cities=1200)


class ExportError(RuntimeError):
    pass


def timestamp(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ExportError('A publication timestamp requires an explicit timezone.')
    return value.astimezone(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def normalize(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ExportError('Non-finite value in public export.')
        return int(value) if value.is_integer() else round(value, 6)
    if isinstance(value, datetime):
        return timestamp(value)
    if isinstance(value, date):
        return value.isoformat()
    raise ExportError('Unsupported public value type.')


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,254}', value):
        raise ExportError('Invalid database or schema identifier.')
    return '"' + value.upper() + '"'


def serialize(bundle):
    body = (json.dumps(bundle, ensure_ascii=False, allow_nan=False, sort_keys=True,
                       separators=(',', ':')) + '\n').encode()
    if len(body) > MAX_BUNDLE_BYTES:
        raise ExportError('Public export exceeds 2 MiB.')
    return body


def validate_bundle(bundle, allow_synthetic=False):
    serialize(bundle)
    validator = Draft202012Validator(json.loads(SCHEMA.read_text()), format_checker=FormatChecker())
    if next(validator.iter_errors(bundle), None):
        raise ExportError('Public EV export failed its field/type/size contract.')
    m = bundle['metadata']
    if m['synthetic'] and not allow_synthetic:
        raise ExportError('Synthetic data cannot be published.')
    if m['sources'] != SOURCES:
        raise ExportError('Public source attribution changed.')
    completed = timestamp(m['warehouse_completed_at'])
    exported = timestamp(m['exported_at'])
    if completed > exported:
        raise ExportError('Export precedes its completed warehouse build.')
    for field in ('stations_captured_at', 'stations_source_updated_at', 'population_captured_at'):
        if m[field] is not None and timestamp(m[field]) > exported:
            raise ExportError('Source capture cannot follow export.')
    states = bundle['states']
    abbrs = {r['state_abbr'] for r in states}
    fips = {r['state_fips'] for r in states}
    if len(abbrs) != len(states) or len(fips) != len(states):
        raise ExportError('Duplicate public state identity.')
    if states != sorted(states, key=lambda r: r['state_abbr']):
        raise ExportError('State ordering is not deterministic.')
    for dataset in ('states', 'regions'):
        for r in bundle[dataset]:
            if r['total_ports'] != r['total_level1_ports'] + r['total_level2_ports'] + r['total_dcfast_ports']:
                raise ExportError('Charging port totals do not reconcile.')
            if r['stations_open'] > r['total_stations'] or r['stations_with_dcfast'] > r['total_stations']:
                raise ExportError('Station populations do not reconcile.')
            if r['total_ev_count'] is not None and r['bev_count'] is not None and r['phev_count'] is not None:
                if r['total_ev_count'] != r['bev_count'] + r['phev_count']:
                    raise ExportError('Registration totals do not reconcile.')
            ratios = {
                'stations_per_100k_pop': (r['total_stations'], r['population'], 100000),
                'evs_per_1k_pop': (r['total_ev_count'], r['population'], 1000),
                'evs_per_open_station': (r['total_ev_count'], r['stations_open'], 1),
                'dcfast_penetration_pct': (r['stations_with_dcfast'], r['total_stations'], 100),
            }
            if dataset == 'states':
                ratios.update(dcfast_ports_per_100k_pop=(r['total_dcfast_ports'], r['population'], 100000),
                              ports_per_ev=(r['total_ports'], r['total_ev_count'], 1),
                              avg_l2_per_open_station=(r['total_level2_ports'], r['stations_open'], 1))
            for field, (numerator, denominator, scale) in ratios.items():
                expected = numerator * scale / denominator if numerator is not None and denominator else None
                actual = r[field]
                if (actual is None) != (expected is None) or (expected is not None and not math.isclose(actual, expected, rel_tol=1e-5, abs_tol=1e-5)):
                    raise ExportError('Derived public metric does not match its population.')
    regions = bundle['regions']
    if len({r['census_region'] for r in regions}) != len(regions):
        raise ExportError('Duplicate region identity.')
    if regions != sorted(regions, key=lambda r: r['census_region']):
        raise ExportError('Region ordering changed.')
    if {r['census_region'] for r in regions} != {r['census_region'] for r in states}:
        raise ExportError('Region coverage disagrees with states.')
    for region in regions:
        members = [r for r in states if r['census_region'] == region['census_region']]
        if region['state_count'] != len(members):
            raise ExportError('Region state count does not reconcile.')
        for field in ('total_stations', 'stations_open', 'stations_with_dcfast', 'total_level1_ports',
                      'total_level2_ports', 'total_dcfast_ports', 'total_ports', 'population', 'total_ev_count', 'bev_count', 'phev_count'):
            values = [r[field] for r in members if r[field] is not None]
            if region[field] != (sum(values) if values else None):
                raise ExportError('Region totals do not reconcile with the state export.')
    growth = bundle['growth']
    if len({(r['state_abbr'], r['year']) for r in growth}) != len(growth):
        raise ExportError('Duplicate growth identity.')
    if growth != sorted(growth, key=lambda r: (r['state_abbr'], r['year'])):
        raise ExportError('Growth ordering changed.')
    max_year = datetime.fromisoformat(exported.replace('Z', '+00:00')).year
    for r in growth:
        if r['year'] > max_year:
            raise ExportError('Future station opening history is not public data.')
        if r['cumulative_stations'] < r['new_stations'] or r['cumulative_total_ports'] < r['new_total_ports']:
            raise ExportError('Growth counts do not reconcile.')
        if r['total_ev_count'] is not None and r['total_ev_count'] != r['bev_count'] + r['phev_count']:
            raise ExportError('Growth registration counts do not reconcile.')
    # The existing model has a complete 1995-current year spine for every jurisdiction.
    expected_growth = {(r['state_abbr'], year) for r in states for year in range(1995, max_year + 1)}
    if {(r['state_abbr'], r['year']) for r in growth} != expected_growth:
        raise ExportError('Growth export lacks required state/year coverage.')
    state_by_abbr = {r['state_abbr']: r for r in states}
    previous = {}
    for r in growth:
        s = state_by_abbr[r['state_abbr']]
        if (r['state_fips'], r['state_name']) != (s['state_fips'], s['state_name']):
            raise ExportError('Growth state dimensions changed.')
        old = previous.get(r['state_abbr'])
        for new, cumulative in (('new_stations','cumulative_stations'), ('new_total_ports','cumulative_total_ports')):
            expected = r[new] + (old[cumulative] if old else 0)
            if r[cumulative] != expected:
                raise ExportError('Cumulative growth history does not reconcile.')
        previous[r['state_abbr']] = r
    cities = bundle['cities']
    if len({(r['state_abbr'], r['city']) for r in cities}) != len(cities):
        raise ExportError('Duplicate city identity.')
    if cities != sorted(cities, key=lambda r: (r['state_abbr'], r['state_rank'])):
        raise ExportError('City ordering changed.')
    for abbr in abbrs:
        rows = [r for r in cities if r['state_abbr'] == abbr]
        if [r['state_rank'] for r in rows] != list(range(1,len(rows)+1)) or len(rows) > 20:
            raise ExportError('City ranks or coverage changed.')
        if rows != sorted(rows, key=lambda r: (-r['total_stations'], r['city'])):
            raise ExportError('City ranking does not match station counts.')
        if sum(r['total_stations'] for r in rows) > state_by_abbr[abbr]['total_stations']:
            raise ExportError('City population exceeds its state.')
    if any(r['state_abbr'] not in abbrs for r in cities):
        raise ExportError('City export contains an unknown state.')


def read(connection, sql, fields, maximum):
    cursor = connection.cursor()
    try:
        cursor.execute(sql, timeout=120)
        names = [d[0].lower() for d in cursor.description]
        if names != fields:
            raise ExportError('Unexpected warehouse columns.')
        rows = cursor.fetchmany(maximum + 1)
        if len(rows) > maximum:
            raise ExportError('Warehouse export exceeds its row bound.')
        return [dict(zip(names, map(normalize, row))) for row in rows]
    finally:
        cursor.close()


def warehouse_fingerprint(connection, database):
    """Detect external DDL/DML as well as competing scheduler runs."""
    database = identifier(database)
    sql = f'''SELECT table_schema, table_name, last_altered, last_ddl FROM {database}.information_schema.tables
              WHERE table_schema IN ('RAW_EV','CURATED_EV','ANALYTICS_EV')
              ORDER BY table_schema, table_name'''
    rows = read(connection, sql, ['table_schema','table_name','last_altered','last_ddl'], 100)
    return hashlib.sha256(json.dumps(rows,sort_keys=True).encode()).hexdigest()


def extract_bundle(connection, evidence, *, database):
    db = identifier(database)
    datasets = {}
    orders = dict(states='state_abbr', regions='census_region', growth='state_abbr, year', cities='state_abbr, state_rank')
    for name, columns in FIELDS.items():
        where = ' WHERE state_rank <= 20' if name == 'cities' else ''
        sql = f'SELECT {", ".join(columns)} FROM {db}.ANALYTICS_EV.{RELATIONS[name]}{where} ORDER BY {orders[name]} LIMIT {LIMITS[name]+1}'
        datasets[name] = read(connection, sql, columns, LIMITS[name])
    # Ensure the state export includes every configured jurisdiction, including PR.
    expected = read(connection, f'SELECT state_abbr FROM {db}.CURATED_EV.DIM_STATES ORDER BY state_abbr', ['state_abbr'], 60)
    if [r['state_abbr'] for r in datasets['states']] != [r['state_abbr'] for r in expected]:
        raise ExportError('Warehouse state coverage is incomplete.')
    m = dict(bundle_id=uuid.uuid4().hex, synthetic=False, exported_at=timestamp(datetime.now(timezone.utc)), sources=SOURCES,
             **{k:evidence.get(k) for k in ('warehouse_completed_at','stations_captured_at','stations_source_updated_at','population_captured_at')})
    bundle = dict(schema_version=1, metadata=m, **datasets)
    validate_bundle(bundle)
    return bundle


def write_bundle(bundle, output):
    validate_bundle(bundle)
    body = serialize(bundle)
    path = Path(output)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent,delete=False) as f:
            temporary = Path(f.name);f.write(body);f.flush();os.fsync(f.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return hashlib.sha256(body).hexdigest()
