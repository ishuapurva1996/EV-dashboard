"""Assemble a complete Pages artifact from trusted main and private validated data."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

from export_dashboard import MAX_BUNDLE_BYTES, validate_bundle

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ('index.html', 'css/dashboard.css', 'js/dashboard.js',
          'assets/vendor/plotly-4.1.1.min.js', 'assets/vendor/plotly-LICENSE.txt', 'assets/maps/usa_110m.json')
SOURCE_ASSETS = {name: name for name in ASSETS}
REPOSITORY = 'ishuapurva1996/EV-dashboard'


class SiteError(RuntimeError):
    pass


def _json(body):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError()
            result[key] = value
        return result
    def invalid_constant(_):
        raise ValueError()
    return json.loads(body, object_pairs_hook=pairs, parse_constant=invalid_constant)


def decode_bundle(body, checksum, *, allow_synthetic=False):
    if len(body) > MAX_BUNDLE_BYTES:
        raise SiteError('Dashboard bundle exceeds the public size limit.')
    if hashlib.sha256(body).hexdigest() != checksum:
        raise SiteError('Dashboard checksum mismatch; nothing was deployed.')
    try:
        bundle = _json(body)
        validate_bundle(bundle, allow_synthetic=allow_synthetic)
        return bundle
    except Exception:
        raise SiteError('Dashboard bundle failed schema or semantic validation; nothing was deployed.') from None


def build_site(source, output, body, checksum, *, allow_synthetic=False):
    bundle = decode_bundle(body, checksum, allow_synthetic=allow_synthetic)
    source, output = Path(source), Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.dashboard-site-', dir=output.parent) as temporary:
        staging = Path(temporary) / 'complete'
        staging.mkdir()
        for name in ASSETS:
            source_name = SOURCE_ASSETS[name]
            asset = source / source_name
            parts = Path(source_name).parts
            linked = source.is_symlink() or any((source.joinpath(*parts[:n])).is_symlink() for n in range(1, len(parts) + 1))
            if not asset.is_file() or linked:
                raise SiteError('Missing or symlinked public asset; nothing was deployed.')
            destination = staging / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(asset, destination)
        (staging / 'data').mkdir()
        (staging / 'data/dashboard.json').write_bytes(body)
        (staging / '.nojekyll').touch()
        if output.exists():
            shutil.rmtree(output)
        os.replace(staging, output)
    return bundle['metadata']


def _s3_read(s3, bucket, key, maximum):
    response = s3.get_object(Bucket=bucket, Key=key)
    stream = response['Body']
    try:
        body = stream.read(maximum + 1)
    finally:
        stream.close()
    if len(body) > maximum:
        raise SiteError('Private dashboard object is oversized.')
    return body


def read_latest(s3, bucket, prefix):
    if not re.fullmatch(r'dashboard/[A-Za-z0-9/_-]+', prefix) or '..' in prefix or prefix.endswith('/'):
        raise SiteError('Configure a dedicated dashboard/<name> S3 prefix.')
    try:
        body = _s3_read(s3, bucket, prefix + '/latest-success.json', 8192)
        pointer = _json(body)
        keys = {'schema_version', 'bundle_id', 'warehouse_completed_at', 'exported_at', 'bundle_key', 'sha256', 'run_id'}
        if set(pointer) != keys or pointer['schema_version'] != 1:
            raise ValueError()
        if not re.fullmatch(r'[0-9a-f]{64}', pointer['sha256']):
            raise ValueError()
        if pointer['bundle_key'] != f'{prefix}/bundles/{pointer["sha256"]}.json':
            raise ValueError()
        return pointer, hashlib.sha256(body).hexdigest()
    except SiteError:
        raise
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') in {'NoSuchKey', '404'}:
            raise SiteError('No first dashboard bundle exists. Run one complete Airflow DAG after setup; fixtures are never a production fallback.') from None
        raise SiteError('Cannot read a valid private dashboard pointer. Check the dashboard-prefix read role and exporter.') from None


def inputs_match(selection, commit, pointer_sha256):
    return selection['commit'] == commit and selection['pointer_sha256'] == pointer_sha256


def _main_commit():
    try:
        output = subprocess.check_output(['git', 'ls-remote', 'origin', 'refs/heads/main'], cwd=ROOT, stderr=subprocess.DEVNULL, text=True)
        commit, ref = output.strip().split()
        if ref != 'refs/heads/main' or not re.fullmatch(r'[0-9a-f]{40}', commit):
            raise ValueError()
        return commit
    except Exception:
        raise SiteError('Cannot resolve the current trusted main commit.') from None


def _cloud_context():
    if os.environ.get('GITHUB_ACTIONS') != 'true' or os.environ.get('GITHUB_REF') != 'refs/heads/main' or os.environ.get('GITHUB_REPOSITORY') != REPOSITORY:
        raise SiteError('Production assembly is restricted to this repository on main.')
    import boto3
    try:
        return boto3.client('s3'), os.environ['DASHBOARD_S3_BUCKET'], os.environ['DASHBOARD_S3_PREFIX']
    except Exception:
        raise SiteError('Configure the dashboard AWS role, region, bucket, and prefix repository variables.') from None


def _checkout_current_main():
    commit = _main_commit()
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    if head != commit:
        try:
            subprocess.run(['git', 'fetch', '--no-tags', 'origin', commit], cwd=ROOT, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(['git', 'checkout', '--detach', commit], cwd=ROOT, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            raise SiteError('Cannot check out the current trusted main commit.') from None
        # Reload the builder and schema from that commit, not the old process.
        restarts = int(os.environ.get('DASHBOARD_ASSEMBLY_RESTARTS', '0')) + 1
        if restarts > 5:
            raise SiteError('Main keeps changing; rerun deployment after changes settle.')
        os.environ['DASHBOARD_ASSEMBLY_RESTARTS'] = str(restarts)
        os.execv(sys.executable, [sys.executable, str(ROOT / 'scripts/build_dashboard_site.py'), *sys.argv[1:]])
    return commit


def assemble(output, state):
    s3, bucket, prefix = _cloud_context()
    commit = _checkout_current_main()
    for _ in range(5):
        pointer, identity = read_latest(s3, bucket, prefix)
        try:
            body = _s3_read(s3, bucket, pointer['bundle_key'], MAX_BUNDLE_BYTES)
        except Exception:
            raise SiteError('The completed private bundle is unavailable; nothing was deployed.') from None
        bundle = decode_bundle(body, pointer['sha256'])
        for key in ('schema_version', 'bundle_id', 'warehouse_completed_at', 'exported_at'):
            actual = bundle['schema_version'] if key == 'schema_version' else bundle['metadata'][key]
            if pointer[key] != actual:
                raise SiteError('Private pointer and validated bundle disagree; nothing was deployed.')
        metadata = build_site(ROOT / 'web_dashboard', output, body, pointer['sha256'])
        selection = {'commit': commit, 'pointer_sha256': identity, 'bundle_id': metadata['bundle_id'], 'sha256': pointer['sha256']}
        current_commit = _main_commit()
        if current_commit != commit:
            _checkout_current_main()
        _, current_pointer = read_latest(s3, bucket, prefix)
        if inputs_match(selection, current_commit, current_pointer):
            Path(state).write_text(json.dumps(selection))
            summary = os.environ.get('GITHUB_STEP_SUMMARY')
            if summary:
                with open(summary, 'a') as handle:
                    handle.write(f'Assembled main `{commit}` · bundle `{metadata["bundle_id"]}`\n\nWarehouse: {metadata["warehouse_completed_at"]} · export: {metadata["exported_at"]}\n\n')
            return
    raise SiteError('Dashboard inputs keep changing; rerun deployment after changes settle.')


def recheck(state):
    s3, bucket, prefix = _cloud_context()
    selection = json.loads(Path(state).read_text())
    _, pointer_identity = read_latest(s3, bucket, prefix)
    fresh = inputs_match(selection, _main_commit(), pointer_identity)
    with open(os.environ['GITHUB_OUTPUT'], 'a') as handle:
        handle.write(f'current={str(fresh).lower()}\n')
    return fresh


def assemble_snapshot(output, state):
    """Publish the reviewed real export in main, independently of Airflow/S3."""
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        if os.environ.get('GITHUB_REF') != 'refs/heads/main' or os.environ.get('GITHUB_REPOSITORY') != REPOSITORY:
            raise SiteError('Snapshot publication is restricted to this repository on main.')
        commit = _checkout_current_main()
    else:
        commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    folder = ROOT / 'web_dashboard/snapshot'
    if folder.is_symlink() or any((folder / name).is_symlink() for name in ('dashboard.json', 'dashboard.sha256')):
        raise SiteError('Snapshot assets must be regular files.')
    body = (folder / 'dashboard.json').read_bytes()
    checksum = (folder / 'dashboard.sha256').read_text().strip()
    metadata = build_site(ROOT / 'web_dashboard', output, body, checksum)
    Path(state).write_text(json.dumps({'commit': commit, 'sha256': checksum,
                                     'bundle_id': metadata['bundle_id']}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--assemble', action='store_true')
    mode.add_argument('--recheck', action='store_true')
    mode.add_argument('--snapshot', action='store_true', help='Reviewed real export checked into main; original dates are retained')
    mode.add_argument('--bundle', type=Path, help='Local validation/preview only')
    parser.add_argument('--output', type=Path, default=ROOT / '_site')
    parser.add_argument('--state', type=Path)
    parser.add_argument('--allow-synthetic', action='store_true', help='Local fixture tests only; unavailable in Actions')
    args = parser.parse_args()
    try:
        if args.allow_synthetic and (os.environ.get('GITHUB_ACTIONS') == 'true' or not args.bundle):
            raise SiteError('Synthetic fixtures cannot be used for production assembly.')
        if args.bundle:
            body = args.bundle.read_bytes()
            build_site(ROOT / 'web_dashboard', args.output, body, hashlib.sha256(body).hexdigest(), allow_synthetic=args.allow_synthetic)
        elif args.state is None:
            raise SiteError('A private assembly state path is required.')
        elif args.snapshot:
            assemble_snapshot(args.output, args.state)
        elif args.assemble:
            assemble(args.output, args.state)
        else:
            recheck(args.state)
    except Exception as exc:
        message = str(exc) if isinstance(exc, SiteError) else 'Dashboard assembly failed; check private configuration and validated inputs.'
        print(message, file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
