import copy
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'scripts'),str(ROOT/'plugins')]
from fixtures import bundle
from export_dashboard import validate_bundle,serialize,ExportError,read,identifier
from build_dashboard_site import build_site,SiteError,read_latest
from ev_publication import publish_bundle,check_receipt,check_eligibility,STATION_DAG,CENSUS_DAG,DBT_DAG,PREDECESSORS,MUTATORS

class ContractTests(unittest.TestCase):
    def test_complete_fixture_and_production_rejection(self):
        b=bundle();validate_bundle(b,allow_synthetic=True)
        with self.assertRaises(ExportError):validate_bundle(b)
    def test_private_fields_nonfinite_missing_coverage_and_mismatched_totals(self):
        for edit in [lambda b:b['states'][0].update(password='private'),lambda b:b['states'][0].update(total_stations=float('nan')),
                     lambda b:b['states'][0].update(total_ports=1),lambda b:b['growth'].pop(),
                     lambda b:b['regions'][0].update(total_stations=999999),lambda b:b['cities'][0].update(state_rank=8),
                     lambda b:b['metadata'].update(stations_captured_at='2027-01-01T00:00:00Z')]:
            b=bundle();edit(b)
            with self.subTest(edit=edit),self.assertRaises((ExportError,ValueError)):validate_bundle(b,allow_synthetic=True)
    def test_bounded_reads_and_identifier_validation(self):
        c=Mock();cur=c.cursor.return_value;cur.description=[('A',)];cur.fetchmany.return_value=[(1,),(2,)]
        with self.assertRaises(ExportError):read(c,'SELECT A',['a'],1)
        cur.close.assert_called_once()
        with self.assertRaises(ExportError):identifier('public; DROP TABLE x')
    def test_assembly_checksum_allowlist_and_synthetic_gate(self):
        b=bundle();body=serialize(b);sha=hashlib.sha256(body).hexdigest()
        with tempfile.TemporaryDirectory() as folder:
            source=Path(folder)/'source';source.mkdir();(source/'.env').write_text('private')
            from build_dashboard_site import ASSETS
            for name in ASSETS:
                p=source/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text('public')
            out=Path(folder)/'site'
            with self.assertRaises(SiteError):build_site(source,out,body,sha)
            self.assertFalse(out.exists())
            build_site(source,out,body,sha,allow_synthetic=True)
            self.assertEqual((out/'data/dashboard.json').read_bytes(),body)
            self.assertFalse((out/'.env').exists())
            with self.assertRaises(SiteError):build_site(source,out,body,'0'*64,allow_synthetic=True)
            self.assertEqual((out/'data/dashboard.json').read_bytes(),body)

class S3Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class MemoryS3:
    def __init__(self):self.objects={};self.etags={};self.fail_bundle=False;self.fail_pointer=False
    def put_object(self,**kw):
        key=kw['Key']
        if self.fail_bundle and '/bundles/' in key:raise S3Error('AccessDenied')
        if self.fail_pointer and key.endswith('latest-success.json'):raise S3Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.objects:raise S3Error('PreconditionFailed')
        if kw.get('IfMatch') and kw['IfMatch']!=self.etags.get(key):raise S3Error('PreconditionFailed')
        self.objects[key]=kw['Body'];self.etags[key]=hashlib.sha256(kw['Body']).hexdigest()
    def get_object(self,**kw):
        key=kw['Key']
        if key not in self.objects:raise S3Error('NoSuchKey')
        return {'Body':io.BytesIO(self.objects[key]),'ETag':self.etags[key]}
class PublicationTests(unittest.TestCase):
    def test_immutable_handoff_and_failed_guard_preserve_pointer(self):
        s3=MemoryS3();b=bundle();body=serialize(b)
        publish_bundle(s3,'bucket','dashboard/ev',body,b['metadata'],'run',lambda:None)
        key='dashboard/ev/latest-success.json';old=s3.objects[key]
        pointer,_=read_latest(s3,'bucket','dashboard/ev');self.assertEqual(pointer['bundle_id'],b['metadata']['bundle_id'])
        def fail():raise RuntimeError('changed warehouse')
        b['metadata']['exported_at']='2026-10-08T09:06:00Z'
        with self.assertRaises(RuntimeError):publish_bundle(s3,'bucket','dashboard/ev',serialize(b),b['metadata'],'run2',fail)
        self.assertEqual(old,s3.objects[key])
        s3.fail_pointer=True
        with self.assertRaises(RuntimeError):publish_bundle(s3,'bucket','dashboard/ev',serialize(b),b['metadata'],'run2',lambda:None)
        self.assertEqual(old,s3.objects[key])
    def test_failed_upload_does_not_create_pointer(self):
        s3=MemoryS3();s3.fail_bundle=True;b=bundle()
        with self.assertRaises(S3Error):publish_bundle(s3,'bucket','dashboard/ev',serialize(b),b['metadata'],'run',lambda:None)
        self.assertNotIn('dashboard/ev/latest-success.json',s3.objects)
    def test_first_pointer_403_requires_confirmed_scoped_absence(self):
        s3=MemoryS3();original=s3.get_object
        def denied_pointer(**kw):
            if kw['Key'].endswith('latest-success.json'):raise S3Error('AccessDenied')
            return original(**kw)
        s3.get_object=denied_pointer
        s3.list_objects_v2=Mock(return_value={'Contents':[], 'IsTruncated':False})
        b=bundle()
        publish_bundle(s3,'bucket','dashboard/ev',serialize(b),b['metadata'],'first',lambda:None)
        s3.list_objects_v2.assert_called_once_with(Bucket='bucket',Prefix='dashboard/ev/latest-success.json',MaxKeys=1)
        old=s3.objects['dashboard/ev/latest-success.json']
        s3.list_objects_v2.return_value={'Contents':[{'Key':'dashboard/ev/latest-success.json'}], 'IsTruncated':False}
        with self.assertRaises(S3Error):publish_bundle(s3,'bucket','dashboard/ev',serialize(b),b['metadata'],'retry',lambda:None)
        self.assertEqual(old,s3.objects['dashboard/ev/latest-success.json'])
    def test_pointer_403_never_initializes_from_uncertain_or_denied_listing(self):
        for listing in ({'Contents':[], 'IsTruncated':True}, {'Contents':[]}, S3Error('AccessDenied')):
            with self.subTest(listing=listing):
                s3=MemoryS3();original=s3.get_object
                def denied_pointer(**kw):
                    if kw['Key'].endswith('latest-success.json'):raise S3Error('AccessDenied')
                    return original(**kw)
                s3.get_object=denied_pointer
                s3.list_objects_v2=Mock()
                if isinstance(listing,Exception):s3.list_objects_v2.side_effect=listing
                else:s3.list_objects_v2.return_value=listing
                b=bundle()
                with self.assertRaises(S3Error):publish_bundle(s3,'bucket','dashboard/ev',serialize(b),b['metadata'],'first',lambda:None)
                self.assertNotIn('dashboard/ev/latest-success.json',s3.objects)
    def test_older_export_does_not_overwrite_newer(self):
        s3=MemoryS3();b=bundle();publish_bundle(s3,'bucket','dashboard/ev',serialize(b),b['metadata'],'new',lambda:None)
        old=s3.objects['dashboard/ev/latest-success.json'];b['metadata']['warehouse_completed_at']='2026-10-07T09:00:00Z'
        with self.assertRaises(RuntimeError):publish_bundle(s3,'bucket','dashboard/ev',serialize(b),b['metadata'],'old',lambda:None)
        self.assertEqual(old,s3.objects['dashboard/ev/latest-success.json'])
    def test_receipt_binds_attempt_interval(self):
        task=dict(dag_id=DBT_DAG,dag_run_id='run',task_id='dbt_run',try_number=1,start_date='2026-10-08T08:00:00Z',end_date='2026-10-08T08:05:00Z')
        r=dict(dag_id=DBT_DAG,run_id='run',task_id='dbt_run',try_number=1,started_at='2026-10-08T08:00:01Z',completed_at='2026-10-08T08:04:59Z')
        check_receipt(r,task)
        for change in [dict(try_number=2),dict(run_id='other'),dict(completed_at='2026-10-08T08:05:01Z')]:
            with self.assertRaises(RuntimeError):check_receipt({**r,**change},task)

if __name__=='__main__':unittest.main()

class FakeMetadata:
    def __init__(self):
        self.rows={};self.receipts={}
        run='current'
        intervals={STATION_DAG:('2026-10-08',7),CENSUS_DAG:('2026-01-01',7),DBT_DAG:('2026-10-08',8)}
        for dag,names in PREDECESSORS.items():
            day,hour=intervals[dag];rows=[]
            for index,name in enumerate(names):
                minute=index*2
                start=f'{day}T{hour:02}:{minute:02}:00Z';end=f'{day}T{hour:02}:{minute+1:02}:00Z'
                row=dict(dag_id=dag,dag_run_id=run,task_id=name,try_number=1,map_index=-1,state='success',start_date=start,end_date=end)
                rows.append(row)
                receipt=dict(dag_id=dag,run_id=run,task_id=name,try_number=1,started_at=start,completed_at=end,
                             source_captured_at=start,source_last_updated_at=start,warehouse_fingerprint='a'*64)
                self.receipts[(dag,run,name)]=receipt
            self.rows[dag]=rows
        self.rows[STATION_DAG].append(dict(dag_id=STATION_DAG,dag_run_id='previous-day',task_id='extract_transform_load',
            state='success',try_number=1,start_date='2026-10-07T07:00:00Z',end_date='2026-10-07T07:05:00Z'))
    def tasks(self,dag,run='~'):return copy.deepcopy([r for r in self.rows[dag] if run=='~' or r['dag_run_id']==run])
    def run(self,dag,run):return {'conf':{'nrel_run_id':'current','census_run_id':'current'}}
    def receipt(self,dag,run,task):return copy.deepcopy(self.receipts[(dag,run,task)])

class EligibilityTests(unittest.TestCase):
    def test_daily_export_can_reuse_annual_population(self):
        result=check_eligibility(FakeMetadata(),'current')
        self.assertEqual(result['population_captured_at'],'2026-01-01T07:04:00Z')
    def test_manual_success_without_real_receipt_is_rejected(self):
        api=FakeMetadata();api.receipts[(DBT_DAG,'current','dbt_run')]['try_number']=0
        with self.assertRaises(RuntimeError):check_eligibility(api,'current')
    def test_failed_predecessor_concurrent_writer_or_changed_attempt_is_rejected(self):
        api=FakeMetadata();api.rows[DBT_DAG][1]['state']='failed'
        with self.assertRaises(RuntimeError):check_eligibility(api,'current')
        api=FakeMetadata();api.rows[STATION_DAG][-1].update(state='running',end_date=None)
        with self.assertRaises(RuntimeError):check_eligibility(api,'current')
        api=FakeMetadata();expected=check_eligibility(api,'current');api.rows[DBT_DAG][0]['end_date']='2026-10-08T08:01:01Z'
        with self.assertRaises(RuntimeError):check_eligibility(api,'current',expected=expected)
