"""Explicitly synthetic EV data for contract and local browser checks only."""
from pathlib import Path
import csv
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from export_dashboard import FIELDS,SOURCES


def bundle():
    root=Path(__file__).resolve().parents[1]
    with (root/'dbt/seeds/dim_states.csv').open() as handle:
        dimensions=list(csv.DictReader(handle))
    states=[];growth=[];cities=[]
    for index,d in enumerate(dimensions):
        stations=(index+1)*25
        r={f:0 for f in FIELDS['states']}
        r.update(d)
        r.update(total_stations=stations,stations_open=stations-5,stations_planned=3,stations_with_dcfast=stations//3,
                 total_level1_ports=10,total_level2_ports=stations*2,total_dcfast_ports=stations,total_ports=10+stations*3,
                 ev_data_year=2024,bev_count=stations*45,phev_count=stations*5,total_ev_count=stations*50,
                 population_year=2024,population=(index+1)*200000)
        r.update(stations_per_100k_pop=stations*1e5/r['population'],dcfast_ports_per_100k_pop=stations*1e5/r['population'],
                 evs_per_1k_pop=r['total_ev_count']*1e3/r['population'],evs_per_open_station=r['total_ev_count']/r['stations_open'],
                 ports_per_ev=r['total_ports']/r['total_ev_count'],dcfast_penetration_pct=r['stations_with_dcfast']*100/stations,
                 avg_l2_per_open_station=r['total_level2_ports']/r['stations_open'])
        if d['state_abbr']=='PR':
            for field in ('ev_data_year','bev_count','phev_count','total_ev_count','evs_per_1k_pop','evs_per_open_station','ports_per_ev'):r[field]=None
        states.append(r)
        for year in range(1995,2027):
            g={f:0 for f in FIELDS['growth']};g.update({k:d[k] for k in ('state_fips','state_abbr','state_name')})
            g.update(year=year,new_stations=index+1,new_total_ports=3*(index+1),cumulative_stations=(year-1994)*(index+1),
                     cumulative_total_ports=3*(year-1994)*(index+1),population_year=2024,population_latest=r['population'],
                     bev_count=None,phev_count=None,total_ev_count=None,ev_yoy_growth_pct=None,stations_yoy_growth_pct=None)
            if 2020<=year<=2024 and d['state_abbr']!='PR':
                g.update(bev_count=(year-2019)*stations*9,phev_count=(year-2019)*stations,total_ev_count=(year-2019)*stations*10)
            growth.append(g)
        for rank in range(1,4):
            c={f:0 for f in FIELDS['cities']};c.update({k:d[k] for k in ('state_fips','state_abbr','state_name')})
            c.update(city='Synthetic City '+str(rank),total_stations=stations//(rank+3),stations_open=stations//(rank+3)-1,
                     stations_with_dcfast=1,total_level2_ports=10,total_dcfast_ports=5,total_ports=15,state_rank=rank,national_rank=1)
            cities.append(c)
    states.sort(key=lambda r:r['state_abbr']);growth.sort(key=lambda r:(r['state_abbr'],r['year']))
    for rank,r in enumerate(sorted(cities,key=lambda r:(-r['total_stations'],r['city'],r['state_abbr'])),1):r['national_rank']=rank
    cities.sort(key=lambda r:(r['state_abbr'],r['state_rank']))
    regions=[]
    for region in sorted({r['census_region'] for r in states}):
        members=[r for r in states if r['census_region']==region]
        r={f:0 for f in FIELDS['regions']};r.update(census_region=region,state_count=len(members))
        for field in ('total_stations','stations_open','stations_with_dcfast','total_level1_ports','total_level2_ports','total_dcfast_ports','total_ports','total_ev_count','bev_count','phev_count','population'):
            values=[m[field] for m in members if m[field] is not None];r[field]=sum(values) if values else None
        for field,a,b,scale in [('stations_per_100k_pop','total_stations','population',1e5),('evs_per_1k_pop','total_ev_count','population',1e3),('evs_per_open_station','total_ev_count','stations_open',1),('dcfast_penetration_pct','stations_with_dcfast','total_stations',100)]:
            r[field]=r[a]*scale/r[b] if r[a] is not None and r[b] else None
        regions.append(r)
    metadata=dict(bundle_id='a'*32,synthetic=True,warehouse_completed_at='2026-10-08T09:00:00Z',exported_at='2026-10-08T09:05:00Z',
                  stations_captured_at='2026-10-08T08:00:00Z',stations_source_updated_at='2026-10-07T03:00:00Z',population_captured_at='2026-10-08T07:00:00Z',sources=SOURCES)
    return dict(schema_version=1,metadata=metadata,states=states,regions=regions,growth=growth,cities=cities)
