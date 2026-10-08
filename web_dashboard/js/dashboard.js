'use strict';
(() => {
  const $ = id => document.getElementById(id);
  const n = value => value == null ? 'Unavailable' : new Intl.NumberFormat(undefined, {maximumFractionDigits: 2}).format(value);
  const short = value => value == null ? '—' : new Intl.NumberFormat(undefined, {notation:'compact',maximumFractionDigits:2}).format(value);
  const sum = (rows, field) => {const values=rows.map(r=>r[field]).filter(v=>v!=null);return values.length?values.reduce((a,b)=>a+b,0):null;};
  const ratio = (a,b,scale=1) => a!=null&&b>0?a*scale/b:null;
  const safe = value => String(value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const stamp = value => value?new Date(value).toLocaleString(undefined,{dateStyle:'medium',timeStyle:'short'}):'Unavailable';
  const colors = ['#0875c9','#b74065','#e48a22','#449988','#7955a5'];
  let data, busy=false, pending=false;
  function theme() {
    const dark=document.documentElement.dataset.theme==='dark';
    return {dark, ink:dark?'#eef3f9':'#202b37',muted:dark?'#b5c3d2':'#566372',grid:dark?'#344454':'#e8edf2',blue:dark?'#50b5ff':'#0875c9',navy:dark?'#89aaff':'#2c5189'};
  }
  function table(id, rows, columns) {
    const host=$(id+'-table');host.replaceChildren();
    if(!rows.length){host.textContent='No records for this selection.';return;}
    const table=document.createElement('table'), caption=document.createElement('caption');
    caption.textContent=$(id).getAttribute('aria-label');caption.className='skip';table.append(caption);
    const head=document.createElement('thead'), tr=document.createElement('tr');
    columns.forEach(([,label])=>{const th=document.createElement('th');th.scope='col';th.textContent=label;tr.append(th);});head.append(tr);table.append(head);
    const body=document.createElement('tbody');
    rows.forEach(row=>{const tr=document.createElement('tr');columns.forEach(([field])=>{const cell=document.createElement('td');cell.textContent=typeof row[field]==='number'?n(row[field]):row[field]??'Unavailable';tr.append(cell);});body.append(tr);});
    table.append(body);host.append(table);
  }
  async function plot(id,traces,extra={}) {
    const host=$(id),t=theme();host.classList.remove('empty');
    const hasData=traces.some(r=>(r.y||r.z||r.values||[]).some(v=>v!=null));
    if(!hasData){if(host.data)Plotly.purge(host);host.replaceChildren();host.classList.add('empty');host.textContent='No data is available for this selection.';return;}
    const layout={paper_bgcolor:'transparent',plot_bgcolor:'transparent',font:{family:'-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif',size:12,color:t.ink},
      margin:{l:60,r:22,t:18,b:60},hoverlabel:{font:{size:13}},
      xaxis:{gridcolor:t.grid,zerolinecolor:t.grid,automargin:true,title:{standoff:12}},
      yaxis:{gridcolor:t.grid,zerolinecolor:t.grid,automargin:true,rangemode:'tozero'},
      legend:{orientation:'h',y:-.23,x:0},...extra};
    await Plotly.react(host,traces,layout,{responsive:true,displayModeBar:false,scrollZoom:false,
      topojsonURL:new URL('assets/maps/',document.baseURI).href});
  }
  function bars(id,rows,field,label,{horizontal=false,percent=false}={}) {
    const t=theme(),labels=rows.map(r=>r.state_abbr||r.census_region||r.label),values=rows.map(r=>r[field]);
    return plot(id,[{type:'bar',x:horizontal?values:labels,y:horizontal?labels:values,orientation:horizontal?'h':'v',
      marker:{color:t.blue},hovertemplate:horizontal?'%{y}: %{x:,.2f}'+(percent?'%':'')+'<extra></extra>':'%{x}: %{y:,.2f}<extra></extra>'}],
      horizontal?{margin:{l:65,r:24,t:12,b:50},xaxis:{title:label,gridcolor:t.grid,zerolinecolor:t.grid,automargin:true,ticksuffix:percent?'%':''},yaxis:{autorange:'reversed',automargin:true}}:
      {yaxis:{title:label,gridcolor:t.grid,zerolinecolor:t.grid,automargin:true,rangemode:'tozero'}});
  }
  function selectedStates(){return data.states.filter(r=>($('region').value==='all'||r.census_region===$('region').value)&&($('state').value==='all'||r.state_abbr===$('state').value));}
  function ranked(rows,field,limit){return rows.filter(r=>r[field]!=null).sort((a,b)=>b[field]-a[field]||String(a.state_abbr||a.label||a.city).localeCompare(String(b.state_abbr||b.label||b.city))).slice(0,limit);}
  async function render() {
    if(!data)return;
    if(busy){pending=true;return;}busy=true;
    try {
      const states=selectedStates(),abbrs=new Set(states.map(r=>r.state_abbr)),t=theme(),tasks=[];
      const evCovered=states.filter(r=>r.total_ev_count!=null),populationCovered=states.filter(r=>r.population>0);
      $('kpi-stations').textContent=short(sum(states,'total_stations'));$('kpi-evs').textContent=short(sum(states,'total_ev_count'));
      $('kpi-gap').textContent=n(ratio(sum(evCovered,'total_ev_count'),sum(evCovered,'stations_open')));
      $('kpi-density').textContent=n(ratio(sum(populationCovered,'total_stations'),sum(populationCovered,'population'),100000));
      const years=[...new Set(evCovered.map(r=>r.ev_data_year))].sort();
      $('ev-note').textContent=(years.length?years.join(' / ')+' registrations':'No registration coverage')+' · '+evCovered.length+'/'+states.length+' jurisdictions covered';
      $('population-note').textContent='ACS '+[...new Set(populationCovered.map(r=>r.population_year))].sort().join(' / ')+' · '+populationCovered.length+'/'+states.length+' covered';
      $('scope').textContent=states.length+' jurisdiction'+(states.length===1?'':'s')+' selected. Controls apply to all charts.';
      $('kpi-gap').parentElement.querySelector('p').textContent='EV-covered jurisdictions · registrations ÷ available stations';
      const mapRows=states.filter(r=>r.state_abbr!=='PR');
      tasks.push(plot('map',[{type:'choropleth',locationmode:'USA-states',locations:mapRows.map(r=>r.state_abbr),z:mapRows.map(r=>r.stations_per_100k_pop),text:mapRows.map(r=>safe(r.state_name)),
        colorscale:[[0,'#fce0d5'],[.5,'#ef87ab'],[1,'#a32073']],colorbar:{title:{text:'Stations / 100k'},thickness:12,len:.75},marker:{line:{color:t.dark?'#344454':'#fff',width:.8}},
        hovertemplate:'%{text}<br>%{z:,.2f} stations / 100k residents<extra></extra>'}],{geo:{scope:'usa',projection:{type:'albers usa'},bgcolor:'transparent',showlakes:false,showland:true,landcolor:t.dark?'#283440':'#eef1f5'},margin:{l:0,r:0,t:4,b:4}}));
      table('map',states,[['state_name','Jurisdiction'],['total_stations','Stations'],['population','Population'],['population_year','ACS year'],['stations_per_100k_pop','Stations / 100k']]);
      const stationRows=ranked(states,'total_stations',15);tasks.push(bars('stations',stationRows,'total_stations','Stations'));table('stations',stationRows,[['state_name','Jurisdiction'],['total_stations','Stations'],['stations_open','Available'],['stations_planned','Planned']]);
      const ports=ranked(states.map(r=>({...r,combined:r.total_level2_ports+r.total_dcfast_ports})),'combined',10).reverse();
      tasks.push(plot('ports',[{type:'bar',orientation:'h',name:'Level 2',x:ports.map(r=>r.total_level2_ports),y:ports.map(r=>r.state_abbr),marker:{color:t.blue},hovertemplate:'%{y}: %{x:,} Level 2 ports<extra></extra>'},
        {type:'bar',orientation:'h',name:'DC fast',x:ports.map(r=>r.total_dcfast_ports),y:ports.map(r=>r.state_abbr),marker:{color:t.navy},hovertemplate:'%{y}: %{x:,} DC fast ports<extra></extra>'}],{barmode:'stack',margin:{l:45,r:20,t:12,b:80},xaxis:{title:'Ports',gridcolor:t.grid,automargin:true},yaxis:{automargin:true}}));
      table('ports',ports.slice().reverse(),[['state_name','Jurisdiction'],['total_level2_ports','Level 2 ports'],['total_dcfast_ports','DC fast ports']]);
      const trendStates=ranked(states,'total_stations',5),start=Number($('start-year').value);
      const growth=data.growth.filter(r=>abbrs.has(r.state_abbr)&&r.year>=start);
      const trendRows=growth.filter(r=>trendStates.some(s=>s.state_abbr===r.state_abbr));
      tasks.push(plot('growth',trendStates.map((s,i)=>{const rows=trendRows.filter(r=>r.state_abbr===s.state_abbr);return {type:'scatter',mode:'lines+markers',name:s.state_abbr,x:rows.map(r=>r.year),y:rows.map(r=>r.cumulative_stations),line:{color:colors[i]},hovertemplate:safe(s.state_name)+'<br>%{x}: %{y:,} known openings<extra></extra>'};}),{xaxis:{dtick:1,gridcolor:t.grid,automargin:true},yaxis:{title:'Cumulative known openings',gridcolor:t.grid,automargin:true,rangemode:'tozero'},margin:{l:65,r:15,t:20,b:85}}));
      table('growth',trendRows,[['state_name','Jurisdiction'],['year','Opening year'],['new_stations','New known openings'],['cumulative_stations','Cumulative known openings']]);
      const gap=ranked(states,'evs_per_open_station',15);tasks.push(bars('gap',gap,'evs_per_open_station','EVs / open station'));table('gap',gap,[['state_name','Jurisdiction'],['total_ev_count','Registered EVs'],['ev_data_year','Registration year'],['stations_open','Available stations'],['evs_per_open_station','EVs / open station']]);
      const registrationYears=[...new Set(growth.filter(r=>r.total_ev_count!=null).map(r=>r.year))].sort((a,b)=>a-b);
      const registrations=registrationYears.map(year=>{const rows=growth.filter(r=>r.year===year&&r.total_ev_count!=null);return {year,bev_count:sum(rows,'bev_count'),phev_count:sum(rows,'phev_count'),total_ev_count:sum(rows,'total_ev_count'),coverage:rows.length};});
      tasks.push(plot('registrations',[{type:'scatter',mode:'lines+markers',name:'Battery electric',x:registrationYears,y:registrations.map(r=>r.bev_count),line:{color:t.blue},hovertemplate:'%{x}: %{y:,} BEVs<extra></extra>'},
        {type:'scatter',mode:'lines+markers',name:'Plug-in hybrid',x:registrationYears,y:registrations.map(r=>r.phev_count),line:{color:t.navy},hovertemplate:'%{x}: %{y:,} PHEVs<extra></extra>'}],{xaxis:{dtick:1,gridcolor:t.grid,automargin:true},yaxis:{title:'Registered vehicles',gridcolor:t.grid,automargin:true,rangemode:'tozero'},margin:{l:75,r:20,t:20,b:80}}));
      table('registrations',registrations,[['year','Registration year'],['bev_count','BEVs'],['phev_count','PHEVs'],['total_ev_count','Total EVs'],['coverage','Jurisdictions with data']]);
      const cities=ranked(data.cities.filter(r=>abbrs.has(r.state_abbr)).map(r=>({...r,label:r.city+', '+r.state_abbr})),'total_stations',15).reverse();
      tasks.push(bars('cities',cities.map(r=>({...r,state_abbr:r.label})),'total_stations','Stations',{horizontal:true}));
      table('cities',cities.slice().reverse(),[['city','City'],['state_abbr','State'],['total_stations','Stations'],['stations_open','Available stations']]);
      const regions=[...new Set(states.map(r=>r.census_region))].sort().map(census_region=>{const rows=states.filter(r=>r.census_region===census_region),ev=rows.filter(r=>r.total_ev_count!=null);return {census_region,total_stations:sum(rows,'total_stations'),total_ev_count:sum(ev,'total_ev_count'),stations_open:sum(ev,'stations_open'),evs_per_open_station:ratio(sum(ev,'total_ev_count'),sum(ev,'stations_open'))};});
      tasks.push(plot('regions',[{type:'pie',hole:.5,labels:regions.map(r=>r.census_region),values:regions.map(r=>r.total_stations),sort:false,textinfo:'label+percent',marker:{colors},hovertemplate:'%{label}: %{value:,} stations (%{percent})<extra></extra>'}],{margin:{l:35,r:35,t:30,b:60},showlegend:false}));
      table('regions',regions,[['census_region','Region'],['total_stations','Stations']]);
      tasks.push(bars('region-gap',regions,'evs_per_open_station','EVs / open station',{horizontal:true}));table('region-gap',regions,[['census_region','Region'],['total_ev_count','Registered EVs'],['stations_open','EV-covered available stations'],['evs_per_open_station','EVs / open station']]);
      const fast=ranked(states,'dcfast_penetration_pct',20),l2=ranked(states,'avg_l2_per_open_station',20);
      tasks.push(bars('fast',fast,'dcfast_penetration_pct','Share of station locations',{horizontal:true,percent:true}));table('fast',fast,[['state_name','Jurisdiction'],['stations_with_dcfast','Stations with DC fast'],['total_stations','All stations'],['dcfast_penetration_pct','Share (%)']]);
      tasks.push(bars('l2',l2,'avg_l2_per_open_station','Level 2 ports / open station',{horizontal:true}));table('l2',l2,[['state_name','Jurisdiction'],['total_level2_ports','Level 2 ports'],['stations_open','Available stations'],['avg_l2_per_open_station','Level 2 / open station']]);
      await Promise.all(tasks);
      document.body.dataset.ready='true';
    } catch(error) { $('status').className='error';$('status').textContent='A chart could not be displayed. Reload the dashboard or use its data tables.';console.error('EV chart rendering failed.'); }
    finally {busy=false;if(pending){pending=false;render();}}
  }
  function populateStates(){const region=$('region').value,old=$('state').value;$('state').replaceChildren(new Option('All jurisdictions','all'));data.states.filter(r=>region==='all'||r.census_region===region).forEach(r=>$('state').add(new Option(r.state_name,r.state_abbr)));if([...$('state').options].some(o=>o.value===old))$('state').value=old;}
  function freshness(){const m=data.metadata,age=m.stations_captured_at?(Date.now()-Date.parse(m.stations_captured_at))/864e5:null;
    $('status').className=(m.synthetic||age==null||age>2)?'warning':'';
    $('status').textContent=m.synthetic?'Preview uses synthetic test data. Production publication rejects this bundle.':age==null?'Station capture date is unavailable. Source freshness cannot be determined.':(age>2?'Station snapshot is more than two days old. ':'')+'Stations captured '+stamp(m.stations_captured_at)+' · Warehouse completed '+stamp(m.warehouse_completed_at);
    const dates=[['Station source captured',stamp(m.stations_captured_at)],['Station source last updated',stamp(m.stations_source_updated_at)],['Population source captured',stamp(m.population_captured_at)],['Warehouse build completed',stamp(m.warehouse_completed_at)],['Public export created',stamp(m.exported_at)],['Export identity',m.bundle_id]];
    $('dates').replaceChildren();dates.forEach(([label,value])=>{const dt=document.createElement('dt'),dd=document.createElement('dd');dt.textContent=label;dd.textContent=value;$('dates').append(dt,dd);});
  }
  async function load(){ $('retry').hidden=true;$('status').className='';$('status').textContent='Loading the latest validated dashboard data…';$('content').hidden=true;
    try{const response=await fetch(new URL('data/dashboard.json',document.baseURI),{cache:'no-store'});if(!response.ok)throw new Error();const body=await response.json();
      if(body.schema_version!==1||!body.metadata||!['states','regions','growth','cities'].every(k=>Array.isArray(body[k])))throw new Error();data=body;
      $('region').replaceChildren(new Option('All regions','all'));[...new Set(data.states.map(r=>r.census_region))].sort().forEach(r=>$('region').add(new Option(r,r)));populateStates();
      const years=[...new Set(data.growth.map(r=>r.year))].sort((a,b)=>a-b);$('start-year').replaceChildren();years.forEach(y=>$('start-year').add(new Option(y,y)));if(years.includes(2020))$('start-year').value='2020';
      freshness();$('content').hidden=false;await render();
    }catch(error){data=null;$('status').className='error';$('status').textContent='Dashboard data could not be loaded. Retry, or check the project’s latest deployment.';$('retry').hidden=false;}
  }
  try{if(localStorage.getItem('ev-theme')==='dark')document.documentElement.dataset.theme='dark';}catch{}
  const syncTheme=()=>{const dark=document.documentElement.dataset.theme==='dark';$('theme').textContent=dark?'Light theme':'Dark theme';$('theme').setAttribute('aria-pressed',String(dark));};syncTheme();
  $('theme').addEventListener('click',()=>{const dark=document.documentElement.dataset.theme!=='dark';document.documentElement.dataset.theme=dark?'dark':'light';try{localStorage.setItem('ev-theme',dark?'dark':'light');}catch{}syncTheme();render();});
  $('region').addEventListener('change',()=>{populateStates();render();});$('state').addEventListener('change',render);$('start-year').addEventListener('change',render);$('retry').addEventListener('click',load);
  load();
})();
