with stations as (
    select * from {{ ref('fct_stations_by_state') }}
),

states as (
    select state_fips, census_region from {{ ref('dim_states') }}
),

latest_ev as (
    select *
    from {{ ref('fct_ev_adoption_by_state_year') }}
    qualify row_number() over (partition by state_fips order by registration_year desc) = 1
),

latest_pop as (
    select *
    from {{ ref('stg_census_population') }}
    qualify row_number() over (partition by state_fips order by acs_year desc) = 1
),

joined as (
    select
        s.state_fips,
        s.state_abbr,
        s.state_name,
        d.census_region,

        -- station snapshot
        s.total_stations,
        s.stations_open,
        s.stations_planned,
        s.stations_with_dcfast,
        s.total_level1_ports,
        s.total_level2_ports,
        s.total_dcfast_ports,
        s.total_ports,
        s.distinct_networks,
        s.earliest_station_opened,
        s.latest_station_opened,

        -- ev snapshot (latest year of AFDC data per state)
        e.registration_year as ev_data_year,
        e.bev_count,
        e.phev_count,
        e.total_ev_count,
        e.bev_share,
        e.phev_share,
        e.yoy_growth_pct as ev_yoy_growth_pct,

        -- population (latest ACS year per state)
        p.acs_year as population_year,
        p.population
    from stations s
    inner join states  d on s.state_fips = d.state_fips
    left  join latest_ev  e on s.state_fips = e.state_fips
    left  join latest_pop p on s.state_fips = p.state_fips
),

with_ratios as (
    select
        *,

        -- per-capita / density
        case when population > 0
             then total_stations * 100000.0 / population
        end as stations_per_100k_pop,

        case when population > 0
             then total_dcfast_ports * 100000.0 / population
        end as dcfast_ports_per_100k_pop,

        case when population > 0
             then total_ports * 100000.0 / population
        end as total_ports_per_100k_pop,

        case when population > 0
             then total_ev_count * 1000.0 / population
        end as evs_per_1k_pop,

        -- infrastructure vs adoption
        case when stations_open > 0
             then total_ev_count * 1.0 / stations_open
        end as evs_per_open_station,

        case when total_ev_count > 0
             then total_ports * 1.0 / total_ev_count
        end as ports_per_ev,

        -- DC fast penetration: share of stations that offer any DC fast charging
        case when total_stations > 0
             then stations_with_dcfast * 100.0 / total_stations
        end as dcfast_penetration_pct,

        -- L2 hub-vs-single-port pattern: avg L2 ports per open station
        case when stations_open > 0
             then total_level2_ports * 1.0 / stations_open
        end as avg_l2_per_open_station
    from joined
)

select * from with_ratios
