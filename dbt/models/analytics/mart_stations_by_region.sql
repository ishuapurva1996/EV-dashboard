with overview as (
    select * from {{ ref('mart_state_ev_overview') }}
),

aggregated as (
    select
        census_region,

        count(*)                              as state_count,

        sum(total_stations)                   as total_stations,
        sum(stations_open)                    as stations_open,
        sum(stations_with_dcfast)             as stations_with_dcfast,
        sum(total_level1_ports)               as total_level1_ports,
        sum(total_level2_ports)               as total_level2_ports,
        sum(total_dcfast_ports)               as total_dcfast_ports,
        sum(total_ports)                      as total_ports,

        sum(total_ev_count)                   as total_ev_count,
        sum(bev_count)                        as bev_count,
        sum(phev_count)                       as phev_count,
        sum(population)                       as population
    from overview
    group by census_region
),

with_ratios as (
    select
        *,

        case when population > 0
             then total_stations * 100000.0 / population
        end as stations_per_100k_pop,

        case when population > 0
             then total_ev_count * 1000.0 / population
        end as evs_per_1k_pop,

        case when stations_open > 0
             then total_ev_count * 1.0 / stations_open
        end as evs_per_open_station,

        case when total_stations > 0
             then stations_with_dcfast * 100.0 / total_stations
        end as dcfast_penetration_pct
    from aggregated
)

select * from with_ratios
