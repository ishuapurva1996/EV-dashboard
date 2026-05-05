with stations as (
    select * from {{ ref('stg_nrel_stations') }}
),

aggregated as (
    select
        state_fips,
        state_abbr,
        state_name,

        count(*)                                              as total_stations,
        count(case when status_code = 'E' then 1 end)         as stations_open,
        count(case when status_code = 'P' then 1 end)         as stations_planned,
        count(case when status_code = 'T' then 1 end)         as stations_temporary,

        sum(coalesce(ev_level1_evse_num, 0))                  as total_level1_ports,
        sum(coalesce(ev_level2_evse_num, 0))                  as total_level2_ports,
        sum(coalesce(ev_dc_fast_num, 0))                      as total_dcfast_ports,
        sum(coalesce(ev_level1_evse_num, 0)
            + coalesce(ev_level2_evse_num, 0)
            + coalesce(ev_dc_fast_num, 0))                    as total_ports,

        count(distinct ev_network)                            as distinct_networks,
        min(open_date)                                        as earliest_station_opened,
        max(open_date)                                        as latest_station_opened
    from stations
    group by state_fips, state_abbr, state_name
)

select * from aggregated
