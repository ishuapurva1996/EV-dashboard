with stations as (
    select * from {{ ref('stg_nrel_stations') }}
),

normalized as (
    select
        state_fips,
        state_abbr,
        state_name,
        -- city names in NREL are inconsistent (case + whitespace).
        -- Title-case + trim to collapse "san francisco" / "San Francisco "
        -- into a single key without losing display readability.
        initcap(trim(city)) as city,
        status_code,
        ev_level1_evse_num,
        ev_level2_evse_num,
        ev_dc_fast_num
    from stations
    where city is not null
        and trim(city) <> ''
),

aggregated as (
    select
        state_fips,
        state_abbr,
        state_name,
        city,

        count(*)                                              as total_stations,
        count(case when status_code = 'E' then 1 end)         as stations_open,
        count(case when coalesce(ev_dc_fast_num, 0) > 0
                    then 1 end)                               as stations_with_dcfast,

        sum(coalesce(ev_level1_evse_num, 0))                  as total_level1_ports,
        sum(coalesce(ev_level2_evse_num, 0))                  as total_level2_ports,
        sum(coalesce(ev_dc_fast_num, 0))                      as total_dcfast_ports,
        sum(coalesce(ev_level1_evse_num, 0)
            + coalesce(ev_level2_evse_num, 0)
            + coalesce(ev_dc_fast_num, 0))                    as total_ports
    from normalized
    group by state_fips, state_abbr, state_name, city
)

select * from aggregated
