with stations as (
    select *
    from {{ ref('stg_nrel_stations') }}
    where open_date is not null
        and status_code in ('E', 'T')
),

new_per_year as (
    select
        state_fips,
        year(open_date)                                       as vintage_year,
        count(*)                                              as new_stations,
        sum(coalesce(ev_level1_evse_num, 0))                  as new_level1_ports,
        sum(coalesce(ev_level2_evse_num, 0))                  as new_level2_ports,
        sum(coalesce(ev_dc_fast_num, 0))                      as new_dcfast_ports,
        sum(coalesce(ev_level1_evse_num, 0)
            + coalesce(ev_level2_evse_num, 0)
            + coalesce(ev_dc_fast_num, 0))                    as new_total_ports
    from stations
    group by state_fips, year(open_date)
),

year_spine as (
    select vintage_year
    from (
        select 1995 + seq4() as vintage_year
        from table(generator(rowcount => 100))
    )
    where vintage_year <= year(current_date())
),

state_year_grid as (
    select
        d.state_fips,
        d.state_abbr,
        d.state_name,
        y.vintage_year
    from {{ ref('dim_states') }} d
    cross join year_spine y
),

joined as (
    select
        g.state_fips,
        g.state_abbr,
        g.state_name,
        g.vintage_year,
        coalesce(n.new_stations, 0)        as new_stations,
        coalesce(n.new_level1_ports, 0)    as new_level1_ports,
        coalesce(n.new_level2_ports, 0)    as new_level2_ports,
        coalesce(n.new_dcfast_ports, 0)    as new_dcfast_ports,
        coalesce(n.new_total_ports, 0)     as new_total_ports
    from state_year_grid g
    left join new_per_year n
        on g.state_fips = n.state_fips
        and g.vintage_year = n.vintage_year
),

with_cumulative as (
    select
        state_fips,
        state_abbr,
        state_name,
        vintage_year,

        new_stations,
        new_level1_ports,
        new_level2_ports,
        new_dcfast_ports,
        new_total_ports,

        sum(new_stations)        over (partition by state_fips order by vintage_year) as cumulative_stations,
        sum(new_level1_ports)    over (partition by state_fips order by vintage_year) as cumulative_level1_ports,
        sum(new_level2_ports)    over (partition by state_fips order by vintage_year) as cumulative_level2_ports,
        sum(new_dcfast_ports)    over (partition by state_fips order by vintage_year) as cumulative_dcfast_ports,
        sum(new_total_ports)     over (partition by state_fips order by vintage_year) as cumulative_total_ports
    from joined
)

select * from with_cumulative
