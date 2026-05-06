with stations as (
    select * from {{ ref('fct_stations_by_state_year') }}
),

ev as (
    select * from {{ ref('fct_ev_adoption_by_state_year') }}
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
        s.vintage_year as year,

        -- station flow (this year)
        s.new_stations,
        s.new_level1_ports,
        s.new_level2_ports,
        s.new_dcfast_ports,
        s.new_total_ports,

        -- station stock (end of year)
        s.cumulative_stations,
        s.cumulative_level1_ports,
        s.cumulative_level2_ports,
        s.cumulative_dcfast_ports,
        s.cumulative_total_ports,

        -- ev counts (NULL outside AFDC coverage)
        e.bev_count,
        e.phev_count,
        e.total_ev_count,
        e.yoy_growth_pct as ev_yoy_growth_pct,

        -- population held at latest ACS year (low temporal variance)
        p.population as population_latest,
        p.acs_year   as population_year
    from stations s
    left join ev e
        on s.state_fips = e.state_fips
        and s.vintage_year = e.registration_year
    left join latest_pop p
        on s.state_fips = p.state_fips
),

with_derived as (
    select
        *,

        -- density (uses latest population — acceptable for dashboard scope)
        case when population_latest > 0
             then cumulative_stations * 100000.0 / population_latest
        end as cumulative_stations_per_100k_pop,

        -- infrastructure vs adoption (only meaningful when both sides present)
        case when cumulative_stations > 0 and total_ev_count is not null
             then total_ev_count * 1.0 / cumulative_stations
        end as evs_per_station,

        -- yoy growth on the station stock
        lag(cumulative_stations) over (
            partition by state_fips order by year
        ) as prior_year_cumulative_stations,

        case
            when lag(cumulative_stations) over (
                     partition by state_fips order by year
                 ) > 0
            then (cumulative_stations
                  - lag(cumulative_stations) over (
                        partition by state_fips order by year
                    )
                 ) * 100.0
                 / lag(cumulative_stations) over (
                       partition by state_fips order by year
                   )
        end as stations_yoy_growth_pct
    from joined
)

select * from with_derived
