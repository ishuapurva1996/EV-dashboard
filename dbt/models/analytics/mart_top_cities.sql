with cities as (
    select * from {{ ref('fct_stations_by_city') }}
),

ranked as (
    select
        state_fips,
        state_abbr,
        state_name,
        city,
        total_stations,
        stations_open,
        stations_with_dcfast,
        total_level2_ports,
        total_dcfast_ports,
        total_ports,

        row_number() over (order by total_stations desc, city)
            as national_rank,

        row_number() over (partition by state_fips
                           order by total_stations desc, city)
            as state_rank
    from cities
)

select * from ranked
