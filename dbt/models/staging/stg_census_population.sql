with source as (
    select * from {{ source('raw_ev', 'census_population') }}
),

states as (
    select * from {{ ref('dim_states') }}
),

renamed as (
    select
        s.state_fips,
        d.state_abbr,
        d.state_name,
        s.population,
        s.acs_year,
        s.loaded_at
    from source s
    inner join states d on s.state_fips = d.state_fips
)

select * from renamed
