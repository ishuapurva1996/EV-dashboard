with source as (
    select * from {{ source('raw_ev', 'afdc_registrations') }}
),

states as (
    select * from {{ ref('dim_states') }}
),

renamed as (
    select
        d.state_fips,
        d.state_abbr,
        s.state_name,
        s.registration_year,
        s.bev_count,
        s.phev_count
    from source s
    inner join states d on s.state_name = d.state_name
)

select * from renamed
