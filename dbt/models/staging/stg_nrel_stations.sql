with source as (
    select * from {{ source('raw_ev', 'nrel_stations') }}
),

states as (
    select * from {{ ref('dim_states') }}
),

renamed as (
    select
        s.id                  as station_id,
        d.state_fips,
        upper(s.state)        as state_abbr,
        d.state_name,
        s.station_name,
        s.street_address,
        s.city,
        s.zip,
        s.latitude,
        s.longitude,
        s.ev_level1_evse_num,
        s.ev_level2_evse_num,
        s.ev_dc_fast_num,
        s.ev_connector_types,
        s.ev_network,
        s.ev_pricing,
        s.status_code,
        s.access_code,
        s.facility_type,
        s.date_last_confirmed,
        s.open_date,
        s.updated_at
    from source s
    inner join states d on upper(s.state) = d.state_abbr
)

select * from renamed
