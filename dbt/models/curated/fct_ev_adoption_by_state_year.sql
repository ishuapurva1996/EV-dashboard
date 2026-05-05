with afdc as (
    select * from {{ ref('stg_afdc_registrations') }}
),

base as (
    select
        state_fips,
        state_abbr,
        state_name,
        registration_year,
        bev_count,
        phev_count,
        bev_count + phev_count as total_ev_count
    from afdc
),

shares as (
    select
        *,
        case
            when total_ev_count > 0 then bev_count * 1.0 / total_ev_count
            else null
        end as bev_share,
        case
            when total_ev_count > 0 then phev_count * 1.0 / total_ev_count
            else null
        end as phev_share,
        lag(total_ev_count) over (
            partition by state_fips
            order by registration_year
        ) as prior_year_total
    from base
),

with_growth as (
    select
        *,
        case
            when prior_year_total > 0
            then (total_ev_count - prior_year_total) * 100.0 / prior_year_total
            else null
        end as yoy_growth_pct
    from shares
)

select
    state_fips,
    state_abbr,
    state_name,
    registration_year,
    bev_count,
    phev_count,
    total_ev_count,
    bev_share,
    phev_share,
    prior_year_total,
    yoy_growth_pct
from with_growth
