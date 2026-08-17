-- Gold mart: the next 24 hours for one home region, ranked by
-- greenness and (where known) cheapness. Shaped directly for the
-- Streamlit "when should I use electricity" view -- no aggregation logic
-- belongs in that app if this mart already has the answer.
--
-- home_region_id = 8 (West Midlands) matches carbon_intensity.py's own
-- --region default, i.e. the one region this pipeline actually ingests
-- in practice. Change both together if that ever changes.
--
-- system_sell_price_gbp_per_mwh is NULL for almost every row here, and
-- that is correct, not a bug: imbalance prices are only known after a
-- settlement period has happened, so there is no genuine forward price
-- for most of the next 24 hours. Do not fabricate one. A real
-- forward-looking price (e.g. Octopus Agile's day-ahead rates) is a
-- later-phase data source, and swapping it in here is a one-model
-- change once it exists.
--
-- generated_at is not decorative: a dashboard that cannot tell you how
-- stale it is has failed at its main job. The Streamlit app should
-- display it next to every figure this mart produces.

{{ config(materialized='table') }}

{% set home_region_id = 8 %}

with upcoming as (

    select
        i.half_hour_start_utc,
        i.region_id,
        r.region_short_name,
        i.intensity_forecast_gco2_per_kwh
    from {{ ref('fct_regional_intensity') }} i
    join {{ ref('dim_region') }} r on i.region_key = r.region_key
    where i.region_id = {{ home_region_id }}
      and i.half_hour_start_utc >= date_trunc('hour', now())
      and i.half_hour_start_utc < now() + interval 24 hour

),

with_price as (

    select
        u.*,
        p.system_sell_price_gbp_per_mwh
    from upcoming u
    left join {{ ref('fct_settlement_period') }} p
        on u.half_hour_start_utc = p.settlement_period_start_utc

)

select
    half_hour_start_utc,
    region_short_name,
    intensity_forecast_gco2_per_kwh,
    system_sell_price_gbp_per_mwh,

    row_number() over (order by intensity_forecast_gco2_per_kwh asc)
        as greenness_rank,
    case
        when system_sell_price_gbp_per_mwh is not null
        then row_number() over (
            order by case when system_sell_price_gbp_per_mwh is null then 1 else 0 end,
                     system_sell_price_gbp_per_mwh asc
        )
    end as cheapness_rank,

    current_timestamp as generated_at

from with_price
order by half_hour_start_utc
