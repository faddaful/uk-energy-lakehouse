-- Gold mart: the next 48 hours for one home region, ranked by
-- greenness and (where known) cheapness. Deliberately not the same
-- model as mart_best_hours_today, even though the two are nearly
-- identical: this one exists specifically to be exported as
-- greenest_hours_next_48h.json (see lakehouse.products.api_export), a
-- public artifact with its own schema contract, while
-- mart_best_hours_today exists specifically to be a 24-hour Streamlit
-- view. Collapsing them into one parametrised mart would couple a
-- public API's shape to whatever the dashboard needs next, which is
-- the wrong direction for either of them to depend on. Small
-- duplication, not a DRY failure: see fct_regional_generation_mix's own
-- comment for the same call made elsewhere in this project.
--
-- home_region_id = 8 (West Midlands), same as mart_best_hours_today and
-- carbon_intensity.py's own --region default: the one region this
-- pipeline actually ingests. "by region" in the product's own name is
-- aspirational until a second region is ever ingested; change this
-- value (and carbon_intensity.py's default) together if that happens.
--
-- system_sell_price_gbp_per_mwh is NULL for almost every row: imbalance
-- prices are only known after a settlement period has happened, so
-- there is no genuine forward price for most of the next 48 hours. Not
-- a bug, see mart_best_hours_today's own comment.
--
-- generated_at is not decorative: an API response that cannot tell a
-- consumer how stale it is has failed at its main job.

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
      and i.half_hour_start_utc < now() + interval 48 hour

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
