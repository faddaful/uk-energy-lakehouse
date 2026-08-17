-- Gold mart: one row per calendar day, shaped for a dashboard's daily
-- overview -- price, generation mix, and carbon intensity, plus a
-- completeness flag so a partial day (today, or a day still missing
-- data) can be greyed out rather than plotted as if it were final.
--
-- intensity_daily filters to region_id = 8 (West Midlands), not 18
-- (Great Britain): this pipeline's extractor only ever lands the one
-- region passed to it, and its default -- and the only region actually
-- ingested in practice so far -- is 8, not the GB-wide aggregate. Region
-- 18 has no rows in fct_regional_intensity at all, so filtering on it
-- here would silently produce a NULL average intensity every day instead
-- of a real one. See carbon_intensity.py's --region default.
--
-- Division by 2 converts MW (an instantaneous half-hourly reading) to
-- MWh (energy over that half hour). Getting this unit conversion right,
-- and not silently treating MW readings as MWh, is exactly the kind of
-- unglamorous correctness this whole project is arguing for.

{{ config(materialized='table') }}

with price_daily as (

    select
        settlement_date,
        avg(system_sell_price_gbp_per_mwh) as avg_price_gbp_per_mwh,
        min(system_sell_price_gbp_per_mwh) as min_price_gbp_per_mwh,
        max(system_sell_price_gbp_per_mwh) as max_price_gbp_per_mwh,
        count(*)                           as periods_with_price,
        count(*) filter (
            where system_sell_price_gbp_per_mwh < 0
        )                                  as negative_price_periods
    from {{ ref('fct_settlement_period') }}
    group by 1

),

generation_daily as (

    select
        g.settlement_date,
        sum(g.generation_mw) / 2.0 as total_generation_mwh,
        sum(case when f.is_renewable then g.generation_mw end) / 2.0
            as renewable_mwh,
        sum(case when f.fuel_category = 'Fossil' then g.generation_mw end) / 2.0
            as fossil_mwh,
        sum(case when f.is_interconnector then g.generation_mw end) / 2.0
            as net_imports_mwh
    from {{ ref('fct_generation') }} g
    join {{ ref('dim_fuel_type') }} f on g.fuel_type_key = f.fuel_type_key
    group by 1

),

intensity_daily as (

    select
        intensity_date,
        avg(intensity_forecast_gco2_per_kwh) as avg_intensity_gco2_per_kwh
    from {{ ref('fct_regional_intensity') }}
    where region_id = 8
    group by 1

)

select
    d.date_key,
    d.date_day,
    d.day_name,
    d.is_weekend,
    d.is_bank_holiday,
    d.settlement_periods_in_day,

    p.avg_price_gbp_per_mwh,
    p.min_price_gbp_per_mwh,
    p.max_price_gbp_per_mwh,
    p.negative_price_periods,
    p.periods_with_price,

    g.total_generation_mwh,
    g.renewable_mwh,
    g.fossil_mwh,
    g.net_imports_mwh,
    case
        when g.total_generation_mwh > 0
        then round(100.0 * g.renewable_mwh / g.total_generation_mwh, 2)
    end as renewable_share_pct,

    i.avg_intensity_gco2_per_kwh,

    -- Completeness, so the dashboard can grey out partial days. Only
    -- correct because dim_date's settlement_periods_in_day already knows
    -- about the two clock-change days -- an unconditional "= 48" check
    -- here would wrongly flag every 46- and 50-period day as incomplete.
    p.periods_with_price = d.settlement_periods_in_day as is_price_complete

from {{ ref('dim_date') }} d
left join price_daily      p on d.date_day = p.settlement_date
left join generation_daily g on d.date_day = g.settlement_date
left join intensity_daily  i on d.date_day = i.intensity_date
where d.date_day <= current_date
  and (p.settlement_date is not null or g.settlement_date is not null or i.intensity_date is not null)
