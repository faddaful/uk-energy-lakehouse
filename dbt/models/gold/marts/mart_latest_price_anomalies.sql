-- Gold mart: settlement periods in the last 30 days whose system sell
-- price was itself negative, or swung by a large amount from the
-- period immediately before it. Exported as latest_price_anomalies.json
-- (see lakehouse.products.api_export).
--
-- This is the same idea apps/streamlit/dashboard.py's "Recent price
-- events" tab already computes (lag() over settlement_period_start_utc,
-- the same NOTABLE_SWING_GBP_PER_MWH = 50 threshold), promoted to a real
-- tested gold model rather than left as SQL embedded in a Streamlit
-- callback. The two are not wired together: the dashboard still
-- computes its own version, deliberately left alone rather than
-- refactored to read this mart as part of building an unrelated data
-- product, since that is a real change to a working, already-verified
-- view for no benefit to this piece. If the two ever disagree, this
-- comment is the reminder that NOTABLE_SWING_GBP_PER_MWH is duplicated,
-- not derived, and both copies need updating together.
--
-- price_change_gbp_per_mwh is computed over ALL settled periods, not
-- just the last 30 days: lag() needs the real previous period to be
-- correct at the boundary, and windowing the input first would make the
-- oldest row in any trailing slice look like it has no previous value
-- when it actually does. The 30-day recency filter is applied only
-- after the window function has already seen full history.
--
-- generated_at, same reasoning as mart_greenest_hours_next_48h.

{{ config(materialized='table') }}

{% set notable_swing_gbp_per_mwh = 50 %}

with settled as (

    select
        settlement_period_start_utc,
        system_sell_price_gbp_per_mwh
    from {{ ref('fct_settlement_period') }}
    where system_sell_price_gbp_per_mwh is not null

),

with_change as (

    select
        settlement_period_start_utc,
        system_sell_price_gbp_per_mwh,
        system_sell_price_gbp_per_mwh
            - lag(system_sell_price_gbp_per_mwh) over (order by settlement_period_start_utc)
            as price_change_gbp_per_mwh
    from settled

),

classified as (

    select
        settlement_period_start_utc,
        system_sell_price_gbp_per_mwh,
        price_change_gbp_per_mwh,
        case
            when system_sell_price_gbp_per_mwh < 0 then 'negative_price'
            when abs(price_change_gbp_per_mwh) >= {{ notable_swing_gbp_per_mwh }} then 'large_swing'
        end as anomaly_type
    from with_change

)

select
    settlement_period_start_utc,
    system_sell_price_gbp_per_mwh,
    price_change_gbp_per_mwh,
    anomaly_type,
    current_timestamp as generated_at
from classified
where anomaly_type is not null
  and settlement_period_start_utc >= now() - interval 30 day
order by settlement_period_start_utc desc
