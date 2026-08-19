-- Gold mart: the money story. One row per hand-logged usage period,
-- answering "would Agile have saved me money on the electricity I
-- actually used?" against what the supplier's own app already says you
-- paid (estimated_cost_gbp, standing charge and VAT excluded, see
-- silver__electricity_usage's own comment for why that number, not a
-- reconstruction of the current tariff's rates, is the fairer baseline).
--
-- No half-hourly consumption exists to join against (see README's "The
-- money story" for why): the join here is period-level, not half-hour-
-- level. Every real Agile half hour whose UTC calendar date falls
-- within [period_start, period_end] is averaged per rate_band (day or
-- night), and that average rate is what day_kwh/night_kwh get
-- multiplied against. This is a genuine approximation, not hidden: it
-- assumes your day-band usage was spread evenly across the period's day
-- half hours (and the same for night), which is the best a monthly
-- day/night split can support without real half-hourly data, but is
-- not the same claim as "every half hour's actual usage times its
-- actual price," which only a real smart-meter export could answer.
--
-- day_half_hours_available/night_half_hours_available/is_complete let a
-- consumer (the Streamlit tab) see when Agile price history genuinely
-- doesn't cover the whole period yet (a period logged before Agile
-- prices were backfilled that far back) rather than silently showing a
-- number computed from partial data as if it were the full picture.
--
-- generated_at, same reasoning as every other mart here: a number this
-- mart produces without a visible build time is a number nobody can
-- tell is stale.

{{ config(materialized='table') }}

with usage as (

    select * from {{ ref('silver__electricity_usage') }}

),

agile_by_period_and_band as (

    select
        u.period_start,
        u.period_end,
        p.rate_band,
        avg(p.unit_rate_inc_vat_p_per_kwh) as avg_rate_p_per_kwh,
        count(*) as half_hours_available
    from usage u
    inner join {{ ref('fct_agile_prices') }} p
        on cast(p.half_hour_start_utc as date) >= u.period_start
       and cast(p.half_hour_start_utc as date) <= u.period_end
    group by u.period_start, u.period_end, p.rate_band

),

pivoted as (

    select
        period_start,
        period_end,
        max(case when rate_band = 'day'   then avg_rate_p_per_kwh   end) as day_avg_rate_p_per_kwh,
        max(case when rate_band = 'night' then avg_rate_p_per_kwh   end) as night_avg_rate_p_per_kwh,
        coalesce(max(case when rate_band = 'day'   then half_hours_available end), 0) as day_half_hours_available,
        coalesce(max(case when rate_band = 'night' then half_hours_available end), 0) as night_half_hours_available
    from agile_by_period_and_band
    group by period_start, period_end

)

select
    u.period_start,
    u.period_end,
    u.day_kwh,
    u.night_kwh,
    u.day_kwh + u.night_kwh as total_kwh,
    u.estimated_cost_gbp as actual_estimated_cost_gbp,

    pv.day_avg_rate_p_per_kwh,
    pv.night_avg_rate_p_per_kwh,

    round(
        (u.day_kwh * pv.day_avg_rate_p_per_kwh + u.night_kwh * pv.night_avg_rate_p_per_kwh) / 100,
        2
    ) as agile_equivalent_cost_gbp,

    -- Positive: Agile would have cost less than what you actually paid.
    -- Negative: Agile would have cost more.
    round(
        u.estimated_cost_gbp
            - (u.day_kwh * pv.day_avg_rate_p_per_kwh + u.night_kwh * pv.night_avg_rate_p_per_kwh) / 100,
        2
    ) as agile_savings_gbp,

    pv.day_half_hours_available,
    pv.night_half_hours_available,
    (pv.day_half_hours_available + pv.night_half_hours_available)
        = (date_diff('day', u.period_start, u.period_end) + 1) * 48
        as is_complete,

    current_timestamp as generated_at

from usage u
left join pivoted pv on u.period_start = pv.period_start and u.period_end = pv.period_end
order by u.period_start
