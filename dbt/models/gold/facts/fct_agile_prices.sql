-- Gold fact: one row per half hour, built on silver__agile_prices,
-- with rate_band (day/night) classified against the Economy 7 window
-- confirmed for this project's actual tariff: night is 00:30-07:30 in
-- LOCAL UK clock time, day is everything else. Getting "local" right
-- here matters: a half hour just after midnight is night in winter
-- (GMT) exactly the same as in summer (BST), but its UTC clock time
-- moves by an hour between the two, so classifying on the raw UTC
-- valid_from directly would misclassify roughly a third of the year.
--
-- Deliberately NOT done with DuckDB's AT TIME ZONE / ::timestamptz:
-- tried first, and it silently depends on the connecting session's own
-- TimeZone setting for the naive-timestamp-to-timestamptz cast (checked
-- directly, not assumed: this project's real local DuckDB session
-- defaults to TimeZone='Europe/London', not UTC, so the same SQL could
-- give a different, wrong answer under dbt's session vs. a CI runner
-- defaulting to UTC). Instead: dim_date.is_bst (already computed,
-- already tested, see that model's own comment on the clock-change
-- formula it uses) gates a plain interval add on the UTC timestamp --
-- +1 hour during BST, +0 otherwise -- which is pure arithmetic with no
-- session-dependent timezone lookup anywhere in it. Verified against
-- five real reference half hours spanning both a BST/GMT case and both
-- sides of the local midnight rollover before trusting it, not assumed
-- from reasoning about the formula alone.
--
-- One accepted approximation, stated plainly rather than hidden: the
-- join to dim_date is on valid_from's UTC calendar date, so a half hour
-- within an hour of local midnight on one of the two real clock-change
-- nights a year could pick up the wrong side's is_bst. This project's
-- own BBC-grade home electricity bill does not need clock-change-night
-- precision to the half hour; a real financial system would need to
-- handle this properly, this one does not pretend to.

{{
    config(
        materialized='incremental',
        unique_key='agile_price_fact_key',
        on_schema_change='append_new_columns',
    )
}}

with prices as (

    select * from {{ ref('silver__agile_prices') }}

    {% if is_incremental() %}
    where valid_from >= (
        select coalesce(max(half_hour_start_utc), timestamp '1900-01-01') from {{ this }}
    ) - interval 7 day
    {% endif %}

),

with_local_equivalent as (

    select
        p.*,
        d.date_key,
        p.valid_from + (case when d.is_bst then 1 else 0 end) * interval 1 hour
            as local_equivalent_start

    from prices p
    inner join {{ ref('dim_date') }} d on cast(p.valid_from as date) = d.date_day

)

select
    {{ dbt_utils.generate_surrogate_key(['valid_from']) }} as agile_price_fact_key,
    date_key,
    valid_from as half_hour_start_utc,
    valid_to   as half_hour_end_utc,
    unit_rate_exc_vat_p_per_kwh,
    unit_rate_inc_vat_p_per_kwh,
    case
        when cast(local_equivalent_start as time) >= time '00:30:00'
         and cast(local_equivalent_start as time) <  time '07:30:00'
        then 'night'
        else 'day'
    end as rate_band,
    loaded_at

from with_local_equivalent
