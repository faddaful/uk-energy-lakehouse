-- Silver: one trustworthy row per settlement_date + settlement_period +
-- fuel_type.
--
-- Same design as silver__system_prices.sql, same reason: bronze has no
-- settlement-run-type field (checked against the live FUELHH endpoint
-- directly, not assumed, see README), so the latest loaded_at is the
-- only, and the correct, signal to resolve on. The one difference from
-- silver__system_prices.sql is the grain: this dataset's row key includes
-- fuel_type, since one row here is one fuel type within one settlement
-- period, not the whole period.
--
-- Incremental: only the trailing window of settlement dates is
-- reprocessed on every run, not the whole of bronze history.

{{
    config(
        materialized='incremental',
        unique_key=['settlement_date', 'settlement_period', 'fuel_type'],
        on_schema_change='sync_all_columns',
    )
}}

with source as (

    select * from {{ ref('stg_elexon_generation_by_fuel') }}

    {% if is_incremental() %}
    -- Reprocess the trailing revision window on every run, not just rows
    -- newer than the last run: a revision re-lands an OLD settlement_date
    -- with a NEW loaded_at, so "only new rows" would silently miss it.
    -- 35 days gives a small buffer over the Dagster asset's 28-day sweep.
    where settlement_date >= (
        select coalesce(max(settlement_date), date '1900-01-01') from {{ this }}
    ) - interval 35 day
    {% endif %}

),

ranked as (

    select
        *,
        row_number() over (
            partition by settlement_date, settlement_period, fuel_type
            order by loaded_at desc
        ) as row_num
    from source

)

select
    settlement_date,
    settlement_period,
    fuel_type,
    generation_mw,
    start_time,
    loaded_at
from ranked
where row_num = 1
