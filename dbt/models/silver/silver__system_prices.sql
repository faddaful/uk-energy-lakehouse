-- Silver: one trustworthy row per settlement_date + settlement_period.
--
-- Adjusted from the implementation guide's original plan: bronze has no
-- settlement-run-type field to rank on. This was checked against the live
-- API before writing this model (see README) rather than assumed from the
-- guide text: the /balancing/settlement/system-prices endpoint does not
-- return a run identifier, so there is no run_rank to tiebreak on first.
-- The latest loaded_at is therefore the only, and the correct, signal
-- here: a revision on this endpoint shows up as a newer loaded_at
-- replacing an older one for the same key, not as a later, more
-- authoritative settlement run.
--
-- Same window-function idiom as silver_regional_intensity.sql in Phase 1,
-- minus the settlement-run ranking that model's comment once anticipated
-- adding here (that ranking turned out not to exist in this data).
--
-- Incremental: only the trailing window of settlement dates is
-- reprocessed on every run, not the whole of bronze history. On the first
-- run (or a full refresh) it processes everything.

{{
    config(
        materialized='incremental',
        unique_key=['settlement_date', 'settlement_period'],
        on_schema_change='sync_all_columns',
    )
}}

with source as (

    select * from {{ ref('stg_elexon_system_prices') }}

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
            partition by settlement_date, settlement_period
            order by loaded_at desc
        ) as row_num
    from source

)

select
    settlement_date,
    settlement_period,
    start_time,
    system_sell_price,
    system_buy_price,
    price_derivation_code,
    net_imbalance_volume,
    loaded_at
from ranked
where row_num = 1
