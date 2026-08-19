-- Singular test. Passes if this query returns zero rows.
--
-- Every "_utc"-suffixed timestamp in gold must equal bronze's own
-- TIMESTAMPTZ value forced through UTC explicitly (timezone('UTC', ...)),
-- never a bare cast(col as timestamp), which silently converts through
-- whichever timezone the connecting session happens to default to. See
-- macros/utc_timestamp.sql for the real, reproduced bug this guards
-- against: 7,378 real settlement_period_start_utc rows in this
-- project's own gold were silently off by an hour during BST before it
-- was fixed, invisible in CI (whose runners default to UTC, exactly
-- where the bug cannot reproduce) and only caught by accident on a real
-- dev machine. Every dbt test that already existed for
-- fct_settlement_period passed the whole time; none of them checked
-- absolute correctness against bronze, only internal consistency. This
-- one does.
--
-- Covers every source that lands a genuinely tz-aware timestamp in
-- bronze (see the macro's own comment for which those are): both
-- Elexon facts, and the Agile price fact this project's "money story"
-- piece added.

with elexon_prices_mismatches as (

    select
        f.settlement_period_start_utc as gold_value,
        timezone('UTC', b.startTime) as bronze_value
    from {{ ref('fct_settlement_period') }} f
    inner join {{ bronze('elexon_system_prices') }} b
        on cast(b.settlementDate as date) = f.settlement_date
       and cast(b.settlementPeriod as integer) = f.settlement_period
    where f.settlement_period_start_utc != timezone('UTC', b.startTime)

),

elexon_generation_mismatches as (

    -- distinct: bronze has one row per (settlement_date,
    -- settlement_period, fuel_type), all sharing the same startTime, so
    -- joining fct_generation to bronze on just the settlement key alone
    -- fans out across every fuel type for no benefit here -- every one
    -- of those rows carries an identical startTime to check against.
    select distinct
        f.settlement_period_start_utc as gold_value,
        timezone('UTC', b.startTime) as bronze_value
    from {{ ref('fct_generation') }} f
    inner join {{ bronze('elexon_generation_by_fuel') }} b
        on cast(b.settlementDate as date) = f.settlement_date
       and cast(b.settlementPeriod as integer) = f.settlement_period
    where f.settlement_period_start_utc != timezone('UTC', b.startTime)

),

-- Agile prices have no natural key independent of the timestamp itself
-- (one row IS one half hour), so this can't be written as "join on a
-- different key, then compare the timestamp" the way the two Elexon
-- checks above are: joining on the timestamp itself, then comparing it
-- to itself, can never fail regardless of whether gold is right or
-- wrong (a real mistake caught rewriting this test before it shipped,
-- not after). Instead: every real half hour bronze actually has
-- (forced through UTC explicitly) must have a matching row in gold.
-- This is exactly the anti-join that caught the original bug by hand:
-- a naive, session-dependent cast in gold shifts every timestamp, so
-- bronze's real half hours stop matching anything in gold at all.
agile_prices_missing_from_gold as (

    select
        cast(null as timestamp) as gold_value,
        timezone('UTC', b.valid_from) as bronze_value
    from {{ bronze('octopus_agile_prices') }} b
    where not exists (
        select 1 from {{ ref('fct_agile_prices') }} f
        where f.half_hour_start_utc = timezone('UTC', b.valid_from)
    )

)

select * from elexon_prices_mismatches
union all
select * from elexon_generation_mismatches
union all
select * from agile_prices_missing_from_gold
