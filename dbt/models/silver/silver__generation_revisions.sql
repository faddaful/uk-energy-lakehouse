-- Silver: audit log of every generation-by-fuel value change ever observed
-- in bronze for a settlement_date + settlement_period + fuel_type.
--
-- Same design as silver__price_revisions.sql: built directly from every
-- bronze landing via the staging model, using lag() rather than a
-- stateful diff against silver__generation_by_fuel's previous build, so
-- the full revision history can be recomputed from scratch on every run.
-- A key's first landing is never a revision of anything and is excluded.
--
-- Expect this table to be sparse or empty most weeks, for the same reason
-- as silver__price_revisions.sql (see README): no evidence this endpoint
-- reflects the multi-month settlement reconciliation cycle either.

with source as (

    select * from {{ ref('stg_elexon_generation_by_fuel') }}

),

with_previous as (

    select
        settlement_date,
        settlement_period,
        fuel_type,
        generation_mw,
        loaded_at,
        lag(generation_mw) over (
            partition by settlement_date, settlement_period, fuel_type
            order by loaded_at
        ) as previous_generation_mw,
        lag(loaded_at) over (
            partition by settlement_date, settlement_period, fuel_type
            order by loaded_at
        ) as previous_loaded_at
    from source

)

select
    settlement_date,
    settlement_period,
    fuel_type,
    previous_generation_mw as old_generation_mw,
    generation_mw as new_generation_mw,
    previous_loaded_at as previously_seen_at,
    loaded_at as revised_at
from with_previous
where previous_generation_mw is not null
  and generation_mw != previous_generation_mw
