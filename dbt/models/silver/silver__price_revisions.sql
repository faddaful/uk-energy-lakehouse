-- Silver: audit log of every price change ever observed in bronze for a
-- settlement_date + settlement_period. The revision observatory reads
-- from this table.
--
-- Built directly from every bronze landing (via the staging model), not
-- by diffing against silver__system_prices's previous build: bronze
-- already keeps every landing (see README for why), so the full revision
-- history can be recomputed from scratch on every run without a snapshot
-- table. Two consecutive landings for the same key, ordered by loaded_at,
-- are "a revision" whenever the price differs between them. A key's first
-- landing is never a revision of anything, since there is nothing before
-- it to compare against, so it is excluded here.
--
-- Expect this table to mostly log fast, small corrections in the day or
-- so after a settlement period (see README): this endpoint has not shown
-- evidence of the multi-month reconciliation cycle BSC settlement is
-- known for more generally. A near-empty table most weeks is the expected
-- healthy state, not a sign the pipeline is broken.

with source as (

    select * from {{ ref('stg_elexon_system_prices') }}

),

with_previous as (

    select
        settlement_date,
        settlement_period,
        system_sell_price,
        loaded_at,
        lag(system_sell_price) over (
            partition by settlement_date, settlement_period
            order by loaded_at
        ) as previous_system_sell_price,
        lag(loaded_at) over (
            partition by settlement_date, settlement_period
            order by loaded_at
        ) as previous_loaded_at
    from source

)

select
    settlement_date,
    settlement_period,
    previous_system_sell_price as old_system_sell_price,
    system_sell_price as new_system_sell_price,
    previous_loaded_at as previously_seen_at,
    loaded_at as revised_at
from with_previous
where previous_system_sell_price is not null
  and system_sell_price != previous_system_sell_price
