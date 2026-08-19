-- Staging: read bronze (a Delta table, not plain parquet, see README),
-- rename to clean names, cast types. No business logic here. One staging
-- model per bronze source.
--
-- Same append-only bronze design as stg_elexon_system_prices.sql, and the
-- same reason: the table can hold many rows per settlement date, one
-- batch per past landing (writes here use mode="append"). Resolution to
-- one row happens in silver.
--
-- The row key here is (settlement_date, settlement_period, fuel_type), not
-- just (settlement_date, settlement_period): one row is one fuel type
-- within one settlement period. See elexon_generation_by_fuel.py's module
-- docstring for the fuller explanation.
--
-- {{ bronze('elexon_generation_by_fuel') }} resolves to a delta_scan()
-- over either local disk or the Azure ADLS container, depending on the
-- dbt target (--target local|azure), see macros/bronze.sql and README.
-- On the local target it still reads its path from the
-- elexon_generation_by_fuel_path var, the same CI-fixture-override
-- mechanism as before:
--   local  -> real bronze data (the default)
--   CI     -> fixture sample (passed via --vars)
--
-- delta_scan() reads the table as of its latest committed version; the
-- delta extension that provides it is loaded on every connection via
-- profiles.yml.

-- startTime/publishTime/loaded_at are TIMESTAMPTZ in bronze (landed
-- from tz-aware pandas Timestamps), not plain TIMESTAMP: cast(col as
-- timestamp) on one of those silently converts through the connecting
-- session's own local TimeZone rather than UTC, a real bug found and
-- fixed project-wide, see macros/utc_timestamp.sql for the full story.

select
    cast(settlementDate as date)          as settlement_date,
    cast(settlementPeriod as integer)     as settlement_period,
    fuelType                              as fuel_type,
    cast(generation as double)            as generation_mw,
    {{ utc_timestamp('startTime') }}      as start_time,
    {{ utc_timestamp('publishTime') }}    as publish_time,
    {{ utc_timestamp('loaded_at') }}      as loaded_at,
    source
from {{ bronze('elexon_generation_by_fuel') }}
