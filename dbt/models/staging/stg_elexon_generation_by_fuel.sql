-- Staging: read bronze (a Delta table, not plain parquet -- see README),
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
-- dbt target (--target local|azure) -- see macros/bronze.sql and README.
-- On the local target it still reads its path from the
-- elexon_generation_by_fuel_path var, the same CI-fixture-override
-- mechanism as before:
--   local  -> real bronze data (the default)
--   CI     -> fixture sample (passed via --vars)
--
-- delta_scan() reads the table as of its latest committed version; the
-- delta extension that provides it is loaded on every connection via
-- profiles.yml.

select
    cast(settlementDate as date)       as settlement_date,
    cast(settlementPeriod as integer)  as settlement_period,
    fuelType                           as fuel_type,
    cast(generation as double)         as generation_mw,
    cast(startTime as timestamp)       as start_time,
    cast(publishTime as timestamp)     as publish_time,
    cast(loaded_at as timestamp)       as loaded_at,
    source
from {{ bronze('elexon_generation_by_fuel') }}
