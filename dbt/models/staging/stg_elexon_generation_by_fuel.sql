-- Staging: read bronze parquet, rename to clean names, cast types.
-- No business logic here. One staging model per bronze source.
--
-- Same append-only bronze design as stg_elexon_system_prices.sql, and the
-- same reason: the glob here can match many files per settlement date,
-- one per past landing. Resolution to one row happens in silver.
--
-- The row key here is (settlement_date, settlement_period, fuel_type), not
-- just (settlement_date, settlement_period): one row is one fuel type
-- within one settlement period. See elexon_generation_by_fuel.py's module
-- docstring for the fuller explanation.
--
-- The path is relative to the dbt/ folder, hence ../data/...
--
-- Source path is a dbt var so it can differ between environments, the same
-- pattern as the other staging models:
--   local  -> real bronze data (the default below)
--   CI     -> fixture sample (passed via --vars)

{% set elexon_generation_by_fuel_path = var(
    'elexon_generation_by_fuel_path',
    '../data/bronze/elexon_generation_by_fuel/*/*.parquet'
) %}

select
    cast(settlementDate as date)       as settlement_date,
    cast(settlementPeriod as integer)  as settlement_period,
    fuelType                           as fuel_type,
    cast(generation as double)         as generation_mw,
    cast(startTime as timestamp)       as start_time,
    cast(publishTime as timestamp)     as publish_time,
    cast(loaded_at as timestamp)       as loaded_at,
    source
from read_parquet('{{ elexon_generation_by_fuel_path }}')
