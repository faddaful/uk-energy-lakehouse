-- Staging: read bronze parquet, rename to clean names, cast types.
-- No business logic here. One staging model per bronze source.
--
-- Unlike stg_carbon_intensity.sql, the same glob here can match many files
-- per settlement date: bronze never overwrites for Elexon (see the README
-- for why), so every past landing of the same date is still on disk, each
-- with its own loaded_at. That is deliberate and this model does not
-- collapse it; silver is where one truth per settlement_date +
-- settlement_period gets picked.
--
-- The path is relative to the dbt/ folder, hence ../data/...
--
-- ADJUST the column names on the left of each AS to match your actual
-- parquet schema. Open a file to check:
--   uv run python -c "import pandas as pd; print(pd.read_parquet('data/bronze/elexon_system_prices/date=2026-08-08').dtypes)"
--
-- Source path is a dbt var so it can differ between environments, the same
-- pattern as stg_carbon_intensity.sql:
--   local  -> real bronze data (the default below)
--   CI     -> fixture sample (passed via --vars)

{% set elexon_system_prices_path = var(
    'elexon_system_prices_path',
    '../data/bronze/elexon_system_prices/*/*.parquet'
) %}

select
    cast(settlementDate as date)         as settlement_date,
    cast(settlementPeriod as integer)    as settlement_period,
    cast(startTime as timestamp)         as start_time,
    cast(createdDateTime as timestamp)   as created_date_time,
    cast(systemSellPrice as double)      as system_sell_price,
    cast(systemBuyPrice as double)       as system_buy_price,
    priceDerivationCode                  as price_derivation_code,
    cast(netImbalanceVolume as double)   as net_imbalance_volume,
    cast(loaded_at as timestamp)         as loaded_at,
    source
from read_parquet('{{ elexon_system_prices_path }}')
