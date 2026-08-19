-- Staging: read bronze (a Delta table, not plain parquet, see README),
-- rename to clean names, cast types. No business logic here. One staging
-- model per bronze source.
--
-- Bronze never overwrites for Elexon (see the README for why): every past
-- landing of the same settlement date is still in the table, each with its
-- own loaded_at, because writes here use mode="append". That is deliberate
-- and this model does not collapse it; silver is where one truth per
-- settlement_date + settlement_period gets picked.
--
-- {{ bronze('elexon_system_prices') }} resolves to a delta_scan() over
-- either local disk or the Azure ADLS container, depending on the dbt
-- target (--target local|azure), see macros/bronze.sql and README. On
-- the local target it still reads its path from the
-- elexon_system_prices_path var, the same CI-fixture-override mechanism
-- as before:
--   local  -> real bronze data (the default)
--   CI     -> fixture sample (passed via --vars)
--
-- delta_scan() reads the table as of its latest committed version; the
-- delta extension that provides it is loaded on every connection via
-- profiles.yml.
--
-- ADJUST the column names on the left of each AS to match your actual
-- schema. Open the table to check:
--   uv run python -c "from deltalake import DeltaTable; print(DeltaTable('data/bronze/elexon_system_prices').to_pandas().dtypes)"
--
-- startTime/createdDateTime/loaded_at are TIMESTAMPTZ in bronze (landed
-- from tz-aware pandas Timestamps), not plain TIMESTAMP: cast(col as
-- timestamp) on one of those silently converts through the connecting
-- session's own local TimeZone rather than UTC, a real bug found and
-- fixed project-wide, see macros/utc_timestamp.sql for the full story
-- (this is the exact model whose settlement_period_start_utc it was
-- caught corrupting -- 7,378 real mismatched rows on this project's own
-- dev machine, off by an hour during BST, before this was fixed).

select
    cast(settlementDate as date)            as settlement_date,
    cast(settlementPeriod as integer)       as settlement_period,
    {{ utc_timestamp('startTime') }}        as start_time,
    {{ utc_timestamp('createdDateTime') }}  as created_date_time,
    cast(systemSellPrice as double)         as system_sell_price,
    cast(systemBuyPrice as double)          as system_buy_price,
    priceDerivationCode                     as price_derivation_code,
    cast(netImbalanceVolume as double)      as net_imbalance_volume,
    {{ utc_timestamp('loaded_at') }}        as loaded_at,
    source
from {{ bronze('elexon_system_prices') }}
