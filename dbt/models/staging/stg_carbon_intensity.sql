-- Staging: read bronze (a Delta table, not plain parquet, see README),
-- rename to clean names, cast types. No business logic here. One staging
-- model per bronze source.
--
-- {{ bronze('carbon_intensity') }} resolves to a delta_scan() over either
-- local disk or the Azure ADLS container, depending on the dbt target
-- (--target local|azure), see macros/bronze.sql and README. On the
-- local target it still reads its path from the carbon_intensity_path
-- var, the same CI-fixture-override mechanism as before:
--   local  -> real bronze data (the default)
--   CI     -> fixture sample (passed via --vars)
--
-- delta_scan() reads the table as of its latest committed version; the
-- delta extension that provides it is loaded on every connection via
-- profiles.yml.
--
-- ADJUST the column names on the left of each AS to match your actual
-- schema. Open the table to check:
--   uv run python -c "from deltalake import DeltaTable; print(DeltaTable('data/bronze/carbon_intensity').to_pandas().dtypes)"
-- Note: "from" and "to" are SQL keywords, so they are quoted here and
-- renamed immediately to valid_from / valid_to.

select
    strptime("from", '%Y-%m-%dT%H:%MZ')  as valid_from,
    strptime("to",   '%Y-%m-%dT%H:%MZ')  as valid_to,
    cast(region_id as integer)           as region_id,
    cast(intensity_forecast as integer)  as intensity_forecast,
    cast(intensity_actual as integer)    as intensity_actual,
    intensity_index,
    cast(loaded_at as timestamp)         as loaded_at,
    source
from {{ bronze('carbon_intensity') }}
