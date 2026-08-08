-- Staging: read bronze parquet, rename to clean names, cast types.
-- No business logic here. One staging model per bronze source.
--
-- The path is relative to the dbt/ folder, hence ../data/...
-- The */* glob matches every date= partition folder and every file inside.
--
-- ADJUST the column names on the left of each AS to match your actual
-- parquet schema. Open a file to check:
--   uv run python -c "import pandas as pd; print(pd.read_parquet('data/bronze/carbon_intensity/date=2026-08-01').dtypes)"
-- Note: "from" and "to" are SQL keywords, so they are quoted here and
-- renamed immediately to valid_from / valid_to.

-- Updated for CI: Use a committed fixture sample instead of real bronze data. 
-- The source path is a dbt var so it can differ between environments:
--   local  -> real bronze data (the default below)
--   CI     -> fixture sample (passed via --vars)
-- This is the one line that lets the same model run in both places.

{% set carbon_intensity_path = var(
    'carbon_intensity_path',
    '../data/bronze/carbon_intensity/*/*.parquet'
) %}

select
    strptime("from", '%Y-%m-%dT%H:%MZ')  as valid_from,
    strptime("to",   '%Y-%m-%dT%H:%MZ')  as valid_to,
    cast(region_id as integer)           as region_id,
    cast(intensity_forecast as integer)  as intensity_forecast,
    cast(intensity_actual as integer)    as intensity_actual,
    cast(loaded_at as timestamp)         as loaded_at,
    source
from read_parquet('{{ carbon_intensity_path }}')