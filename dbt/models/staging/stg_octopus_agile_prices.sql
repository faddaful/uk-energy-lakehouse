-- Staging: read bronze (a Delta table, not plain parquet, see README),
-- rename to clean names, cast types. No business logic here.
--
-- {{ bronze('octopus_agile_prices') }} resolves to a delta_scan() over
-- either local disk or the Azure ADLS container, depending on the dbt
-- target, see macros/bronze.sql and README. On the local target it
-- still reads its path from the octopus_agile_prices_path var, the
-- same CI-fixture-override mechanism as every other source.
--
-- ADJUST the column names on the left of each AS to match your actual
-- schema. Open the table to check:
--   uv run python -c "from deltalake import DeltaTable; print(DeltaTable('data/bronze/octopus_agile_prices').to_pandas().dtypes)"
--
-- valid_from/valid_to/loaded_at are TIMESTAMPTZ in bronze (landed from
-- tz-aware pandas Timestamps): cast(col as timestamp) on one of those
-- silently converts through the connecting session's own local
-- TimeZone rather than UTC, a real bug found and fixed project-wide
-- while building this exact model, see macros/utc_timestamp.sql for
-- the full story.

select
    {{ utc_timestamp('valid_from') }}  as valid_from,
    {{ utc_timestamp('valid_to') }}    as valid_to,
    cast(value_exc_vat as double)      as unit_rate_exc_vat_p_per_kwh,
    cast(value_inc_vat as double)      as unit_rate_inc_vat_p_per_kwh,
    {{ utc_timestamp('loaded_at') }}   as loaded_at,
    source
from {{ bronze('octopus_agile_prices') }}
