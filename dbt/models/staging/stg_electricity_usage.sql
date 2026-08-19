-- Staging: read bronze (a Delta table, not plain parquet, see README),
-- rename to clean names, cast types. No business logic here.
--
-- {{ bronze('electricity_usage') }} resolves to a delta_scan() over
-- either local disk or the Azure ADLS container, depending on the dbt
-- target, see macros/bronze.sql and README. On the local target it
-- still reads its path from the electricity_usage_path var, the same
-- CI-fixture-override mechanism as every other source: the fixture
-- here is a small synthetic CSV's worth of rows, never real personal
-- usage figures, see tests/fixtures/electricity_usage's own note.

-- loaded_at is TIMESTAMPTZ in bronze: cast(col as timestamp) would
-- silently convert through the session's local TimeZone rather than
-- UTC, a real bug found and fixed project-wide, see
-- macros/utc_timestamp.sql.

select
    cast(period_start as date)         as period_start,
    cast(period_end as date)           as period_end,
    cast(day_kwh as double)            as day_kwh,
    cast(night_kwh as double)          as night_kwh,
    cast(estimated_cost_gbp as double) as estimated_cost_gbp,
    {{ utc_timestamp('loaded_at') }}   as loaded_at,
    source
from {{ bronze('electricity_usage') }}
