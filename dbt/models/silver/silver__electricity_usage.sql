-- Silver: a straightforward pass-through, deliberately not the usual
-- row_number()-dedup idiom every other silver model in this project
-- uses. Bronze here is a full-table overwrite of whatever the CSV
-- currently says (see manual_usage.py), not an append-only or
-- partition-scoped landing: there is only ever one loaded_at value in
-- the whole table at any moment, so "latest loaded_at wins" can never
-- pick between two different landings of the same period the way it
-- does for the API-sourced silvers. Genuinely no dedup question to
-- answer here.
--
-- A duplicate or overlapping period in the CSV is a real data-entry
-- mistake worth surfacing loudly, not silently resolving by picking a
-- winner: dbt_utils.unique_combination_of_columns on
-- (period_start, period_end) in _silver.yml is what actually catches
-- that, by failing the build, rather than this model quietly deciding
-- which of two rows for the same period to keep.

select
    period_start,
    period_end,
    day_kwh,
    night_kwh,
    estimated_cost_gbp,
    loaded_at
from {{ ref('stg_electricity_usage') }}
