-- Staging: read bronze (a Delta table, not plain parquet, see README),
-- rename NESO's raw CSV headers to clean snake_case names, cast types.
-- One row per project tranche, as NESO published it in the most recent
-- register landing. No business logic here beyond that filter and the
-- key below; the rest is the same rename-and-cast job every other
-- staging model does.
--
-- Filtered to max(as_of_date): bronze itself is append-only, one
-- partition per twice-weekly landing (see neso_connections.py), because
-- that is what gives this project any queue history at all. But this
-- model feeds dbt/snapshots/snapshot_connection_queue.sql, and a dbt
-- snapshot works by re-querying "the current state" each time it runs
-- and comparing it to what it saw last time, not by being handed a
-- table that already contains every past state. Handing it every
-- as_of_date landing at once would make every past landing look like
-- "the current state" simultaneously, which a snapshot's unique_key
-- logic can't make sense of. The actual landing-by-landing history
-- comes from the snapshot table itself, once dbt has run across enough
-- real landings; see that file's own comment.
--
-- connections_key: NESO's own field notes (checked against the live
-- CKAN API before writing this, not assumed) document that (Project ID,
-- Stage) is not always a unique row: an already-built project adding
-- new generation gets a second row with the same Project ID and a blank
-- Stage, until the addition is itself built and the two rows merge back
-- into one. Confirmed against a real landing (2,198 rows, 2026-08-18):
-- exactly one such pair, project "Immingham". dedup_rank breaks that tie
-- deterministically by sorting on fields that describe what the row is
-- rather than where it happened to sit in the CSV, so the same logical
-- row keeps the same key landing to landing, as long as its relative
-- order among the tie doesn't change. This is what
-- snapshot_connection_queue.sql actually snapshots on: dbt snapshot
-- needs a genuinely unique key per row, and (project_id, stage) alone
-- cannot promise that.
--
-- ADJUST the column names on the left of each AS to match your actual
-- schema. Open the table to check:
--   uv run python -c "from deltalake import DeltaTable; print(DeltaTable('data/bronze/neso_connections').to_pandas().dtypes)"

with source as (

    select
        cast("Project Name" as varchar)                        as project_name,
        cast("Customer Name" as varchar)                        as customer_name,
        cast("Connection Site" as varchar)                      as connection_site,
        cast(Stage as integer)                                  as stage,
        cast("MW Connected" as double)                          as mw_connected,
        cast("MW Increase / Decrease" as double)                as mw_increase_decrease,
        cast("Cumulative Total Capacity (MW)" as double)        as cumulative_capacity_mw,
        cast("MW Effective From" as date)                       as mw_effective_from,
        cast("Project Status" as varchar)                       as project_status,
        cast("Agreement Type" as varchar)                       as agreement_type,
        cast("HOST TO" as varchar)                              as host_to,
        cast("Plant Type" as varchar)                           as plant_type,
        cast("Project ID" as varchar)                           as project_id,
        cast("Project Number" as varchar)                       as project_number,
        cast(Gate as integer)                                   as gate,
        cast(as_of_date as date)                                as as_of_date,
        cast(loaded_at as timestamp)                            as loaded_at,
        source
    from {{ bronze('neso_connections') }}

),

latest_landing as (

    select * from source
    where as_of_date = (select max(as_of_date) from source)

),

deduped as (

    select
        *,
        row_number() over (
            partition by project_id, stage
            order by project_status, connection_site, mw_effective_from, project_number
        ) as dedup_rank
    from latest_landing

)

select
    project_id || '-' || coalesce(cast(stage as varchar), 'none') || '-' || cast(dedup_rank as varchar)
        as connections_key,
    * exclude (dedup_rank)
from deduped
