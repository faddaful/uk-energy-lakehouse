-- Gold fact: a periodic snapshot, one row per project tranche
-- (connections_key) per as_of_month, built from
-- snapshot_connection_queue's row-level SCD history, not from
-- stg_neso_connections directly (which only ever shows the latest
-- landing, see that model's comment).
--
-- month_spine is bounded to [the month of the earliest observed
-- dbt_valid_from, the current month]: widening it further back would put
-- months on the spine no version of the snapshot has ever actually
-- described. It grows automatically as more twice-weekly landings
-- accumulate real history behind it; there is nothing to backfill by
-- hand.
--
-- "The state as of month M" is whichever snapshot version was open
-- through the end of M: dbt_valid_from before the following month
-- starts, and either still open (dbt_valid_to is null) or open at least
-- until the following month starts. A project that first appears
-- mid-month still gets an M row, showing its state by month-end. A
-- project invalidated (see snapshot_connection_queue.sql's
-- invalidate_hard_deletes) before month-end simply gets no M row at
-- all, which is the correct "not in the queue that month" answer, not a
-- NULL to filter around later.
--
-- primary_technology: NESO's own Plant Type is semicolon-delimited and
-- multi-value ("Energy Storage System;Wind Onshore"), and NESO's own
-- field notes admit capacity isn't split by technology within a row
-- (see stg_neso_connections.sql). Attributing the whole row's MW to
-- every listed technology would double-count capacity in any
-- technology-mix total; attributing it to only the first-listed one is
-- the honest choice given what the source actually provides, not a
-- precise per-technology split. mart_queue_evolution's technology mix is
-- built on this column, not a naive unnest of the raw list.

{{ config(materialized='table') }}

with bounds as (

    select
        date_trunc('month', min(dbt_valid_from)) as min_month,
        date_trunc('month', current_date)        as max_month
    from {{ ref('snapshot_connection_queue') }}

),

month_spine as (

    select unnest(generate_series(min_month, max_month, interval 1 month)) as as_of_month
    from bounds

),

queue_state_by_month as (

    select
        m.as_of_month,
        s.*,
        split_part(s.plant_type, ';', 1) as primary_technology
    from month_spine m
    inner join {{ ref('snapshot_connection_queue') }} s
        on s.dbt_valid_from < m.as_of_month + interval 1 month
       and (s.dbt_valid_to is null or s.dbt_valid_to >= m.as_of_month + interval 1 month)

)

select
    {{ dbt_utils.generate_surrogate_key(['q.connections_key', 'q.as_of_month']) }} as connection_queue_fact_key,

    dt.date_key                as as_of_month_date_key,
    tech.technology_key,

    q.as_of_month,
    q.connections_key,
    q.project_id,
    q.project_name,
    q.customer_name,
    q.connection_site,
    q.stage,
    q.host_to,
    q.agreement_type,
    q.plant_type,
    q.primary_technology,
    q.project_status,
    q.mw_connected,
    q.mw_increase_decrease,
    q.cumulative_capacity_mw,
    q.mw_effective_from,
    q.gate,
    q.dbt_valid_from                as version_valid_from,
    q.dbt_valid_to                  as version_valid_to

from queue_state_by_month q
inner join {{ ref('dim_date') }}       dt   on q.as_of_month = dt.date_day
left join  {{ ref('dim_technology') }} tech on q.primary_technology = tech.technology
