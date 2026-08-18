-- Gold mart: the connections queue observatory. One row per as_of_month,
-- answering "how big is the connections queue, what's it made of, and
-- how much is NESO's own published connection date slipping?" in a
-- shape ready to chart.
--
-- Built on fct_connection_queue, itself sourced from
-- snapshot_connection_queue's row-level SCD history (see that model's
-- comment for why a real dbt snapshot, not a recomputed-from-bronze
-- window function like the Elexon revision marts use, is the right tool
-- for this source).
--
-- connection_date_slippage_days: for the same connections_key, how many
-- days did mw_effective_from move between this month and the previous
-- one on the spine. lag() naturally returns NULL for a project's first
-- observed month, correctly excluding it from slippage rather than
-- comparing it to nothing. A project whose mw_effective_from is NULL in
-- either month contributes no slippage figure that month either, not a
-- NULL-arithmetic zero. Positive means the date moved later, a delay;
-- negative means it moved earlier.

{{ config(materialized='table') }}

with queue_by_month as (

    select
        f.as_of_month,
        f.connections_key,
        f.project_status,
        f.cumulative_capacity_mw,
        f.mw_effective_from,
        t.technology_category,
        t.is_generation,
        lag(f.mw_effective_from) over (
            partition by f.connections_key order by f.as_of_month
        ) as previous_mw_effective_from
    from {{ ref('fct_connection_queue') }} f
    left join {{ ref('dim_technology') }} t using (technology_key)

),

with_slippage as (

    select
        *,
        case
            when mw_effective_from is not null and previous_mw_effective_from is not null
            then date_diff('day', previous_mw_effective_from, mw_effective_from)
        end as connection_date_slippage_days
    from queue_by_month

),

monthly as (

    select
        as_of_month,
        count(*)                                                          as projects_in_queue,
        sum(cumulative_capacity_mw)                                       as total_capacity_mw,
        sum(case when is_generation then cumulative_capacity_mw end)      as generation_capacity_mw,
        sum(case when technology_category = 'Renewable' then cumulative_capacity_mw end) as renewable_capacity_mw,
        sum(case when technology_category = 'Storage' then cumulative_capacity_mw end)   as storage_capacity_mw,
        sum(case when technology_category = 'Fossil' then cumulative_capacity_mw end)    as fossil_capacity_mw,
        sum(case when project_status = 'Built' then cumulative_capacity_mw end)          as built_capacity_mw,
        count(*) filter (where connection_date_slippage_days is not null) as projects_with_known_slippage,
        avg(connection_date_slippage_days)                                as avg_connection_date_slippage_days,
        median(connection_date_slippage_days)                             as median_connection_date_slippage_days,
        count(*) filter (where connection_date_slippage_days > 0)         as projects_delayed,
        count(*) filter (where connection_date_slippage_days < 0)         as projects_accelerated
    from with_slippage
    group by 1

)

select
    as_of_month,
    projects_in_queue,
    total_capacity_mw,
    generation_capacity_mw,
    renewable_capacity_mw,
    storage_capacity_mw,
    fossil_capacity_mw,
    built_capacity_mw,
    case
        when generation_capacity_mw > 0
        then round(100.0 * renewable_capacity_mw / generation_capacity_mw, 2)
    end as renewable_share_of_generation_pct,
    projects_with_known_slippage,
    round(avg_connection_date_slippage_days, 1) as avg_connection_date_slippage_days,
    median_connection_date_slippage_days,
    projects_delayed,
    projects_accelerated
from monthly
order by as_of_month
