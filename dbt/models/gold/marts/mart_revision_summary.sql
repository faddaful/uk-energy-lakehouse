-- Gold mart: the revision observatory. One row per settlement month,
-- answering "how often are Elexon's published system prices wrong when
-- first published, and by how much?" in a shape ready to chart.
--
-- pct_periods_revised is the headline number this whole mart exists to
-- produce. Expect it to be small most months: this project's own README
-- documents that the system-prices endpoint has shown no evidence of
-- the multi-month BSC reconciliation cycle, only fast same-day
-- corrections, so a near-zero rate most months is the healthy state,
-- not a sign the pipeline or the revision-logging logic is broken.
--
-- Built on silver__price_revisions (prices only). This project also logs
-- generation-by-fuel revisions (silver__generation_revisions) but they
-- are not part of this mart. The headline story here is specifically
-- about prices, and generation revisions are a materially different
-- question (measurement corrections, not a financial reassessment) that
-- deserves its own mart if it turns out to be worth one, not a bolt-on.

{{ config(materialized='table') }}

with revisions as (

    select
        settlement_date,
        settlement_period,
        old_system_sell_price,
        new_system_sell_price,
        new_system_sell_price - old_system_sell_price as revision_gbp_per_mwh,
        abs(new_system_sell_price - old_system_sell_price) as abs_revision_gbp_per_mwh,
        revised_at,
        date_diff('day', settlement_date, cast(revised_at as date)) as days_to_revision
    from {{ ref('silver__price_revisions') }}

),

monthly as (

    select
        date_trunc('month', settlement_date)          as settlement_month,
        count(*)                                       as revision_count,
        count(distinct settlement_date)                as days_with_revisions,
        avg(abs_revision_gbp_per_mwh)                  as avg_abs_revision_gbp_per_mwh,
        max(abs_revision_gbp_per_mwh)                  as max_abs_revision_gbp_per_mwh,
        median(abs_revision_gbp_per_mwh)               as median_abs_revision_gbp_per_mwh,
        sum(case when revision_gbp_per_mwh > 0 then 1 else 0 end) as upward_revisions,
        sum(case when revision_gbp_per_mwh < 0 then 1 else 0 end) as downward_revisions,
        avg(days_to_revision)                          as avg_days_to_revision,
        max(days_to_revision)                          as max_days_to_revision
    from revisions
    group by 1

),

periods_in_month as (

    select
        date_trunc('month', settlement_date) as settlement_month,
        count(*)                             as total_periods
    from {{ ref('fct_settlement_period') }}
    group by 1

)

select
    m.settlement_month,
    m.revision_count,
    m.days_with_revisions,
    p.total_periods,
    round(100.0 * m.revision_count / nullif(p.total_periods, 0), 3)
        as pct_periods_revised,
    round(m.avg_abs_revision_gbp_per_mwh, 3)    as avg_abs_revision_gbp_per_mwh,
    round(m.median_abs_revision_gbp_per_mwh, 3) as median_abs_revision_gbp_per_mwh,
    round(m.max_abs_revision_gbp_per_mwh, 3)    as max_abs_revision_gbp_per_mwh,
    m.upward_revisions,
    m.downward_revisions,
    round(m.avg_days_to_revision, 1)            as avg_days_to_revision,
    m.max_days_to_revision
from monthly m
left join periods_in_month p on m.settlement_month = p.settlement_month
order by m.settlement_month
