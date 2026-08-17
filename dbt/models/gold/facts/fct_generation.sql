-- Gold fact: one row per settlement_date + settlement_period +
-- fuel_type, with each fuel's share of the half-hourly mix, built on
-- silver__generation_by_fuel.
--
-- Interconnector semantics, decided explicitly rather than left implicit:
-- total_generation_mw (the share_of_mix_pct denominator) INCLUDES
-- interconnectors. This is net-mix semantics, not "domestic generation
-- only". The alternative -- excluding interconnectors from the
-- denominator -- was considered and rejected: it would make
-- share_of_mix_pct answer "what powered GB, including imports we cannot
-- attribute a carbon figure to" while still calling the column a share
-- of "the mix", which is a more misleading number than the one chosen.
-- The consequence of this choice, checked against 90 days of this
-- project's real bronze data (2026-05-18 to 2026-08-16, 4,352 settlement
-- periods) before picking a test tolerance for it: domestic (non-
-- interconnector) fuels' combined share ranged from 0% to ~144% of the
-- net-mix total, median ~82%, with the great majority (89%) between 60%
-- and 101%. It goes over 100% when GB is a net exporter (interconnector
-- generation_mw negative, shrinking the denominator below domestic
-- generation alone) and under 100% when GB is a net importer. It hit
-- exactly 0% for two consecutive periods on 2026-07-07, when every
-- domestic fuel type was reported as literal 0 while interconnector
-- flows were already populated -- a genuine late-publish gap in Elexon's
-- feed, not a bug in this model. See
-- tests/assert_mix_shares_within_expected_range.sql for how this
-- tolerance is actually enforced.
--
-- Division is guarded (case ... when total > 0): a settlement period
-- with a zero or missing total does occur in raw feeds, and a divide-by-
-- zero failure at 3am in a scheduled Dagster run is a bad way to find
-- that out. See the share_of_mix_pct doc block (models/gold/docs.md) for
-- the full net-mix explanation.

{{
    config(
        materialized='incremental',
        unique_key='generation_fact_key',
        on_schema_change='append_new_columns',
    )
}}

with generation as (

    select * from {{ ref('silver__generation_by_fuel') }}

    -- Same 35-day lookback as fct_settlement_period, matching
    -- silver__generation_by_fuel's own reprocessing window -- see that
    -- model's comment and fct_settlement_period.sql for why.
    {% if is_incremental() %}
    where settlement_date >= (
        select coalesce(max(settlement_date), date '1900-01-01') from {{ this }}
    ) - interval 35 day
    {% endif %}

),

with_totals as (

    select
        g.*,
        sum(g.generation_mw) over (
            partition by g.settlement_date, g.settlement_period
        ) as total_generation_mw
    from generation g

)

select
    {{ dbt_utils.generate_surrogate_key([
        'g.settlement_date', 'g.settlement_period', 'g.fuel_type'
    ]) }} as generation_fact_key,

    d.date_key,
    sp.settlement_period_key,
    f.fuel_type_key,

    g.settlement_date,
    g.settlement_period,
    g.fuel_type as fuel_type_code,
    g.start_time as settlement_period_start_utc,

    g.generation_mw,
    g.total_generation_mw,
    case
        when g.total_generation_mw > 0
        then round(100.0 * g.generation_mw / g.total_generation_mw, 4)
    end as share_of_mix_pct,

    g.loaded_at

from with_totals g
inner join {{ ref('dim_date') }}              d  on g.settlement_date = d.date_day
inner join {{ ref('dim_settlement_period') }} sp on g.settlement_period = sp.settlement_period
inner join {{ ref('dim_fuel_type') }}         f  on g.fuel_type = f.fuel_type_code
