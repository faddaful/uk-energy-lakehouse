-- Gold fact: one row per settlement_date + settlement_period, built on
-- silver__system_prices.
--
-- The 35-day incremental lookback matches silver__system_prices's own
-- reprocessing window exactly (see that model's comment), which is
-- itself already wider than the Dagster job's 28-day revision
-- re-download sweep. It has to be at least that wide: if this window
-- were narrower than silver's, a revision could land in silver on a run
-- that falls outside gold's lookback and would then never propagate
-- here. Late-arriving data and incremental models is exactly the kind of
-- thing that is easy to get quietly wrong, so the two numbers matching
-- is deliberate, not a coincidence to be tidied away.
--
-- settlement_period_start_utc is Elexon's own startTime field, carried
-- through unchanged since staging -- it is a real UTC timestamp Elexon
-- publishes, not something derived here from the settlement period
-- number and a clock-change formula the way an earlier version of this
-- plan assumed it would have to be. See the settlement_period_start_utc
-- doc block (models/gold/docs.md) for the full explanation.
--
-- inner join to both dimensions on purpose, not left join: a settlement
-- date or period with no matching dimension row should drop the row and
-- fail dbt_utils.unique_combination_of_columns loudly (silver row count
-- vs gold row count would diverge), rather than silently carry a NULL
-- foreign key into the fact table.

{{
    config(
        materialized='incremental',
        unique_key='settlement_period_fact_key',
        on_schema_change='append_new_columns',
    )
}}

with prices as (

    select * from {{ ref('silver__system_prices') }}

    {% if is_incremental() %}
    where settlement_date >= (
        select coalesce(max(settlement_date), date '1900-01-01') from {{ this }}
    ) - interval 35 day
    {% endif %}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['p.settlement_date', 'p.settlement_period']) }}
            as settlement_period_fact_key,

        -- Foreign keys
        d.date_key,
        sp.settlement_period_key,

        -- Degenerate dimensions, kept for human-readable querying
        p.settlement_date,
        p.settlement_period,

        -- The authoritative timestamp -- see model comment above
        p.start_time as settlement_period_start_utc,

        -- Measures
        p.system_sell_price as system_sell_price_gbp_per_mwh,
        p.system_buy_price  as system_buy_price_gbp_per_mwh,
        p.net_imbalance_volume as net_imbalance_volume_mwh,

        -- Provenance
        p.price_derivation_code,
        p.loaded_at

    from prices p
    inner join {{ ref('dim_date') }} d
        on p.settlement_date = d.date_day
    inner join {{ ref('dim_settlement_period') }} sp
        on p.settlement_period = sp.settlement_period

)

select * from final
