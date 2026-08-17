-- Gold fact: one row per half hour + region, built on
-- silver_regional_intensity.
--
-- Only carries what the extractor actually lands: half-hour window,
-- region, forecast intensity, and the (always-NULL-by-design) actual
-- intensity. The companion guide this project follows assumed a
-- generation-mix percentage breakdown (pct_gas, pct_coal, ... ) and an
-- intensity_index column on this fact -- checked against
-- carbon_intensity.py before writing this model, not assumed, and
-- neither is in the record the extractor builds. The Carbon Intensity
-- API's regional endpoint does return a mix breakdown and an index in
-- its raw response, but this project's extractor only ever parsed out
-- from/to/intensity.actual/intensity.forecast/region_id (see
-- fetch_carbon_intensity_data), so there is nothing further downstream
-- to carry through. Adding the mix/index fields is a bronze-and-up
-- change, not something gold can conjure from data that was never kept.
--
-- intensity_actual_gco2_per_kwh is kept, not dropped, even though it is
-- NULL for every row today: the regional Carbon Intensity endpoint is
-- forecast-only (see README's "schema gotcha" note), so this column
-- documents that fact for anyone querying gold directly rather than
-- silently omitting a field a consumer might otherwise assume exists.
--
-- 7-day lookback, not 35 like the two Elexon facts: Carbon Intensity
-- does not revise the way Elexon does (its bronze extractor overwrites
-- each data_date in place, with no append-and-resolve history to catch
-- up on -- see README's "Why bronze never overwrites for Elexon").
-- Nothing here needs to be revisited once landed; the lookback exists
-- for incremental build efficiency, not to catch late revisions.

{{
    config(
        materialized='incremental',
        unique_key='regional_intensity_fact_key',
        on_schema_change='append_new_columns',
    )
}}

with intensity as (

    select * from {{ ref('silver_regional_intensity') }}

    {% if is_incremental() %}
    where valid_from >= (
        select coalesce(max(half_hour_start_utc), timestamp '1900-01-01') from {{ this }}
    ) - interval 7 day
    {% endif %}

)

select
    {{ dbt_utils.generate_surrogate_key(['i.valid_from', 'i.region_id']) }}
        as regional_intensity_fact_key,

    d.date_key,
    r.region_key,

    i.valid_from as half_hour_start_utc,
    cast(i.valid_from as date) as intensity_date,
    i.region_id,

    -- Forecast only: the regional endpoint does not publish actuals.
    i.intensity_forecast as intensity_forecast_gco2_per_kwh,
    i.intensity_actual   as intensity_actual_gco2_per_kwh,

    i.loaded_at

from intensity i
inner join {{ ref('dim_date') }}   d on cast(i.valid_from as date) = d.date_day
inner join {{ ref('dim_region') }} r on i.region_id = r.region_id
