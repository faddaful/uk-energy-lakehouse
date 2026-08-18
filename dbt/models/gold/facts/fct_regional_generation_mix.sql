-- Gold fact: one row per half hour + region + fuel, built on
-- silver__carbon_intensity_regional_mix.
--
-- This is the genuinely regional fuel mix fct_regional_intensity's own
-- comment says gold does not have: the Carbon Intensity API's regional
-- endpoint returns a generationmix breakdown alongside the intensity
-- forecast, and this fact finally keeps it. Deliberately not merged into
-- fct_generation: that fact is Elexon's transmission-metered, GB-wide mix
-- at settlement-period grain across 20 FUELHH codes; this one is a
-- forecast-based, region-specific mix across the Carbon Intensity API's
-- own coarser 9-fuel taxonomy (see seed_ci_fuel_type), and it carries a
-- percentage only: the API does not publish MW for this breakdown, so
-- there is no generation_mw column here the way fct_generation has one.
-- Joining the two mixes as if they were the same measure on two grains
-- would silently compare numbers that do not mean the same thing.
--
-- 7-day lookback, matching fct_regional_intensity exactly, for the same
-- reason: Carbon Intensity does not revise the way Elexon does, so
-- nothing here needs to be revisited once landed. The lookback is for
-- incremental build efficiency only.

{{
    config(
        materialized='incremental',
        unique_key='regional_generation_mix_fact_key',
        on_schema_change='append_new_columns',
    )
}}

with mix as (

    select * from {{ ref('silver__carbon_intensity_regional_mix') }}

    {% if is_incremental() %}
    where valid_from >= (
        select coalesce(max(half_hour_start_utc), timestamp '1900-01-01') from {{ this }}
    ) - interval 7 day
    {% endif %}

)

select
    {{ dbt_utils.generate_surrogate_key(['m.valid_from', 'm.region_id', 'm.ci_fuel_code']) }}
        as regional_generation_mix_fact_key,

    d.date_key,
    r.region_key,
    f.ci_fuel_type_key,

    m.valid_from as half_hour_start_utc,
    cast(m.valid_from as date) as intensity_date,
    m.region_id,
    m.ci_fuel_code,

    m.mix_pct,

    m.loaded_at

from mix m
inner join {{ ref('dim_date') }}         d on cast(m.valid_from as date) = d.date_day
inner join {{ ref('dim_region') }}       r on m.region_id = r.region_id
inner join {{ ref('dim_ci_fuel_type') }} f on m.ci_fuel_code = f.ci_fuel_code
