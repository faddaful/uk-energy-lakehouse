-- Silver: one trustworthy row per half hour + region + fuel.
-- Same dedup idiom as silver_regional_intensity.sql (newest loaded_at
-- wins per key) -- see that model's comment for why bronze can contain
-- more than one landing for the same key. Named with the double
-- underscore this project's other silver models use, not the single
-- underscore silver_regional_intensity has -- see dbt_project.yml's
-- silver_prefixes comment for why that one is a known, deliberately
-- un-renamed inconsistency rather than a convention to keep matching.

with ranked as (

    select
        *,
        row_number() over (
            partition by valid_from, region_id, ci_fuel_code
            order by loaded_at desc
        ) as row_num
    from {{ ref('stg_carbon_intensity_regional_mix') }}

)

select
    valid_from,
    valid_to,
    region_id,
    ci_fuel_code,
    mix_pct,
    loaded_at
from ranked
where row_num = 1
