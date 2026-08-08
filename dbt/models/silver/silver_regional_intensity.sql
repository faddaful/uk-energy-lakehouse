-- Silver: one trustworthy row per half hour per region.
-- Your extractor runs every 30 minutes and each run can re-fetch periods it
-- has seen before, so bronze contains duplicates by design. Here the latest
-- loaded_at wins for each (valid_from, region_id) pair.
--
-- The pattern: number the rows within each group, newest first, keep row 1.
-- This same window-function idiom, with a settlement-run ranking added,
-- becomes the Elexon revision resolution in Phase 2. Learn it here on easy
-- data.

with ranked as (

    select
        *,
        row_number() over (
            partition by valid_from, region_id
            order by loaded_at desc
        ) as row_num
    from {{ ref('stg_carbon_intensity') }}

)

select
    valid_from,
    valid_to,
    region_id,
    intensity_forecast,
    intensity_actual,
    loaded_at
from ranked
where row_num = 1