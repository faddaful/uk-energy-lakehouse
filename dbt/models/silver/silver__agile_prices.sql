-- Silver: one trustworthy row per half hour (valid_from). Bronze lands
-- via a partition-scoped overwrite per data_date (see octopus_agile.py),
-- so this dedup is defensive, not something normal operation is
-- expected to need: the same reasoning silver_regional_intensity's own
-- comment gives for Carbon Intensity, which is landed the same way.
-- Latest loaded_at wins per valid_from, same window-function idiom as
-- every other silver model in this project.
--
-- No day/night classification here: that is a real business question
-- (what counts as "night" on this specific tariff), not a rename/dedup
-- concern, and it needs dim_date.is_bst to answer correctly -- silver
-- does not depend on gold in this project, so that logic lives in
-- fct_agile_prices.sql instead, which already joins dim_date routinely.

with ranked as (

    select
        *,
        row_number() over (
            partition by valid_from
            order by loaded_at desc
        ) as row_num
    from {{ ref('stg_octopus_agile_prices') }}

)

select
    valid_from,
    valid_to,
    unit_rate_exc_vat_p_per_kwh,
    unit_rate_inc_vat_p_per_kwh,
    loaded_at
from ranked
where row_num = 1
