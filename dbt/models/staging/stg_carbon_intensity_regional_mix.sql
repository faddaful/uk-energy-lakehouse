-- Staging: read bronze (a Delta table), rename to clean names, cast
-- types. No business logic here. Same shape as stg_carbon_intensity.sql,
-- reading a sibling bronze table. See carbon_intensity.py's module
-- docstring for why generationmix lands as its own table rather than a
-- nested column on the intensity one.
--
-- "from"/"to" are SQL keywords, so they are quoted here and renamed
-- immediately to valid_from/valid_to, matching stg_carbon_intensity.sql.

select
    strptime("from", '%Y-%m-%dT%H:%MZ')  as valid_from,
    strptime("to",   '%Y-%m-%dT%H:%MZ')  as valid_to,
    cast(region_id as integer)           as region_id,
    fuel                                 as ci_fuel_code,
    cast(perc as double)                 as mix_pct,
    cast(loaded_at as timestamp)         as loaded_at,
    source
from {{ bronze('carbon_intensity_regional_mix') }}
