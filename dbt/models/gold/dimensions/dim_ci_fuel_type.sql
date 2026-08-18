-- Gold dimension: wraps the seed_ci_fuel_type seed with a surrogate key,
-- the same pattern as dim_fuel_type.sql over seed_fuel_type -- see that
-- seed's own documentation for why this is a second, separate fuel
-- taxonomy rather than a join target for dim_fuel_type.

{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['ci_fuel_code']) }} as ci_fuel_type_key,
    ci_fuel_code,
    ci_fuel_name,
    fuel_category,
    is_renewable,
    is_interconnector,
    sort_order
from {{ ref('seed_ci_fuel_type') }}
