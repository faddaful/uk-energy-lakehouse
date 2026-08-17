-- Gold dimension: wraps the seed_fuel_type seed with a surrogate key.
-- See seeds/_seeds.yml for the seed's own documentation, including how
-- the code list was verified against 90 days of this project's real
-- bronze data, and why the seed is named seed_fuel_type rather than
-- dim_fuel_type.

{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['fuel_type_code']) }} as fuel_type_key,
    fuel_type_code,
    fuel_type_name,
    fuel_category,
    is_renewable,
    is_interconnector,
    is_dispatchable,
    sort_order
from {{ ref('seed_fuel_type') }}
