-- Gold dimension: wraps the seed_neso_technology seed with a surrogate
-- key, same pattern as dim_fuel_type over seed_fuel_type. See
-- seeds/_seeds.yml for the seed's own documentation.

{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['technology']) }} as technology_key,
    technology,
    technology_category,
    is_renewable,
    is_generation,
    sort_order
from {{ ref('seed_neso_technology') }}
