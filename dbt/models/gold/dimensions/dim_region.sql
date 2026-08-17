-- Gold dimension: wraps the seed_region seed with a surrogate key, so
-- facts join on a uniform hashed key type the same way every other
-- dimension does, rather than joining on the natural key directly.
-- See seeds/_seeds.yml for the seed's own documentation, including the
-- "country" simplification for region 6, the GSP group verification, and
-- why the seed is named seed_region rather than dim_region.

{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['region_id']) }} as region_key,
    region_id,
    region_name,
    region_short_name,
    country,
    gsp_group_id,
    is_aggregate
from {{ ref('seed_region') }}
