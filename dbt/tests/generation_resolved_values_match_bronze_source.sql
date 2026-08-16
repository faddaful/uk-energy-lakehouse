-- Singular test. Passes if this query returns zero rows.
--
-- Every resolved row in silver__generation_by_fuel must be a literal copy
-- of some bronze landing, not a computed or fabricated value. Fails if a
-- resolved row has no matching source row on the columns that should have
-- passed through unchanged.

select
    silver.settlement_date,
    silver.settlement_period,
    silver.fuel_type,
    silver.generation_mw,
    silver.loaded_at
from {{ ref('silver__generation_by_fuel') }} as silver
left join {{ ref('stg_elexon_generation_by_fuel') }} as source
    on silver.settlement_date = source.settlement_date
    and silver.settlement_period = source.settlement_period
    and silver.fuel_type = source.fuel_type
    and silver.generation_mw = source.generation_mw
    and silver.loaded_at = source.loaded_at
where source.settlement_date is null
