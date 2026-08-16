-- Singular test. Passes if this query returns zero rows.
--
-- Every resolved row in silver__system_prices must be a literal copy of
-- some bronze landing, not a computed or fabricated value. Fails if a
-- resolved row has no matching source row on the columns that should have
-- passed through unchanged.

select
    silver.settlement_date,
    silver.settlement_period,
    silver.system_sell_price,
    silver.loaded_at
from {{ ref('silver__system_prices') }} as silver
left join {{ ref('stg_elexon_system_prices') }} as source
    on silver.settlement_date = source.settlement_date
    and silver.settlement_period = source.settlement_period
    and silver.system_sell_price = source.system_sell_price
    and silver.loaded_at = source.loaded_at
where source.settlement_date is null
