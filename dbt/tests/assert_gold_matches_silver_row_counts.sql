-- Singular test. Passes if this query returns zero rows.
--
-- fct_settlement_period inner-joins to dim_date and dim_settlement_period
-- on purpose (see that model's comment), which means a silver row with
-- no matching dimension row silently disappears rather than erroring.
-- This is the check that would actually catch that: if gold's row count
-- ever drops below silver's, something dropped a row that should not
-- have been dropped, most likely dim_date's spine not covering a real
-- settlement_date, or a settlement_period outside 1-50.

with silver_count as (
    select count(*) as n from {{ ref('silver__system_prices') }}
),
gold_count as (
    select count(*) as n from {{ ref('fct_settlement_period') }}
)
select s.n as silver_rows, g.n as gold_rows
from silver_count s, gold_count g
where s.n != g.n
