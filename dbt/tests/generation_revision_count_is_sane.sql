-- Singular test. Passes if this query returns zero rows.
--
-- Same rationale as revision_count_is_sane.sql: a key with an implausibly
-- large number of logged revisions almost certainly means the diff logic
-- is broken (e.g. floating-point noise tripping the != comparison on
-- every run) rather than genuine revisions.

select
    settlement_date,
    settlement_period,
    fuel_type,
    count(*) as revision_count
from {{ ref('silver__generation_revisions') }}
group by settlement_date, settlement_period, fuel_type
having count(*) > 10
