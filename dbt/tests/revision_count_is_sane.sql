-- Singular test. Passes if this query returns zero rows.
--
-- A settlement_date + settlement_period with an implausibly large number
-- of logged revisions almost certainly means the diff logic itself is
-- broken (e.g. floating-point noise tripping the != comparison on every
-- run) rather than genuine price reassessment. Fails if any key has more
-- than this many revisions logged.

select
    settlement_date,
    settlement_period,
    count(*) as revision_count
from {{ ref('silver__price_revisions') }}
group by settlement_date, settlement_period
having count(*) > 10
