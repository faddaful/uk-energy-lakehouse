-- Singular test. Passes if this query returns zero rows.
--
-- Every settlement_date with a materially complete day of prices should
-- have exactly as many distinct settlement periods as dim_date says that
-- calendar day is supposed to have -- 46 on the short clock-change day,
-- 50 on the long one, 48 otherwise. This is the test that would actually
-- catch a clock-change bug in production, as opposed to
-- assert_clock_change_day_period_counts.sql, which proves the
-- calendar-side logic in dim_date is right but never touches real
-- ingested data.
--
-- "actual_periods >= 40" excludes materially incomplete days rather than
-- every day: CI's own fixture data is deliberately a handful of rows for
-- one partial settlement_date (see tests/fixtures/elexon_system_prices),
-- not a real complete day, and this test would otherwise flag that
-- fixture as a clock-change bug on every CI run. 40 is comfortably below
-- 46, the lowest real period count a genuinely complete day ever has, so
-- nothing that looks like real, complete production data gets excluded
-- by this floor -- only fixtures and days still mid-ingestion.

select
    a.settlement_date,
    a.actual_periods,
    d.settlement_periods_in_day as expected_periods
from (
    select settlement_date, count(distinct settlement_period) as actual_periods
    from {{ ref('fct_settlement_period') }}
    where settlement_date < current_date - 3
    group by 1
) a
join {{ ref('dim_date') }} d on a.settlement_date = d.date_day
where a.actual_periods >= 40
  and a.actual_periods != d.settlement_periods_in_day
