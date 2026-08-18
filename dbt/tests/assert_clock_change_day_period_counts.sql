-- Singular test. Passes if this query returns zero rows.
--
-- This is the "fixture proving it would" version of the clock-change
-- test: it does not depend on any real bronze/silver data ever having
-- reached a real clock-change day, because dim_date is fully
-- deterministic (a date spine plus a static holidays seed, both always
-- present). It runs against dim_date's entire real range on every
-- build (2023 through ~400 days past today), so it covers every
-- clock-change day in that span, not just one hand-picked example.
--
-- A dbt unit test (fixed given/expect rows) was the guide's original
-- suggestion here, but does not actually fit this model: dim_date's rows
-- come from dbt_utils.date_spine's own generate_series call, not from a
-- ref() that a unit test's `given:` block can override, so there is no
-- way to shrink the unit test's expected output down to a couple of
-- example dates without also somehow overriding the spine. Testing every
-- day the real model produces is strictly stronger anyway: it is what
-- caught, for free, that 31 March 2024 is itself a Sunday (exactly the
-- off-by-one edge case the guide warned needed dedicated coverage), with
-- no need to have picked that year by hand.

select
    date_day,
    is_short_clock_change_day,
    is_long_clock_change_day,
    settlement_periods_in_day
from {{ ref('dim_date') }}
where (is_short_clock_change_day and settlement_periods_in_day != 46)
   or (is_long_clock_change_day and settlement_periods_in_day != 50)
   or (
        not is_short_clock_change_day
        and not is_long_clock_change_day
        and settlement_periods_in_day != 48
   )
