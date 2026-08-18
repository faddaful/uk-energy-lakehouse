-- Gold dimension: a static 50-row dimension, one row per possible
-- settlement period number (1-50). A degenerate-ish dimension: the
-- period number already carries meaning on its own, and everything below
-- is a documented convention layered on top of it, not a fact.
--
-- Three things worth knowing before joining to this table:
--
-- 1. nominal_start_time_local is a CONVENIENCE, not truth. It maps
--    period N to the wall-clock time it names on a normal 48-period day.
--    On the two clock-change days a year that mapping shifts (see
--    dim_date.sql), so this column is never a substitute for a real
--    timestamp. fct_settlement_period carries its own authoritative
--    timestamp (Elexon's own startTime, already UTC, not derived from
--    period arithmetic) for exactly this reason.
--
-- 2. is_peak_block is a DEFINED CONVENTION, not a fact. 07:00-19:00 is
--    the standard GB traded peak block; a reader reusing this column
--    should know that is a choice this project made, not a property of
--    the market.
--
-- 3. is_evening_peak is ANALYTICAL, not commercial. The old triad regime
--    that made 16:00-19:00 winter evenings financially decisive for
--    demand customers was reformed for half-hourly settlement, so treat
--    this as "system stress window", not "your bill depends on this".
--
-- Periods 49 and 50 exist in this dimension every day, even though they
-- only ever appear in a fact table on the one long clock-change day a
-- year (see dim_date.settlement_periods_in_day). That is correct
-- behaviour for a conformed dimension, not a bug: is_clock_change_only_
-- period makes it explicit rather than leaving it to be discovered.

{{ config(materialized='table') }}

with periods as (
    select unnest(generate_series(1, 50)) as settlement_period
),

enriched as (
    select
        settlement_period,
        -- Nominal local clock time. Valid on 48-period days only.
        (settlement_period - 1) / 2                            as nominal_hour,
        case when settlement_period % 2 = 1 then 0 else 30 end as nominal_minute
    from periods
)

select
    {{ dbt_utils.generate_surrogate_key(['settlement_period']) }} as settlement_period_key,
    settlement_period,

    lpad(cast(nominal_hour as varchar), 2, '0') || ':' ||
    lpad(cast(nominal_minute as varchar), 2, '0')            as nominal_start_time_local,

    nominal_hour,

    -- Traded peak block: 07:00 to 19:00, i.e. periods 15 to 38.
    settlement_period between 15 and 38                       as is_peak_block,

    -- System-stress evening window: 16:00 to 19:00, periods 33 to 38.
    settlement_period between 33 and 38                       as is_evening_peak,

    -- Overnight: 23:00 to 07:00.
    settlement_period >= 47 or settlement_period <= 14        as is_overnight,

    -- EFA blocks: six four-hour blocks starting at 23:00, the standard
    -- GB balancing-market convention.
    case
        when settlement_period between 47 and 50 or settlement_period <= 6 then 1
        when settlement_period between 7 and 14                            then 2
        when settlement_period between 15 and 22                          then 3
        when settlement_period between 23 and 30                          then 4
        when settlement_period between 31 and 38                          then 5
        else 6
    end                                                       as efa_block,

    settlement_period > 48                                    as is_clock_change_only_period

from enriched
