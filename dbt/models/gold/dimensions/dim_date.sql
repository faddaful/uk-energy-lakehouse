-- Gold dimension: one row per calendar day, 2023-01-01 through 400 days
-- past today (comfortably past the Carbon Intensity API's 48-hour
-- forecast horizon and any realistic backfill need).
--
-- The one thing worth reading carefully here: settlement_periods_in_day.
-- Settlement periods are numbered in LOCAL clock time, not UTC, so the
-- two clock-change days each year do not have 48:
--   - Last Sunday in March (clocks forward, BST starts): 46 periods.
--     The 01:00-02:00 hour does not happen.
--   - Last Sunday in October (clocks back, BST ends): 50 periods.
--     The 01:00-02:00 hour happens twice.
-- (Not "49 or 50": that is a common mistake, corrected here on purpose.)
--
-- bst_start_date / bst_end_date compute "the last Sunday in March/
-- October" by taking 31 March (or 31 October) and walking back to the
-- most recent Sunday. DuckDB's dayofweek() returns 0 for Sunday, so if
-- the 31st is itself a Sunday the subtraction is zero and it stays put;
-- any other day walks back 1-6 days to the Sunday before it. Verified
-- against real, known UK clock-change dates before trusting it, not just
-- reasoned about: this formula gives 2026-03-29 and 2026-10-25, which are
-- the actual 2026 BST start/end dates.

{{ config(materialized='table') }}

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast(current_date + interval 400 day as date)"
    ) }}

),

base as (
    select cast(date_day as date) as date_day
    from spine
),

clock_changes as (
    select distinct
        year(date_day) as yr,
        make_date(year(date_day), 3, 31)
            - cast(dayofweek(make_date(year(date_day), 3, 31)) as integer)  as bst_start_date,
        make_date(year(date_day), 10, 31)
            - cast(dayofweek(make_date(year(date_day), 10, 31)) as integer) as bst_end_date
    from base
),

joined as (
    select
        b.date_day,
        c.bst_start_date,
        c.bst_end_date
    from base b
    join clock_changes c on year(b.date_day) = c.yr
)

select
    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
    date_day,
    year(date_day)                    as calendar_year,
    quarter(date_day)                 as calendar_quarter,
    month(date_day)                   as calendar_month,
    monthname(date_day)               as month_name,
    dayofmonth(date_day)              as day_of_month,
    isodow(date_day)                  as iso_day_of_week,
    dayname(date_day)                 as day_name,
    isodow(date_day) in (6, 7)        as is_weekend,
    week(date_day)                    as iso_week,
    date_trunc('month', date_day)     as month_start_date,

    -- British Summer Time and settlement period counts
    date_day >= bst_start_date and date_day < bst_end_date  as is_bst,
    date_day = bst_start_date                               as is_short_clock_change_day,
    date_day = bst_end_date                                 as is_long_clock_change_day,
    case
        when date_day = bst_start_date then 46
        when date_day = bst_end_date   then 50
        else 48
    end                                                     as settlement_periods_in_day,

    h.holiday_name is not null                              as is_bank_holiday,
    h.holiday_name,

    -- Working day: excludes weekends and England/Wales bank holidays.
    -- England/Wales specifically, not Scotland or Northern Ireland: this
    -- project's data and audience are both GB-wide-but-England-centric
    -- (the home carbon-intensity region is West Midlands), and mixing in
    -- Scottish/NI-only holidays would mark days as non-working that are
    -- ordinary working days for most of the pipeline's actual coverage.
    not (isodow(date_day) in (6, 7) or h.holiday_name is not null) as is_working_day

from joined
left join {{ ref('uk_bank_holidays') }} h
    on joined.date_day = h.holiday_date
    and h.nation = 'england-and-wales'
