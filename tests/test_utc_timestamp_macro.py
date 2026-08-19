"""
Proves the exact SQL idiom macros/utc_timestamp.sql compiles to
(cast(timezone('UTC', col) as timestamp)) stays correct under a
session TimeZone other than UTC, and that the naive cast it replaced
(cast(col as timestamp)) does not.

This is not a hypothetical regression to guard against: it is exactly
the bug that shipped in this project's original Elexon staging models
and was only caught by accident (see journal.md and the macro's own
comment). It is also invisible on GitHub Actions, whose runners default
to TimeZone='UTC': a test that never sets a different session zone
would pass in CI even if the naive-cast bug came back, the same way it
already did once. Forcing America/New_York here (deliberately not
Europe/London, this project's own dev machine's zone: a genuinely
different zone, both in offset and in which months it's in DST, catches
a wider class of mistake than re-testing against the one zone the bug
was originally found in) is what actually makes this test mean
something in CI, not just locally.
"""

import duckdb


def test_utc_timestamp_macro_is_correct_under_a_non_utc_session():
    con = duckdb.connect()
    con.execute("set TimeZone='America/New_York'")

    result = con.execute("""
        select cast(timezone('UTC', timestamp with time zone '2026-07-01 00:30:00+00') as timestamp) as forced_utc
    """).fetchone()

    assert str(result[0]) == "2026-07-01 00:30:00"


def test_the_naive_cast_this_macro_replaced_is_actually_wrong_under_a_non_utc_session():
    # Documents the failure mode this project shipped with, not just
    # asserts the fix works in isolation: proves cast(col as timestamp)
    # really does silently corrupt the value under a non-UTC session,
    # so there is a concrete, reproduced "before" to compare the "after"
    # against, not just a claim in a comment.
    con = duckdb.connect()
    con.execute("set TimeZone='America/New_York'")

    result = con.execute("""
        select cast(timestamp with time zone '2026-07-01 00:30:00+00' as timestamp) as naive_cast
    """).fetchone()

    assert str(result[0]) != "2026-07-01 00:30:00"
    assert str(result[0]) == "2026-06-30 20:30:00"  # New York is UTC-4 in July (EDT)


def test_utc_timestamp_macro_agrees_across_different_session_timezones():
    # The real property that matters: the same input produces the same
    # output no matter which session runs it, which is what makes this
    # safe to run identically in CI (UTC) and on a real dev machine
    # (whatever the OS's local zone happens to be).
    results = set()
    for tz in ["UTC", "Europe/London", "America/New_York", "Australia/Sydney"]:
        con = duckdb.connect()
        con.execute(f"set TimeZone='{tz}'")
        r = con.execute("""
            select cast(timezone('UTC', timestamp with time zone '2026-07-01 00:30:00+00') as timestamp) as forced_utc
        """).fetchone()
        results.add(str(r[0]))

    assert results == {"2026-07-01 00:30:00"}
