"""
Feed a synthetic revision scenario through the same window-function idiom
that silver__system_prices.sql and silver__price_revisions.sql use, and
check the right row wins.

This is a fast, dbt-independent check of the resolution *logic* using an
in-memory DuckDB table: it does not run dbt itself (CI's `dbt build`
against tests/fixtures/elexon_system_prices/ already exercises the real
compiled models end to end). The SQL here is a deliberate, minimal copy of
the ordering rule those models use: row_number() over (partition by
settlement_date, settlement_period order by loaded_at desc), keep row 1.
If that ordering rule ever changes in the dbt models, change it here too.
"""

import duckdb
import pandas as pd


def synthetic_bronze() -> pd.DataFrame:
    """
    Three settlement periods. Period 1 is landed twice, with a different
    price the second time (a revision). Periods 2 and 3 are each landed
    once (the common case: nothing to revise).
    """
    return pd.DataFrame(
        [
            # period 1: first landing, later revised
            {
                "settlement_date": "2026-08-14",
                "settlement_period": 1,
                "system_sell_price": 163.0,
                "loaded_at": pd.Timestamp("2026-08-16 08:00:00", tz="UTC"),
            },
            # period 1: revision, higher price, landed an hour later
            {
                "settlement_date": "2026-08-14",
                "settlement_period": 1,
                "system_sell_price": 168.0,
                "loaded_at": pd.Timestamp("2026-08-16 09:00:00", tz="UTC"),
            },
            # period 2: never revised
            {
                "settlement_date": "2026-08-14",
                "settlement_period": 2,
                "system_sell_price": 163.0,
                "loaded_at": pd.Timestamp("2026-08-16 08:00:00", tz="UTC"),
            },
        ]
    )


def resolve(df: pd.DataFrame) -> pd.DataFrame:
    """The same row_number()-over-partition idiom as silver__system_prices.sql."""
    con = duckdb.connect()
    # .copy() forces contiguous arrays. DuckDB's pandas scan cannot read the
    # negative-stride numpy views a reversed slice (df.iloc[::-1]) leaves
    # behind on non-string columns, even after reset_index.
    con.register("bronze", df.copy())
    return con.sql(
        """
        with ranked as (
            select
                *,
                row_number() over (
                    partition by settlement_date, settlement_period
                    order by loaded_at desc
                ) as row_num
            from bronze
        )
        select settlement_date, settlement_period, system_sell_price, loaded_at
        from ranked
        where row_num = 1
        """
    ).df()


def revisions(df: pd.DataFrame) -> pd.DataFrame:
    """The same lag()-over-partition idiom as silver__price_revisions.sql."""
    con = duckdb.connect()
    con.register("bronze", df.copy())
    return con.sql(
        """
        with with_previous as (
            select
                settlement_date,
                settlement_period,
                system_sell_price,
                loaded_at,
                lag(system_sell_price) over (
                    partition by settlement_date, settlement_period
                    order by loaded_at
                ) as previous_system_sell_price
            from bronze
        )
        select settlement_date, settlement_period,
               previous_system_sell_price as old_price,
               system_sell_price as new_price
        from with_previous
        where previous_system_sell_price is not null
          and system_sell_price != previous_system_sell_price
        """
    ).df()


def test_resolution_keeps_the_latest_loaded_at_row():
    resolved = resolve(synthetic_bronze())

    # One resolved row per settlement_date + settlement_period.
    assert len(resolved) == 2

    period_1 = resolved[resolved["settlement_period"] == 1].iloc[0]
    # The later landing (168.0), not the first one (163.0), must win.
    assert period_1["system_sell_price"] == 168.0
    assert period_1["loaded_at"] == pd.Timestamp("2026-08-16 09:00:00", tz="UTC")

    period_2 = resolved[resolved["settlement_period"] == 2].iloc[0]
    assert period_2["system_sell_price"] == 163.0


def test_resolution_is_unaffected_by_input_row_order():
    # The window function must not depend on the order rows happen to
    # arrive in bronze, only on loaded_at. Feed the same scenario in
    # reverse and check the outcome is identical.
    reversed_df = synthetic_bronze().iloc[::-1].reset_index(drop=True)
    resolved = resolve(reversed_df)

    period_1 = resolved[resolved["settlement_period"] == 1].iloc[0]
    assert period_1["system_sell_price"] == 168.0


def test_revision_log_captures_old_and_new_value():
    revision_rows = revisions(synthetic_bronze())

    # Only period 1 was revised; period 2 must not appear.
    assert len(revision_rows) == 1
    row = revision_rows.iloc[0]
    assert row["settlement_period"] == 1
    assert row["old_price"] == 163.0
    assert row["new_price"] == 168.0


def test_revision_log_is_empty_when_nothing_was_revised():
    unrevised = synthetic_bronze().iloc[[0, 2]].reset_index(drop=True)
    assert revisions(unrevised).empty
