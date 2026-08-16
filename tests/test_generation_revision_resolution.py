"""
Feed a synthetic revision scenario through the same window-function idiom
that silver__generation_by_fuel.sql and silver__generation_revisions.sql
use, and check the right row wins.

Same shape as test_price_revision_resolution.py, with the one difference
that matters: the partition/group key here is (settlement_date,
settlement_period, fuel_type), not (settlement_date, settlement_period),
since one row is one fuel type within one settlement period for this
dataset. This is a fast, dbt-independent check of the resolution *logic*
using an in-memory DuckDB table; CI's `dbt build` against
tests/fixtures/elexon_generation_by_fuel/ exercises the real compiled
models end to end.
"""

import duckdb
import pandas as pd


def synthetic_bronze() -> pd.DataFrame:
    """
    Two fuel types in one settlement period. WIND is landed twice, with a
    different generation figure the second time (a revision). CCGT is
    landed once (the common case: nothing to revise). Same settlement
    period, different fuel_type, is a different key, not a revision of
    each other.
    """
    return pd.DataFrame(
        [
            # WIND: first landing, later revised
            {
                "settlement_date": "2026-08-14",
                "settlement_period": 1,
                "fuel_type": "WIND",
                "generation_mw": 5441.0,
                "loaded_at": pd.Timestamp("2026-08-16 08:00:00", tz="UTC"),
            },
            # WIND: revision, higher output, landed an hour later
            {
                "settlement_date": "2026-08-14",
                "settlement_period": 1,
                "fuel_type": "WIND",
                "generation_mw": 5900.0,
                "loaded_at": pd.Timestamp("2026-08-16 09:00:00", tz="UTC"),
            },
            # CCGT: same period, different fuel_type, never revised
            {
                "settlement_date": "2026-08-14",
                "settlement_period": 1,
                "fuel_type": "CCGT",
                "generation_mw": 8046.0,
                "loaded_at": pd.Timestamp("2026-08-16 08:00:00", tz="UTC"),
            },
        ]
    )


def resolve(df: pd.DataFrame) -> pd.DataFrame:
    """The same row_number()-over-partition idiom as silver__generation_by_fuel.sql."""
    con = duckdb.connect()
    # .copy() forces contiguous arrays; see test_price_revision_resolution.py.
    con.register("bronze", df.copy())
    return con.sql(
        """
        with ranked as (
            select
                *,
                row_number() over (
                    partition by settlement_date, settlement_period, fuel_type
                    order by loaded_at desc
                ) as row_num
            from bronze
        )
        select settlement_date, settlement_period, fuel_type, generation_mw, loaded_at
        from ranked
        where row_num = 1
        """
    ).df()


def revisions(df: pd.DataFrame) -> pd.DataFrame:
    """The same lag()-over-partition idiom as silver__generation_revisions.sql."""
    con = duckdb.connect()
    con.register("bronze", df.copy())
    return con.sql(
        """
        with with_previous as (
            select
                settlement_date,
                settlement_period,
                fuel_type,
                generation_mw,
                loaded_at,
                lag(generation_mw) over (
                    partition by settlement_date, settlement_period, fuel_type
                    order by loaded_at
                ) as previous_generation_mw
            from bronze
        )
        select settlement_date, settlement_period, fuel_type,
               previous_generation_mw as old_generation_mw,
               generation_mw as new_generation_mw
        from with_previous
        where previous_generation_mw is not null
          and generation_mw != previous_generation_mw
        """
    ).df()


def test_resolution_keeps_the_latest_loaded_at_row():
    resolved = resolve(synthetic_bronze())

    # One resolved row per settlement_date + settlement_period + fuel_type.
    assert len(resolved) == 2

    wind = resolved[resolved["fuel_type"] == "WIND"].iloc[0]
    # The later landing (5900.0), not the first one (5441.0), must win.
    assert wind["generation_mw"] == 5900.0
    assert wind["loaded_at"] == pd.Timestamp("2026-08-16 09:00:00", tz="UTC")

    ccgt = resolved[resolved["fuel_type"] == "CCGT"].iloc[0]
    assert ccgt["generation_mw"] == 8046.0


def test_resolution_does_not_mix_up_fuel_types_in_the_same_period():
    # WIND and CCGT share settlement_date + settlement_period but are
    # different keys. A partition bug that dropped fuel_type would either
    # merge them or let one clobber the other.
    resolved = resolve(synthetic_bronze())
    assert set(resolved["fuel_type"]) == {"WIND", "CCGT"}


def test_revision_log_captures_old_and_new_value():
    revision_rows = revisions(synthetic_bronze())

    # Only WIND was revised; CCGT must not appear.
    assert len(revision_rows) == 1
    row = revision_rows.iloc[0]
    assert row["fuel_type"] == "WIND"
    assert row["old_generation_mw"] == 5441.0
    assert row["new_generation_mw"] == 5900.0


def test_revision_log_is_empty_when_nothing_was_revised():
    unrevised = synthetic_bronze().iloc[[0, 2]].reset_index(drop=True)
    assert revisions(unrevised).empty
