"""
Feeds synthetic mart_revision_summary/silver__price_revisions rows into
a throwaway DuckDB file (same idea as test_price_revision_resolution.py,
just with a real file on disk instead of an in-memory connection, since
revision_report.py opens the DuckDB file by path, not by an
already-open connection) and checks target_month, render_markdown, and
the full fetch-render-write pipeline.
"""

import datetime

import duckdb
import pandas as pd

from lakehouse.reports.revision_report import (
    generate_report,
    render_markdown,
    target_month,
    write_report,
)


def test_target_month_reports_on_the_month_that_just_closed():
    assert target_month(datetime.date(2026, 9, 3)) == datetime.date(2026, 8, 1)
    assert target_month(datetime.date(2026, 1, 15)) == datetime.date(2025, 12, 1)  # year rollover
    assert target_month(datetime.date(2026, 3, 1)) == datetime.date(2026, 2, 1)


def _synthetic_summary() -> dict:
    return {
        "settlement_month": pd.Timestamp("2026-07-01"),
        "revision_count": 3,
        "days_with_revisions": 2,
        "total_periods": 1488,
        "pct_periods_revised": 0.202,
        "avg_abs_revision_gbp_per_mwh": 4.5,
        "median_abs_revision_gbp_per_mwh": 3.0,
        "max_abs_revision_gbp_per_mwh": 9.0,
        "upward_revisions": 2,
        "downward_revisions": 1,
        "avg_days_to_revision": 0.8,
        "max_days_to_revision": 1,
    }


def _synthetic_movers() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "settlement_date": "2026-07-05",
                "settlement_period": 10,
                "old_system_sell_price": 100.0,
                "new_system_sell_price": 109.0,
                "revision_gbp_per_mwh": 9.0,
                "revised_at": "2026-07-06 08:00:00",
            }
        ]
    )


def test_render_markdown_includes_the_headline_numbers():
    markdown = render_markdown(_synthetic_summary(), _synthetic_movers())

    assert "July 2026" in markdown
    assert "0.20%" in markdown
    assert "£4.50/MWh" in markdown  # mean revision size
    assert "£9.00/MWh" in markdown  # largest single revision
    assert "2026-07-05" in markdown  # the mover shows up in the table


def test_render_markdown_handles_no_movers():
    # A month can have revisions logged in the summary but, in principle,
    # an empty movers query (e.g. gold rebuilt between the two queries).
    # Must not crash formatting an empty table.
    markdown = render_markdown(_synthetic_summary(), pd.DataFrame())
    assert "No revisions this month." in markdown


def test_write_report_writes_one_file_per_month(tmp_path):
    path = write_report("# test content", datetime.date(2026, 7, 1), reports_dir=tmp_path)

    assert path == tmp_path / "revision-summary-2026-07.md"
    assert path.read_text() == "# test content"


def test_write_report_overwrites_the_same_month_on_rerun(tmp_path):
    write_report("# first version", datetime.date(2026, 7, 1), reports_dir=tmp_path)
    write_report("# second version", datetime.date(2026, 7, 1), reports_dir=tmp_path)

    files = list(tmp_path.glob("revision-summary-2026-07*.md"))
    assert len(files) == 1
    assert files[0].read_text() == "# second version"


def test_generate_report_end_to_end(tmp_path, monkeypatch):
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("create schema main_gold")
    con.execute("""
        create table main_gold.mart_revision_summary as
        select
            date '2026-07-01' as settlement_month, 3 as revision_count, 2 as days_with_revisions,
            1488 as total_periods, 0.202 as pct_periods_revised, 4.5 as avg_abs_revision_gbp_per_mwh,
            3.0 as median_abs_revision_gbp_per_mwh, 9.0 as max_abs_revision_gbp_per_mwh,
            2 as upward_revisions, 1 as downward_revisions, 0.8 as avg_days_to_revision, 1 as max_days_to_revision
    """)
    con.execute("""
        create table main.silver__price_revisions as
        select * from (values
            (date '2026-07-05', 10, 100.0, 109.0, timestamp '2026-07-06 08:00:00')
        ) as t(settlement_date, settlement_period, old_system_sell_price, new_system_sell_price, revised_at)
    """)
    con.close()

    # revision_report.py opens DB_PATH/REPORTS_DIR by name at call time
    # (see write_report()'s own comment on why this has to be a
    # monkeypatch of the module attribute, not a default-parameter
    # override), so patching the module's globals is what actually
    # redirects it, the same as monkeypatch.setenv redirects the
    # extractors' table_uri() elsewhere in this test suite.
    import lakehouse.reports.revision_report as rr

    monkeypatch.setattr(rr, "DB_PATH", str(db_path))
    monkeypatch.setattr(rr, "REPORTS_DIR", tmp_path / "reports")

    path = generate_report(datetime.date(2026, 7, 1))

    assert path == tmp_path / "reports" / "revision-summary-2026-07.md"
    assert "July 2026" in path.read_text()


def test_generate_report_returns_none_for_a_month_with_no_data(tmp_path, monkeypatch):
    db_path = tmp_path / "empty.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("create schema main_gold")
    con.execute("""
        create table main_gold.mart_revision_summary (
            settlement_month date, revision_count integer, days_with_revisions integer,
            total_periods integer, pct_periods_revised double, avg_abs_revision_gbp_per_mwh double,
            median_abs_revision_gbp_per_mwh double, max_abs_revision_gbp_per_mwh double,
            upward_revisions integer, downward_revisions integer,
            avg_days_to_revision double, max_days_to_revision integer
        )
    """)
    con.close()

    import lakehouse.reports.revision_report as rr

    monkeypatch.setattr(rr, "DB_PATH", str(db_path))
    monkeypatch.setattr(rr, "REPORTS_DIR", tmp_path / "reports")

    assert generate_report(datetime.date(2026, 7, 1)) is None
    assert not (tmp_path / "reports").exists()  # nothing written, nothing to write
