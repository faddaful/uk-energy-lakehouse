"""
Render a short Markdown report off `mart_revision_summary` (and, for the
biggest-movers table, `silver__price_revisions` directly): mean revision
size, share of periods revised, and the largest individual moves for one
settlement month. Reads the same DuckDB file the Streamlit dashboard
does, read-only, no export step.

Three small functions, the same fetch/render/write split every extractor
in this project already uses, for the same reason: each piece is
testable on its own without a real database.

Reports on the month that just closed, not the current one: run monthly
on the 1st (see dagster_defs/reports.py), by which point last month's
`mart_revision_summary` row is complete. A mid-month report would be
reporting on a month still accumulating revisions, a moving target no
report should claim is final.
"""
import argparse
import datetime
import logging
import os
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]

# Same convention dashboard.py uses to pick the right DuckDB file for
# TARGET=local|azure: gold lives in whichever one dbt last built.
DB_NAME = "lakehouse_azure.duckdb" if os.environ.get("TARGET") == "azure" else "lakehouse.duckdb"
DB_PATH = REPO_ROOT / "data" / DB_NAME

REPORTS_DIR = REPO_ROOT / "reports"


def target_month(today: datetime.date) -> datetime.date:
    """
    The first day of the month before `today`'s month: what "run on the
    1st, report on the month that just closed" actually resolves to.

    Args:
        today (datetime.date): The date the report is being generated on.
    """
    first_of_this_month = today.replace(day=1)
    last_day_of_previous_month = first_of_this_month - datetime.timedelta(days=1)
    return last_day_of_previous_month.replace(day=1)


def fetch_summary(con: duckdb.DuckDBPyConnection, month: datetime.date) -> dict | None:
    """
    One row from mart_revision_summary for `month`, as a dict, or None if
    the month isn't there yet (no settlement periods, or the mart hasn't
    been rebuilt since). None is a real, expected outcome, not an error:
    a month with nothing to report on yet should skip the report, not
    fail loudly.

    Args:
        con (duckdb.DuckDBPyConnection): Read-only connection to gold.
        month (datetime.date): First day of the settlement month to report on.
    """
    df = con.execute(
        "select * from main_gold.mart_revision_summary where settlement_month = ?",
        [month],
    ).fetchdf()
    if df.empty:
        return None
    return df.iloc[0].to_dict()


def fetch_biggest_movers(con: duckdb.DuckDBPyConnection, month: datetime.date, limit: int = 5) -> pd.DataFrame:
    """
    The `limit` largest individual price revisions in `month`, by
    absolute size, from silver__price_revisions directly:
    mart_revision_summary is already aggregated to one row per month, so
    the individual settlement periods behind avg_abs_revision_gbp_per_mwh
    have to come from silver, not gold.

    Args:
        con (duckdb.DuckDBPyConnection): Read-only connection to silver/gold.
        month (datetime.date): First day of the settlement month to report on.
        limit (int): How many movers to return.
    """
    return con.execute(
        """
        select
            settlement_date,
            settlement_period,
            old_system_sell_price,
            new_system_sell_price,
            new_system_sell_price - old_system_sell_price as revision_gbp_per_mwh,
            revised_at
        from main.silver__price_revisions
        where date_trunc('month', settlement_date) = ?
        order by abs(new_system_sell_price - old_system_sell_price) desc
        limit ?
        """,
        [month, limit],
    ).fetchdf()


def render_markdown(summary: dict, movers: pd.DataFrame) -> str:
    """
    Fill the report template with one month's numbers.

    Args:
        summary (dict): One row of mart_revision_summary, from fetch_summary().
        movers (pd.DataFrame): The biggest-movers table, from fetch_biggest_movers().
    """
    month_label = pd.Timestamp(summary["settlement_month"]).strftime("%B %Y")

    lines = [
        f"# Revision summary: {month_label}",
        "",
        (
            f"**{summary['pct_periods_revised']:.2f}%** of settlement periods this month "
            f"({summary['revision_count']} of {summary['total_periods']}) had their published "
            "system sell price revised after first publication."
        ),
        "",
        "## Headline numbers",
        "",
        f"- Periods revised: {summary['revision_count']} of {summary['total_periods']} ({summary['pct_periods_revised']:.2f}%)",
        f"- Days with at least one revision: {summary['days_with_revisions']}",
        f"- Mean revision size: £{summary['avg_abs_revision_gbp_per_mwh']:.2f}/MWh",
        f"- Median revision size: £{summary['median_abs_revision_gbp_per_mwh']:.2f}/MWh",
        f"- Largest single revision: £{summary['max_abs_revision_gbp_per_mwh']:.2f}/MWh",
        f"- Upward vs downward: {summary['upward_revisions']} up, {summary['downward_revisions']} down",
    ]

    if pd.notna(summary["avg_days_to_revision"]):
        lines.append(
            f"- Average time to revision: {summary['avg_days_to_revision']:.1f} days "
            f"(longest: {summary['max_days_to_revision']} days)"
        )

    lines += ["", "## Biggest movers", ""]

    if movers.empty:
        lines.append("No revisions this month.")
    else:
        lines.append("| Settlement date | Period | Old price | New price | Change | Revised at |")
        lines.append("|---|---|---|---|---|---|")
        for _, row in movers.iterrows():
            lines.append(
                f"| {row['settlement_date']} | {row['settlement_period']} "
                f"| £{row['old_system_sell_price']:.2f} | £{row['new_system_sell_price']:.2f} "
                f"| {row['revision_gbp_per_mwh']:+.2f} | {row['revised_at']} |"
            )

    lines += [
        "",
        (
            f"_Generated {datetime.datetime.now(tz=datetime.UTC).strftime('%Y-%m-%d %H:%M UTC')} "
            "from `mart_revision_summary` and `silver__price_revisions`. "
            "See the project README for why this is expected to be small most months._"
        ),
        "",
    ]
    return "\n".join(lines)


def write_report(markdown: str, month: datetime.date, reports_dir: Path | None = None) -> Path:
    """
    Write the rendered report to reports/revision-summary-YYYY-MM.md.
    One file per month, overwritten in place on a re-run for the same
    month rather than accumulating duplicates: a month's numbers can
    still move (a late revision landing after the report first ran), and
    the report should reflect the latest build, not the first one.

    reports_dir defaults to None, not the REPORTS_DIR module constant
    directly: a default *value* is bound once, at def time, so a test
    that monkeypatches revision_report.REPORTS_DIR after import would be
    silently ignored and this would keep writing into the real reports/
    folder. Resolving the module attribute inside the function body
    instead picks up whatever it's set to at call time. Caught by
    actually monkeypatching it and checking where the file landed, not
    assumed from how the syntax reads.

    Args:
        markdown (str): The rendered report, from render_markdown().
        month (datetime.date): First day of the settlement month reported on.
        reports_dir (Path | None): Where reports live. Defaults to REPORTS_DIR.
    """
    if reports_dir is None:
        reports_dir = REPORTS_DIR
    reports_dir.mkdir(parents=True, exist_ok=True)
    path = reports_dir / f"revision-summary-{month.strftime('%Y-%m')}.md"
    path.write_text(markdown)
    return path


def generate_report(month: datetime.date | None = None) -> Path | None:
    """
    Fetch, render and write one month's revision report. Returns the
    path written, or None if there was nothing to report (see
    fetch_summary()).

    Args:
        month (datetime.date | None): The settlement month to report on.
            Defaults to the month before today's, see target_month().
    """
    if month is None:
        month = target_month(datetime.datetime.now(tz=datetime.UTC).date())

    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        summary = fetch_summary(con, month)
        if summary is None:
            logger.warning(f"No mart_revision_summary row for {month}, nothing to report")
            return None
        movers = fetch_biggest_movers(con, month)
    finally:
        con.close()

    markdown = render_markdown(summary, movers)
    path = write_report(markdown, month)
    logger.info(f"Wrote revision report for {month} to {path}")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the monthly price-revision report.")
    parser.add_argument("--month", help="Settlement month to report on, YYYY-MM-01 (defaults to last month).")
    args = parser.parse_args()

    month = datetime.date.fromisoformat(args.month) if args.month else None
    generate_report(month)


if __name__ == "__main__":
    main()
