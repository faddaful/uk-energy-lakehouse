"""
Land your own hand-entered electricity usage into bronze: the manual-
data equivalent of every other extractor's fetch/validate/land shape.
Feeds mart_tariff_comparison (see README's "The money story"). Without
a smart meter's half-hourly export, day/night totals per billing
period, read straight off your supplier's own app, are the real
granularity actually available, and the plan this follows says as much:
"Manual CSV export monthly is fine; do not over-engineer this."

Reads data/manual/electricity_usage.csv (gitignored, like every other
real bronze table under data/: this is personal financial-adjacent
data, never meant for a public repo, see README's own reasoning for why
the Streamlit dashboard itself stays off the open internet). One row
per period you've looked at in your supplier's app, e.g. one row per
week or one row per month, whatever the app happened to show when you
last checked it. Periods do not need to be contiguous or a fixed length.

CSV columns (header row required):
    period_start,period_end,day_kwh,night_kwh,estimated_cost_gbp
    2026-07-01,2026-07-31,172.21,107.50,52.38
    2026-08-10,2026-08-16,38.29,23.83,11.63

period_start/period_end are inclusive calendar dates, matching what the
app itself shows ("10 Aug - 16 Aug 2026", "Jul 2026"). estimated_cost_gbp
is the app's own number, standing charge and VAT excluded (matching the
app's own caption): using the app's own real bill estimate as "what you
actually paid" is more honest than this project trying to reconstruct
your current tariff's rate structure from scratch, which it does not
know and cannot verify independently.

Overwrite, not append, unlike Elexon: you are expected to hand-edit this
CSV over time (correcting a typo, adding a new period), and there is no
"the CSV revised itself" concept worth keeping an audit trail for, the
way Elexon's published prices can genuinely change after the fact.
Every run replaces the whole bronze table with the CSV's current
contents. schema_mode="overwrite", not "merge", for the same reason: a
future column added to the CSV format should just replace the schema,
not be treated as a partition-scoped additive change the way Carbon
Intensity's schema_mode="merge" is (there is no partition here at all).
"""
import argparse
import logging
from pathlib import Path

import pandas as pd
from deltalake import write_deltalake

from lakehouse.io.storage import storage_options, table_uri

# Use own logger to identify logging messages by module name.
logger = logging.getLogger(__name__)

SOURCE = "manual_entry"

DEFAULT_CSV_PATH = "data/manual/electricity_usage.csv"

REQUIRED_COLUMNS = ["period_start", "period_end", "day_kwh", "night_kwh", "estimated_cost_gbp"]


def fetch_usage_data(csv_path: str = DEFAULT_CSV_PATH) -> pd.DataFrame:
    """
    Args:
        csv_path (str): Path to your hand-maintained usage CSV, see the
            module docstring for the format.
    """
    path = Path(csv_path)
    if not path.exists():
        logger.error(f"{csv_path} does not exist. See manual_usage.py's module docstring for the format.")
        return pd.DataFrame()

    df = pd.read_csv(path)
    if not df.empty:
        # Normalise to plain YYYY-MM-DD strings regardless of how the
        # CSV happened to write the date (pandas is lenient on read),
        # so bronze always stores one consistent date format.
        df["period_start"] = pd.to_datetime(df["period_start"]).dt.strftime("%Y-%m-%d")
        df["period_end"] = pd.to_datetime(df["period_end"]).dt.strftime("%Y-%m-%d")
    df["loaded_at"] = pd.Timestamp.now(tz="UTC")
    df["source"] = SOURCE
    return df


def validate_usage_data(df: pd.DataFrame) -> bool:
    """
    Checks the required columns are present and no period's end date is
    before its start date. Does not check for overlapping periods:
    that's a real question (double-counted usage) but not one this
    function can answer from one CSV alone without knowing whether two
    overlapping rows are a mistake or a deliberate re-check, so it's
    left for a human reviewing the CSV, not silently rejected here.

    Args:
        df (pd.DataFrame): The DataFrame containing manually entered usage data.
    """
    if df.empty:
        logger.warning("DataFrame is empty")
        return False

    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        logger.error(f"Missing required columns: {missing}")
        return False

    bad_order = df[pd.to_datetime(df["period_end"]) < pd.to_datetime(df["period_start"])]
    if not bad_order.empty:
        logger.error(
            f"period_end before period_start: {bad_order[['period_start', 'period_end']].to_dict('records')}"
        )
        return False

    return True


def land_usage_data(df: pd.DataFrame) -> None:
    """
    Args:
        df (pd.DataFrame): Fetched, already-validated usage data.
    """
    write_deltalake(
        table_uri("bronze", "electricity_usage"),
        df,
        mode="overwrite",
        schema_mode="overwrite",
        storage_options=storage_options(),
    )
    logger.info(f"Landed {len(df)} usage periods")


def main() -> None:
    parser = argparse.ArgumentParser(description="Land hand-entered electricity usage into bronze.")
    parser.add_argument(
        "--csv-path", default=DEFAULT_CSV_PATH, help=f"Path to the usage CSV (default: {DEFAULT_CSV_PATH})."
    )
    args = parser.parse_args()

    df = fetch_usage_data(args.csv_path)
    if validate_usage_data(df):
        land_usage_data(df)


if __name__ == "__main__":
    main()
