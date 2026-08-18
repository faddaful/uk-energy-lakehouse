"""
Call the Elexon Insights API for system buy/sell price and imbalance data
at settlement period grain, check the response looks right, and land it as
a Delta table under bronze/elexon_system_prices (data/bronze/... for
TARGET=local, an ADLS container for TARGET=azure, see lakehouse.io.storage).

Same shape as carbon_intensity.py: one function fetches, one validates, one
lands. Unlike carbon_intensity.py, landing here is append-only
(mode="append"), not idempotent-by-overwrite: every write adds rows, none
ever replaces or removes an existing one, so landing the same settlement
date twice leaves both the old and the new value sitting in the table side
by side. Elexon can revise a published price after the fact; overwriting
bronze on every re-download would throw away the earlier value a revision
audit needs to diff against. Carbon Intensity has no revision concept, so
overwriting there is correct and simpler. See the README for the fuller
version of this distinction.

Bronze keeps every field the API returned, using the API's own field names,
plus loaded_at and source. No cleaning, dedup, or revision resolution here.
That happens in silver, where it is visible and testable. This is stricter
about keeping the full raw shape than carbon_intensity.py, which only keeps
a handful of chosen fields. That was fine there because the fields it drops
(the generation mix breakdown) are nested and need a real modelling decision
to flatten. Nothing here is nested, so there is no reason to drop anything.

The natural key is (settlement_date, settlement_period), and both columns
are preserved as landed under their API names (settlementDate,
settlementPeriod). Settlement periods run 1-48 on almost all days, 46 on
the spring clock-change day, and 50 on the autumn one. The valid count for
a given date is computed, not hardcoded, in elexon_common.py.
"""
import argparse
import datetime
import logging

import pandas as pd
from deltalake import write_deltalake

from lakehouse.extractors.elexon_common import (
    expected_settlement_period_count,
    get_with_retry,
    parse_api_timestamp,
)
from lakehouse.io.storage import storage_options, table_uri

# Use own logger to identify logging messages by module name.
logger = logging.getLogger(__name__)

SOURCE = "elexon_insights_system_prices"

# Fields in the raw response that are timestamps and need explicit parsing
# rather than being left as raw strings or blind-cast later.
TIMESTAMP_FIELDS = ["startTime", "createdDateTime"]


def fetch_system_prices_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch system sell/buy prices for every settlement period in a date range.

    Args:
        start_date (str): Start of the range, in 'YYYY-MM-DD' format, inclusive.
        end_date (str): End of the range, in 'YYYY-MM-DD' format, inclusive.

    Both dates are inclusive here. That is different from
    fetch_carbon_intensity_data(), where end_date is exclusive, because that
    matches the Carbon Intensity API's own convention. Do not assume the
    same convention applies across extractors, check each API.

    The system-prices endpoint takes exactly one settlement date in the URL
    path, it does not accept a date range. So a multi-day call here loops
    one request per day and concatenates the results. get_with_retry()
    sleeps before every request, so this loop is already rate limited.
    """
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    if end < start:
        raise ValueError(f"end_date {end_date} is before start_date {start_date}")

    # One loaded_at per fetch call, not one per record. Every row landed by
    # this call was landed in the same batch, so they should share one
    # timestamp rather than each drifting by a few milliseconds.
    loaded_at = pd.Timestamp.now(tz="UTC")

    all_rows = []
    current = start
    while current <= end:
        date_str = current.isoformat()
        data = get_with_retry(f"/balancing/settlement/system-prices/{date_str}")
        records = data.get("data", [])

        if not records:
            logger.warning(f"No system price records returned for {date_str}")

        for entry in records:
            row = dict(entry)
            for field in TIMESTAMP_FIELDS:
                if field in row:
                    row[field] = parse_api_timestamp(row[field])
            row["loaded_at"] = loaded_at
            row["source"] = SOURCE
            all_rows.append(row)

        current += datetime.timedelta(days=1)

    return pd.DataFrame(all_rows)


def validate_system_prices_data(df: pd.DataFrame) -> bool:
    """
    Validate the fetched system prices data.

    Checks the required columns are present, and that every settlement
    period falls within the valid range for its date (1-46, 1-48, or 1-50
    depending on the date). Does not require every period in the range to
    be present. A same-day fetch run before the day has finished will
    legitimately be missing later periods, and that is not a data quality
    problem at bronze grain, it is just a partial day so far.

    Args:
        df (pd.DataFrame): The DataFrame containing system prices data.

    Returns:
        bool: True if the data is valid, False otherwise.
    """
    if df.empty:
        logger.warning("DataFrame is empty")
        return False

    required_columns = [
        "settlementDate",
        "settlementPeriod",
        "systemSellPrice",
        "systemBuyPrice",
        "loaded_at",
        "source",
    ]
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            return False

    valid = True
    for settlement_date, group in df.groupby("settlementDate"):
        settlement_date = (
            settlement_date.decode() if isinstance(settlement_date, (bytes, bytearray)) else str(settlement_date)
        )
        date_obj = datetime.date.fromisoformat(settlement_date)
        expected_count = expected_settlement_period_count(date_obj)
        periods = group["settlementPeriod"]

        out_of_range = periods[(periods < 1) | (periods > expected_count)]
        if not out_of_range.empty:
            logger.error(
                f"{settlement_date} has settlement periods outside the valid "
                f"1-{expected_count} range: {sorted(out_of_range.unique().tolist())}"
            )
            valid = False

        if len(periods) < expected_count:
            logger.info(
                f"{settlement_date} has {len(periods)} of {expected_count} "
                "settlement periods so far, this is expected for a day that "
                "has not finished yet"
            )

    return valid


def land_system_prices_data(df: pd.DataFrame) -> None:
    """
    Append a fetched, validated DataFrame to the bronze Delta table.

    One write regardless of how many settlement dates df spans:
    partition_by=["settlementDate"] is what makes Delta physically
    separate each date's files, there is no need to group by date and
    write once per date the way the pre-Delta file-per-landing version of
    this function had to. mode="append" never replaces or removes a row,
    so landing a settlement date that already has rows in the table adds
    to it rather than overwriting it, see the module docstring for why.

    schema_mode="merge": a df with a column this table doesn't have yet
    still lands, backfilling NULL on every row already there, rather than
    raising SchemaMismatchError and forcing bronze to be dropped and
    re-landed from the API to add a column. See carbon_intensity.py's
    save_carbon_intensity_data() for where that was actually reached for
    and shouldn't have been, since this project documents bronze as the
    immutable record.

    Args:
        df (pd.DataFrame): Fetched, already-validated system prices data,
            possibly spanning more than one settlement date.
    """
    write_deltalake(
        table_uri("bronze", "elexon_system_prices"),
        df,
        mode="append",
        partition_by=["settlementDate"],
        schema_mode="merge",
        storage_options=storage_options(),
    )
    logger.info(f"Landed {len(df)} rows")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch, validate, and save Elexon system price data."
    )
    parser.add_argument("--date", help="Single settlement date to fetch, in YYYY-MM-DD format.")
    parser.add_argument("--start-date", help="Start of a date range, in YYYY-MM-DD format (inclusive).")
    parser.add_argument("--end-date", help="End of a date range, in YYYY-MM-DD format (inclusive).")
    args = parser.parse_args()

    if args.date:
        start_date = end_date = args.date
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        parser.error("Provide either --date, or both --start-date and --end-date.")

    df = fetch_system_prices_data(start_date, end_date)
    if validate_system_prices_data(df):
        land_system_prices_data(df)


if __name__ == "__main__":
    main()
