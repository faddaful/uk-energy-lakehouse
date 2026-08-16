"""
Call the Elexon Insights API for generation by fuel type at settlement
period grain, check the response looks right, and save it as a parquet
file under data/bronze/elexon_generation_by_fuel/date=YYYY-MM-DD/.

Same shape as carbon_intensity.py and elexon_system_prices.py: one function
fetches, one validates, one lands. Landing is append-only, the same as
elexon_system_prices.py and for the same reason: the file name embeds the
loaded_at of this landing, so running this twice for the same date adds a
second file rather than replacing the first. Bronze keeps every field the
API returned, under the API's own field names, plus loaded_at and source.
No cleaning, dedup, or revision resolution here; that happens in silver.

This uses the FUELHH dataset (half-hourly generation by fuel type), not
FUELINST (five-minute instantaneous generation by fuel type). FUELHH is
the one at settlement period grain, which is what the natural key for this
project needs, and what lines up with system prices for a future join.

One thing worth being explicit about: for system prices, one row is one
settlement period, so (settlement_date, settlement_period) is a full row
key. For this dataset, one row is one fuel type within one settlement
period, so the actual row key is (settlement_date, settlement_period,
fuel_type). (settlement_date, settlement_period) is still the natural key
in the sense of being the settlement grain this dataset shares with system
prices, it just is not, on its own, unique per row here.
"""
import argparse
import datetime
import logging
import os

import pandas as pd

from lakehouse.extractors.elexon_common import (
    expected_settlement_period_count,
    get_with_retry,
    parse_api_timestamp,
)

# Use own logger to identify logging messages by module name.
logger = logging.getLogger(__name__)

SOURCE = "elexon_insights_generation_by_fuel"

# Fields in the raw response that are timestamps and need explicit parsing
# rather than being left as raw strings or blind-cast later.
TIMESTAMP_FIELDS = ["publishTime", "startTime"]


# The API rejects settlementDateFrom/settlementDateTo spans wider than this
# with a 400: "The date range between SettlementDateFrom and SettlementDateTo
# inclusive must not exceed 7 days". Confirmed against the live API, not
# documented up front, so a multi-week backfill has to be chunked into
# windows of this size rather than sent as one request.
MAX_DAYS_PER_REQUEST = 7


def fetch_generation_by_fuel_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch generation by fuel type for every settlement period in a date range.

    Args:
        start_date (str): Start of the range, in 'YYYY-MM-DD' format, inclusive.
        end_date (str): End of the range, in 'YYYY-MM-DD' format, inclusive.

    Both dates are inclusive here, matching the API's own
    settlementDateFrom/settlementDateTo convention. FUELHH accepts a native
    date range, so a backfill needs far fewer requests than one per day, but
    the range is capped server-side at MAX_DAYS_PER_REQUEST days inclusive.
    A longer range is chunked into windows of that size and concatenated, the
    same idea as the one-request-per-day loop in elexon_system_prices.py,
    just with a bigger step.
    """
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    if end < start:
        raise ValueError(f"end_date {end_date} is before start_date {start_date}")

    # One loaded_at per fetch call, not one per chunk or per record. Every
    # row landed by this call was landed in the same batch, so they should
    # share one timestamp rather than each drifting by a few milliseconds
    # or, worse, by however long the backfill takes to run.
    loaded_at = pd.Timestamp.now(tz="UTC")

    rows = []
    window_start = start
    while window_start <= end:
        window_end = min(window_start + datetime.timedelta(days=MAX_DAYS_PER_REQUEST - 1), end)
        params = {
            "settlementDateFrom": window_start.isoformat(),
            "settlementDateTo": window_end.isoformat(),
        }
        data = get_with_retry("/datasets/FUELHH", params=params)

        records = data.get("data", [])
        if not records:
            logger.warning(
                f"No generation-by-fuel records returned for "
                f"{window_start.isoformat()} to {window_end.isoformat()}"
            )

        for entry in records:
            row = dict(entry)
            for field in TIMESTAMP_FIELDS:
                if field in row:
                    row[field] = parse_api_timestamp(row[field])
            row["loaded_at"] = loaded_at
            row["source"] = SOURCE
            rows.append(row)

        window_start = window_end + datetime.timedelta(days=1)

    return pd.DataFrame(rows)


def validate_generation_by_fuel_data(df: pd.DataFrame) -> bool:
    """
    Validate the fetched generation-by-fuel data.

    Checks the required columns are present, and that every settlement
    period falls within the valid range for its date (1-46, 1-48, or 1-50
    depending on the date). Does not require every period, or every fuel
    type within a period, to be present. A same-day fetch run before the
    day has finished will legitimately be missing later periods.

    Args:
        df (pd.DataFrame): The DataFrame containing generation-by-fuel data.

    Returns:
        bool: True if the data is valid, False otherwise.
    """
    if df.empty:
        logger.warning("DataFrame is empty")
        return False

    required_columns = [
        "settlementDate",
        "settlementPeriod",
        "fuelType",
        "generation",
        "loaded_at",
        "source",
    ]
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            return False

    valid = True
    for settlement_date, group in df.groupby("settlementDate"):
        date_value = settlement_date
        if isinstance(date_value, bytes):
            date_value = date_value.decode("utf-8")
        date_obj = datetime.date.fromisoformat(str(date_value))
        expected_count = expected_settlement_period_count(date_obj)
        periods = group["settlementPeriod"]

        out_of_range = periods[(periods < 1) | (periods > expected_count)]
        if not out_of_range.empty:
            logger.error(
                f"{settlement_date} has settlement periods outside the valid "
                f"1-{expected_count} range: {sorted(out_of_range.unique().tolist())}"
            )
            valid = False

    return valid


def save_generation_by_fuel_data(df: pd.DataFrame, date: str, base_dir: str = "data") -> None:
    """
    Save the validated generation-by-fuel data as a new parquet file.

    Bronze is append-only here: the file name embeds the loaded_at of this
    landing (shared by every row, since fetch_generation_by_fuel_data()
    stamps one loaded_at per call), so calling this again for a date that
    was already landed writes a second file alongside the first rather
    than replacing it. Silver is what picks a single truth out of however
    many bronze landings exist for a given settlement_date +
    settlement_period + fuel_type; this function's job is only to keep
    every one of them.

    Args:
        df (pd.DataFrame): The DataFrame containing generation-by-fuel data.
        date (str): The settlement date this data is for, in 'YYYY-MM-DD' format.
        base_dir (str): The root directory to save under. Defaults to 'data'.
    """
    output_dir = os.path.join(base_dir, f"bronze/elexon_generation_by_fuel/date={date}")
    os.makedirs(output_dir, exist_ok=True)

    loaded_at_tag = df["loaded_at"].iloc[0].strftime("%Y%m%dT%H%M%SZ")
    output_file = os.path.join(output_dir, f"elexon_generation_by_fuel_{date}_{loaded_at_tag}.parquet")

    df.to_parquet(output_file, index=False)
    # Use own logger
    logger.info(f"Saved {len(df)} rows to {output_file}")


def land_generation_by_fuel_data(df: pd.DataFrame, base_dir: str = "data") -> None:
    """
    Split a fetched, validated DataFrame by settlement date and save each
    date's rows as its own bronze landing.

    Shared by the CLI and the Dagster asset so the settlement-date grouping
    (and the bytes-vs-str handling groupby can hand back, depending on the
    backing dtype) lives in one place.

    Args:
        df (pd.DataFrame): Fetched, already-validated generation-by-fuel
            data, possibly spanning more than one settlement date.
        base_dir (str): The root directory to save under. Defaults to 'data'.
    """
    for settlement_date, group in df.groupby("settlementDate"):
        date_value = settlement_date
        if isinstance(date_value, bytes):
            date_value = date_value.decode("utf-8")
        save_generation_by_fuel_data(group.reset_index(drop=True), str(date_value), base_dir=base_dir)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch, validate, and save Elexon generation-by-fuel-type data."
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

    df = fetch_generation_by_fuel_data(start_date, end_date)
    if validate_generation_by_fuel_data(df):
        land_generation_by_fuel_data(df)


if __name__ == "__main__":
    main()
