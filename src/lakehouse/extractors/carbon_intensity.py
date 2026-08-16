"""
Call the API for your specific region, check the response looks right, and
land it as a Delta table under bronze/carbon_intensity (data/bronze/... for
TARGET=local, an ADLS container for TARGET=azure -- see lakehouse.io.storage).

One function fetches, one validates, one lands. These small functions are
testable.

Idempotent: landing the same data_date twice replaces that date's rows in
place rather than creating duplicates or growing the table unboundedly.
This is a partition-scoped Delta overwrite (mode="overwrite", predicate on
data_date), not a raw file overwrite: every other date already in the
table is left untouched, and the replaced version is still recoverable
through Delta's history/time travel if you ever need it. Elexon's bronze
tables use mode="append" instead and never replace anything -- see
elexon_system_prices.py's module docstring for why the two sources differ.

Backfill-capable: the function takes a date range argument, so I can load
history with one call.

Keep the raw shape: bronze stores what the API sent, plus columns for
loaded_at and source. No cleaning is done here. Cleaning happens in
silver, where it is visible and testable.
"""
import argparse
import datetime
import logging

import pandas as pd
import requests
from deltalake import write_deltalake

from lakehouse.io.storage import storage_options, table_uri

# Use own logger to identify logging messages by module name.
logger = logging.getLogger(__name__)


def fetch_carbon_intensity_data(start_date: str, end_date: str, region: str) -> pd.DataFrame:
    """
    Fetch carbon intensity data from the API for a specific region and date range.

    Args:
        start_date (str): The start date in 'YYYY-MM-DD' format.
        end_date (str): The end date in 'YYYY-MM-DD' format.
        region (str): The region for which to fetch carbon intensity data.
    """
    url = f"https://api.carbonintensity.org.uk/regional/intensity/{start_date}/{end_date}/regionid/{region}"
    response = requests.get(url)

    if response.status_code != 200:
        logger.error(f"Failed to fetch data: {response.status_code} - {response.text}")
        return pd.DataFrame()  # Return an empty DataFrame on failure

    data = response.json()
    if 'data' not in data or 'data' not in data['data']:
        logger.error("Unexpected response structure")
        return pd.DataFrame()  # Return an empty DataFrame on unexpected structure

    records = []
    for entry in data['data']['data']:
        record = {
            'data_date': entry['from'][:10],
            'from': entry['from'],
            'to': entry['to'],
            'intensity_actual': entry['intensity'].get('actual'),
            'intensity_forecast': entry['intensity']['forecast'],
            'region_id': region,
            'loaded_at': datetime.datetime.now(tz=datetime.UTC).isoformat(),
            'source': 'carbon_intensity_api'
        }
        records.append(record)

    df = pd.DataFrame(records)
    # The regional endpoint is forecast-only (see README): intensity_actual
    # is None for every row. A pandas column that is None in every row has
    # no non-null value for pyarrow to infer a type from, so it lands as
    # Arrow's "null"/void type rather than a real nullable numeric type.
    # DuckDB's delta_scan() (and other Delta readers) reject void-typed
    # columns outright, so this must be forced to a concrete type before
    # write_deltalake ever sees it, not left to type inference.
    if not df.empty:
        df['intensity_actual'] = df['intensity_actual'].astype('float64')
    return df

def validate_carbon_intensity_data(df: pd.DataFrame) -> bool:
    """
    Validate the fetched carbon intensity data.

    Args:
        df (pd.DataFrame): The DataFrame containing carbon intensity data.

    Returns:
        bool: True if the data is valid, False otherwise.
    """
    if df.empty:
        logger.warning("DataFrame is empty")
        return False

    required_columns = ['data_date', 'from', 'to', 'intensity_actual', 'intensity_forecast', 'region_id', 'loaded_at', 'source']
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            return False

    return True

def save_carbon_intensity_data(df: pd.DataFrame, date: str) -> None:
    """
    Land one data_date's rows into the bronze Delta table, replacing
    whatever was there for that date before (idempotent re-run) without
    touching any other date already in the table.

    partition_by=["data_date"] means Delta physically separates each
    date's files, so this replace only ever rewrites files for `date`.
    The predicate is also a correctness check for free: every row in df
    must actually match data_date = date, or the write fails -- catching a
    fetch/grouping bug here instead of silently landing a date's data
    under the wrong partition.

    Args:
        df (pd.DataFrame): The DataFrame containing carbon intensity data
            for exactly one data_date.
        date (str): The data_date being landed, in 'YYYY-MM-DD' format.
    """
    write_deltalake(
        table_uri("bronze", "carbon_intensity"),
        df,
        mode="overwrite",
        predicate=f"data_date = '{date}'",
        partition_by=["data_date"],
        storage_options=storage_options(),
    )
    logger.info(f"Landed {len(df)} rows for data_date={date}")


def land_carbon_intensity_data(df: pd.DataFrame) -> None:
    """
    Split a fetched, validated DataFrame by data_date and land each date's
    rows as its own partition-scoped overwrite. A single fetch call can
    span more than one date (--start-date/--end-date), so this must not
    assume the whole DataFrame belongs to one date.

    Args:
        df (pd.DataFrame): Fetched, already-validated carbon intensity
            data, possibly spanning more than one data_date.
    """
    for data_date, group in df.groupby("data_date"):
        save_carbon_intensity_data(group.reset_index(drop=True), str(data_date))

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch, validate, and save UK Carbon Intensity regional data."
    )
    parser.add_argument("--date", help="Single date to fetch, in YYYY-MM-DD format.")
    parser.add_argument("--start-date", help="Start of a date range, in YYYY-MM-DD format.")
    parser.add_argument("--end-date", help="End of a date range, in YYYY-MM-DD format.")
    parser.add_argument(
        "--region", default="8", help="Region id to fetch data for (default: 8, West Midlands)."
    )
    args = parser.parse_args()

    if args.date:
        start_date = args.date
        end_date = (datetime.date.fromisoformat(args.date) + datetime.timedelta(days=1)).isoformat()
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        parser.error("Provide either --date, or both --start-date and --end-date.")

    df = fetch_carbon_intensity_data(start_date, end_date, args.region)
    if validate_carbon_intensity_data(df):
        land_carbon_intensity_data(df)


if __name__ == "__main__":
    main()
