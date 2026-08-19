"""
Fetch Octopus Agile's published half-hourly unit rates for the home
region, check the response looks right, and land it as a Delta table
under bronze/octopus_agile_prices (data/bronze/... for TARGET=local, an
ADLS container for TARGET=azure, see lakehouse.io.storage).

Same fetch/validate/land shape as every other extractor. No API key:
the products/tariffs endpoints are Octopus's own public price feed, not
account data, confirmed by actually calling it unauthenticated before
writing this, not assumed.

PRODUCT_CODE/TARIFF_CODE are for West Midlands (GSP group letter "E",
matching seed_region.gsp_group_id for region_id 8, this project's
HOME_REGION_ID everywhere else it appears: carbon_intensity.py,
mart_best_hours_today.sql, and now mart_tariff_comparison.sql). Change
the GSP letter here if that region ever changes; Octopus's tariff code
format (E-1R-<product code>-<GSP letter>) bakes the region straight
into the code, there is no separate region parameter to pass.

Idempotent, like Carbon Intensity, not append-only like Elexon: Octopus
publishes each half hour's rate once, the day before it applies (a
day-ahead auction result), and does not revise it afterward. Landing
the same date range twice replaces those rows rather than duplicating
them, the same partition-scoped overwrite as
carbon_intensity.save_carbon_intensity_data().

Paginated, unlike every other extractor here: confirmed against a real
31-day request before writing this, not assumed from the docs. The
default page size (100) would need 15 requests for a month; page_size
is set generously (1500, comfortably more than a year's worth in one
call: 365 * 48 = 17,520 still needs 12 pages, but any realistic backfill
window for this project needs far less) and `next` is still followed in
a loop regardless, so a request wide enough to need a second page still
works correctly rather than silently returning page 1 only.
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

BASE_URL = "https://api.octopus.energy/v1"
PRODUCT_CODE = "AGILE-24-10-01"
GSP_GROUP_LETTER = "E"  # West Midlands, see module docstring
TARIFF_CODE = f"E-1R-{PRODUCT_CODE}-{GSP_GROUP_LETTER}"

SOURCE = "octopus_agile"

USER_AGENT = "uk-energy-lakehouse/0.1 (personal learning project; contact: olayanjubiodun24@gmail.com)"

PAGE_SIZE = 1500


def fetch_agile_prices(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch every half-hourly Agile rate published for [start_date, end_date).

    Args:
        start_date (str): Start date, 'YYYY-MM-DD', inclusive.
        end_date (str): End date, 'YYYY-MM-DD', exclusive, matching
            Octopus's own period_to convention (checked against the
            live API, not assumed): a request for period_to=2026-08-01
            does not include any 2026-08-01 half hour.
    """
    url = f"{BASE_URL}/products/{PRODUCT_CODE}/electricity-tariffs/{TARIFF_CODE}/standard-unit-rates/"
    params = {
        "period_from": f"{start_date}T00:00:00Z",
        "period_to": f"{end_date}T00:00:00Z",
        "page_size": PAGE_SIZE,
    }
    loaded_at = pd.Timestamp.now(tz="UTC")

    rows = []
    next_url, next_params = url, params
    while next_url:
        response = requests.get(next_url, params=next_params, headers={"User-Agent": USER_AGENT}, timeout=30)
        response.raise_for_status()
        data = response.json()

        for entry in data["results"]:
            rows.append(
                {
                    "valid_from": entry["valid_from"],
                    "valid_to": entry["valid_to"],
                    "value_exc_vat": entry["value_exc_vat"],
                    "value_inc_vat": entry["value_inc_vat"],
                    "loaded_at": loaded_at,
                    "source": SOURCE,
                }
            )

        # `next` is already a full URL with its own query string, so no
        # params are passed alongside it on the following request.
        next_url = data["next"]
        next_params = None

    df = pd.DataFrame(rows)
    if not df.empty:
        df["valid_from"] = pd.to_datetime(df["valid_from"], utc=True, format="ISO8601")
        df["valid_to"] = pd.to_datetime(df["valid_to"], utc=True, format="ISO8601")
        # Partition key for land_agile_prices_data(): the UTC calendar
        # date the half hour starts on, same idea as carbon_intensity.py's
        # data_date.
        df["data_date"] = df["valid_from"].dt.date.astype(str)
    return df


def validate_agile_prices_data(df: pd.DataFrame) -> bool:
    """
    Checks the required columns are present and every half hour has a
    real, distinct valid_from/valid_to pair.

    Args:
        df (pd.DataFrame): The DataFrame containing fetched Agile prices.
    """
    if df.empty:
        logger.warning("DataFrame is empty")
        return False

    required_columns = ["valid_from", "valid_to", "value_exc_vat", "value_inc_vat", "loaded_at", "source"]
    for col in required_columns:
        if col not in df.columns:
            logger.error(f"Missing required column: {col}")
            return False

    bad_rows = df[df["valid_from"] >= df["valid_to"]]
    if not bad_rows.empty:
        logger.error(f"valid_from not before valid_to: {bad_rows[['valid_from', 'valid_to']].to_dict('records')}")
        return False

    return True


def land_agile_prices_data(df: pd.DataFrame) -> None:
    """
    Split a fetched, validated DataFrame by data_date and land each
    date's rows as its own partition-scoped overwrite, the same pattern
    (and the same reasoning) as carbon_intensity.land_carbon_intensity_data().

    Args:
        df (pd.DataFrame): Fetched, already-validated Agile prices,
            possibly spanning more than one data_date.
    """
    for data_date, group in df.groupby("data_date"):
        write_deltalake(
            table_uri("bronze", "octopus_agile_prices"),
            group.reset_index(drop=True),
            mode="overwrite",
            predicate=f"data_date = '{data_date}'",
            partition_by=["data_date"],
            schema_mode="merge",
            storage_options=storage_options(),
        )
    logger.info(f"Landed {len(df)} half-hourly rates across {df['data_date'].nunique()} date(s)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch, validate, and save Octopus Agile half-hourly unit rates.")
    parser.add_argument("--date", help="Single date to fetch, in YYYY-MM-DD format.")
    parser.add_argument("--start-date", help="Start of a date range, in YYYY-MM-DD format (inclusive).")
    parser.add_argument("--end-date", help="End of a date range, in YYYY-MM-DD format (exclusive).")
    args = parser.parse_args()

    if args.date:
        start_date = args.date
        end_date = (datetime.date.fromisoformat(args.date) + datetime.timedelta(days=1)).isoformat()
    elif args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        parser.error("Provide either --date, or both --start-date and --end-date.")

    df = fetch_agile_prices(start_date, end_date)
    if validate_agile_prices_data(df):
        land_agile_prices_data(df)


if __name__ == "__main__":
    main()
