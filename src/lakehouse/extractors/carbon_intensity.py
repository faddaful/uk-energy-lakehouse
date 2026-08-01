"""
Call the API for your specific region, check the response looks right, and save it as a parquet file under data/bronze/carbon_intensity/date=YYYY-MM-DD/.

One function fetches, one validates, one lands. These small functions are testable.

Idempotent: running this code twice on the same day overwrites the same file rather than creating duplicates. I achieve this by naming files by their data date, not by the time the script.

Backfill-capable: the function takes a date range argument, so I can load history with one call.

Keep the raw shape: bronze stores what the API sent, plus columns for loaded_at and source. No cleaning is done here. Cleaning happens in silver, where it is visible and testable.

"""

import argparse
import datetime
import json
import logging
import os
import requests
import pandas as pd

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
        logging.error(f"Failed to fetch data: {response.status_code} - {response.text}")
        return pd.DataFrame()  # Return an empty DataFrame on failure

    data = response.json()
    if 'data' not in data or 'data' not in data['data']:
        logging.error("Unexpected response structure")
        return pd.DataFrame()  # Return an empty DataFrame on unexpected structure

    records = []
    for entry in data['data']['data']:
        record = {
            'from': entry['from'],
            'to': entry['to'],
            'intensity_actual': entry['intensity'].get('actual'),
            'intensity_forecast': entry['intensity']['forecast'],
            'region_id': region,
            'loaded_at': datetime.datetime.now().isoformat(),
            'source': 'carbon_intensity_api'
        }
        records.append(record)

    return pd.DataFrame(records)

def validate_carbon_intensity_data(df: pd.DataFrame) -> bool:
    """
    Validate the fetched carbon intensity data.

    Args:
        df (pd.DataFrame): The DataFrame containing carbon intensity data.

    Returns:
        bool: True if the data is valid, False otherwise.
    """
    if df.empty:
        logging.warning("DataFrame is empty")
        return False

    required_columns = ['from', 'to', 'intensity_actual', 'intensity_forecast', 'region_id', 'loaded_at', 'source']
    for col in required_columns:
        if col not in df.columns:
            logging.error(f"Missing required column: {col}")
            return False

    return True

def save_carbon_intensity_data(df: pd.DataFrame, date: str, base_dir: str = "data") -> None:
    """
    Save the validated carbon intensity data as a parquet file.

    Args:
        df (pd.DataFrame): The DataFrame containing carbon intensity data.
        date (str): The date for which the data is being saved, in 'YYYY-MM-DD' format.
        base_dir (str): The root directory to save under. Defaults to 'data'.
    """
    output_dir = os.path.join(base_dir, f"bronze/carbon_intensity/date={date}")
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"carbon_intensity_{date}.parquet")
    
    df.to_parquet(output_file, index=False)
    logging.info(f"Data saved to {output_file}")

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
        save_carbon_intensity_data(df, start_date)


if __name__ == "__main__":
    main()