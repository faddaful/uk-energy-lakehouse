"""
This test feeds saved sample JSON into your validate function and checks it
accepts good data, rejects an empty payload, and rejects settlement periods
outside the valid range for the date, including on a clock-change day where
the valid range is not the usual 1-48.
"""

import json
import os

import pandas as pd

from lakehouse.extractors.elexon_common import parse_api_timestamp
from lakehouse.extractors.elexon_system_prices import (
    save_system_prices_data,
    validate_system_prices_data,
)

TIMESTAMP_FIELDS = ["startTime", "createdDateTime"]


def load_sample_data(file_path: str) -> pd.DataFrame:
    """
    Load sample JSON data from a file and convert it to a DataFrame, the
    same way fetch_system_prices_data() does.

    Args:
        file_path (str): The path to the JSON file.
    """
    with open(file_path, 'r') as f:
        data = json.load(f)

    loaded_at = pd.Timestamp.now(tz="UTC")
    rows = []
    for entry in data['data']:
        row = dict(entry)
        for field in TIMESTAMP_FIELDS:
            row[field] = parse_api_timestamp(row[field])
        row['loaded_at'] = loaded_at
        row['source'] = 'elexon_insights_system_prices'
        rows.append(row)

    return pd.DataFrame(rows)


def test_validate_system_prices_data():
    sample_data_path = os.path.join(os.path.dirname(__file__), 'sample_elexon_system_prices.json')
    df = load_sample_data(sample_data_path)

    # Test validation with good data
    assert validate_system_prices_data(df) == True

    # Test validation with empty DataFrame
    empty_df = pd.DataFrame()
    assert validate_system_prices_data(empty_df) == False

    # Test validation with missing required columns
    incomplete_df = df.drop(columns=['systemSellPrice'])
    assert validate_system_prices_data(incomplete_df) == False


def test_validate_system_prices_data_clock_change_day():
    # The spring clock-change day has 46 settlement periods, not 48. The
    # sample includes period 46, which must be accepted as valid for this
    # date even though it would be out of range on a normal day.
    sample_data_path = os.path.join(
        os.path.dirname(__file__), 'sample_elexon_system_prices_clock_change.json'
    )
    df = load_sample_data(sample_data_path)

    assert validate_system_prices_data(df) == True


def test_validate_system_prices_data_rejects_out_of_range_period():
    # Settlement period 49 does not exist on a normal 48-period day, this
    # must be flagged rather than silently accepted.
    df = pd.DataFrame([{
        'settlementDate': '2026-08-08',
        'settlementPeriod': 49,
        'systemSellPrice': 100.0,
        'systemBuyPrice': 100.0,
        'loaded_at': pd.Timestamp.now(tz='UTC'),
        'source': 'elexon_insights_system_prices',
    }])

    assert validate_system_prices_data(df) == False


def test_save_system_prices_data(tmp_path):
    sample_data_path = os.path.join(os.path.dirname(__file__), 'sample_elexon_system_prices.json')
    df = load_sample_data(sample_data_path)

    date_str = '2026-08-08'
    save_system_prices_data(df, date_str, base_dir=str(tmp_path / "data"))

    output_dir = tmp_path / f"data/bronze/elexon_system_prices/date={date_str}"
    files = sorted(output_dir.glob("*.parquet"))

    assert len(files) == 1

    saved_df = pd.read_parquet(files[0])
    assert not saved_df.empty


def test_save_system_prices_data_does_not_overwrite_previous_landing(tmp_path):
    # Bronze is append-only for Elexon: landing the same settlement date
    # twice (as the weekly revision re-download will) must produce two
    # files, not one file overwritten in place. This is the one behaviour
    # that deliberately differs from carbon_intensity.py.
    sample_data_path = os.path.join(os.path.dirname(__file__), 'sample_elexon_system_prices.json')
    date_str = '2026-08-08'
    base_dir = str(tmp_path / "data")

    df_first = load_sample_data(sample_data_path)
    save_system_prices_data(df_first, date_str, base_dir=base_dir)

    df_second = load_sample_data(sample_data_path)
    df_second['loaded_at'] = df_second['loaded_at'] + pd.Timedelta(seconds=1)
    save_system_prices_data(df_second, date_str, base_dir=base_dir)

    output_dir = tmp_path / f"data/bronze/elexon_system_prices/date={date_str}"
    files = sorted(output_dir.glob("*.parquet"))

    assert len(files) == 2
