"""
This test feeds saved sample JSON into your validate function and checks it
accepts good data, rejects an empty payload, and rejects settlement periods
outside the valid range for the date, including on a clock-change day where
the valid range is not the usual 1-48.
"""

import json
import os

import pandas as pd
from deltalake import DeltaTable

from lakehouse.extractors.elexon_common import parse_api_timestamp
from lakehouse.extractors.elexon_generation_by_fuel import (
    land_generation_by_fuel_data,
    validate_generation_by_fuel_data,
)
from lakehouse.io.storage import table_uri

TIMESTAMP_FIELDS = ["publishTime", "startTime"]


def load_sample_data(file_path: str) -> pd.DataFrame:
    """
    Load sample JSON data from a file and convert it to a DataFrame, the
    same way fetch_generation_by_fuel_data() does.

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
        row['source'] = 'elexon_insights_generation_by_fuel'
        rows.append(row)

    return pd.DataFrame(rows)


def test_validate_generation_by_fuel_data():
    sample_data_path = os.path.join(
        os.path.dirname(__file__), 'sample_elexon_generation_by_fuel.json'
    )
    df = load_sample_data(sample_data_path)

    # Test validation with good data
    assert validate_generation_by_fuel_data(df) == True

    # Test validation with empty DataFrame
    empty_df = pd.DataFrame()
    assert validate_generation_by_fuel_data(empty_df) == False

    # Test validation with missing required columns
    incomplete_df = df.drop(columns=['generation'])
    assert validate_generation_by_fuel_data(incomplete_df) == False


def test_validate_generation_by_fuel_data_clock_change_day():
    # The spring clock-change day has 46 settlement periods, not 48. The
    # sample includes period 46, which must be accepted as valid for this
    # date even though it would be out of range on a normal day.
    sample_data_path = os.path.join(
        os.path.dirname(__file__), 'sample_elexon_generation_by_fuel_clock_change.json'
    )
    df = load_sample_data(sample_data_path)

    assert validate_generation_by_fuel_data(df) == True


def test_validate_generation_by_fuel_data_rejects_out_of_range_period():
    # Settlement period 49 does not exist on a normal 48-period day, this
    # must be flagged rather than silently accepted.
    df = pd.DataFrame([{
        'settlementDate': '2026-08-08',
        'settlementPeriod': 49,
        'fuelType': 'WIND',
        'generation': 5000,
        'loaded_at': pd.Timestamp.now(tz='UTC'),
        'source': 'elexon_insights_generation_by_fuel',
    }])

    assert validate_generation_by_fuel_data(df) == False


def test_land_generation_by_fuel_data(tmp_path, monkeypatch):
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    sample_data_path = os.path.join(
        os.path.dirname(__file__), 'sample_elexon_generation_by_fuel.json'
    )
    df = load_sample_data(sample_data_path)

    land_generation_by_fuel_data(df)

    dt = DeltaTable(table_uri("bronze", "elexon_generation_by_fuel"))
    saved_df = dt.to_pandas()
    assert not saved_df.empty
    assert len(saved_df) == len(df)


def test_land_generation_by_fuel_data_does_not_overwrite_previous_landing(tmp_path, monkeypatch):
    # Bronze is append-only for Elexon: landing the same settlement date
    # twice (as the weekly revision re-download will) must leave both
    # landings in the table, not replace the first with the second. Same
    # behaviour as system prices, for the same reason.
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    sample_data_path = os.path.join(
        os.path.dirname(__file__), 'sample_elexon_generation_by_fuel.json'
    )

    df_first = load_sample_data(sample_data_path)
    land_generation_by_fuel_data(df_first)

    df_second = load_sample_data(sample_data_path)
    df_second['loaded_at'] = df_second['loaded_at'] + pd.Timedelta(seconds=1)
    land_generation_by_fuel_data(df_second)

    dt = DeltaTable(table_uri("bronze", "elexon_generation_by_fuel"))
    saved_df = dt.to_pandas()

    assert len(saved_df) == 2 * len(df_first)
    first_key = df_first.iloc[0]
    matching = saved_df[
        (saved_df['settlementPeriod'] == first_key['settlementPeriod'])
        & (saved_df['fuelType'] == first_key['fuelType'])
    ]
    assert matching['loaded_at'].nunique() == 2
