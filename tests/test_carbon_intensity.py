# Here I want to test the carbon intensity data extraction, validation, and saving process for the west midlands region (region id 13) for the last 7 days.
"""
This test feeds a saved sample JSON into your validate function and checks
it accepts good data and rejects an empty payload, then lands it as a
Delta table and checks the idempotent-overwrite-by-date behaviour: landing
the same data_date twice replaces that date's rows rather than
duplicating them.
"""

import json
import os

import pandas as pd
from deltalake import DeltaTable

from lakehouse.extractors.carbon_intensity import (
    land_carbon_intensity_data,
    land_regional_mix_data,
    validate_carbon_intensity_data,
    validate_regional_mix_data,
)
from lakehouse.io.storage import table_uri


def load_sample_data(file_path: str) -> pd.DataFrame:
    """
    Load sample JSON data from a file and convert it to a DataFrame, the
    same way fetch_carbon_intensity_data() does.

    Args:
        file_path (str): The path to the JSON file.
    """
    with open(file_path, 'r') as f:
        data = json.load(f)

    records = []
    for entry in data['data']['data']:
        record = {
            'data_date': entry['from'][:10],
            'from': entry['from'],
            'to': entry['to'],
            'intensity_actual': entry['intensity'].get('actual'),
            'intensity_forecast': entry['intensity']['forecast'],
            'intensity_index': entry['intensity'].get('index'),
            'region_id': 13,  # West Midlands region id
            'loaded_at': pd.Timestamp.now().isoformat(),
            'source': 'carbon_intensity_api'
        }
        records.append(record)

    return pd.DataFrame(records)


def load_sample_regional_mix_data(file_path: str) -> pd.DataFrame:
    """
    Load the same sample JSON's generationmix arrays, the same way
    fetch_regional_mix_data() does: one row per half hour + fuel.

    Args:
        file_path (str): The path to the JSON file.
    """
    with open(file_path, 'r') as f:
        data = json.load(f)

    records = []
    for entry in data['data']['data']:
        for fuel_entry in entry.get('generationmix', []):
            records.append({
                'data_date': entry['from'][:10],
                'from': entry['from'],
                'to': entry['to'],
                'region_id': 13,
                'fuel': fuel_entry['fuel'],
                'perc': fuel_entry['perc'],
                'loaded_at': pd.Timestamp.now().isoformat(),
                'source': 'carbon_intensity_api',
            })

    return pd.DataFrame(records)

def test_validate_carbon_intensity_data():
    # Load sample data
    sample_data_path = os.path.join(os.path.dirname(__file__), 'sample_carbon_intensity.json')
    df = load_sample_data(sample_data_path)

    # Test validation with good data
    assert validate_carbon_intensity_data(df) == True

    # Test validation with empty DataFrame
    empty_df = pd.DataFrame()
    assert validate_carbon_intensity_data(empty_df) == False

    # Test validation with missing required columns
    incomplete_df = df.drop(columns=['intensity_actual'])
    assert validate_carbon_intensity_data(incomplete_df) == False

def test_land_carbon_intensity_data(tmp_path, monkeypatch):
    # Redirect the local Delta table root at tmp_path, same idea as the
    # old base_dir parameter, but through the env var table_uri() reads.
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    sample_data_path = os.path.join(os.path.dirname(__file__), 'sample_carbon_intensity.json')
    df = load_sample_data(sample_data_path)

    land_carbon_intensity_data(df)

    dt = DeltaTable(table_uri("bronze", "carbon_intensity"))
    saved_df = dt.to_pandas()
    assert not saved_df.empty
    assert len(saved_df) == len(df)


def test_land_carbon_intensity_data_is_idempotent_per_date(tmp_path, monkeypatch):
    # Landing the same data_date twice must replace that date's rows, not
    # duplicate them -- the Delta equivalent of the old "same day
    # overwrites the same file" rule.
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    sample_data_path = os.path.join(os.path.dirname(__file__), 'sample_carbon_intensity.json')
    df = load_sample_data(sample_data_path)

    land_carbon_intensity_data(df)
    land_carbon_intensity_data(df)  # re-run: same data_date(s), should not double the row count

    dt = DeltaTable(table_uri("bronze", "carbon_intensity"))
    assert len(dt.to_pandas()) == len(df)


def test_land_carbon_intensity_data_does_not_touch_other_dates(tmp_path, monkeypatch):
    # Landing one data_date must leave rows already in the table for a
    # different data_date untouched -- the whole point of the
    # partition-scoped predicate over a blind full-table overwrite.
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    day_one = pd.DataFrame([{
        'data_date': '2026-06-01', 'from': '2026-06-01T00:00Z', 'to': '2026-06-01T00:30Z',
        'intensity_actual': None, 'intensity_forecast': 100, 'region_id': 13,
        'loaded_at': pd.Timestamp.now().isoformat(), 'source': 'carbon_intensity_api',
    }])
    day_two = pd.DataFrame([{
        'data_date': '2026-06-02', 'from': '2026-06-02T00:00Z', 'to': '2026-06-02T00:30Z',
        'intensity_actual': None, 'intensity_forecast': 200, 'region_id': 13,
        'loaded_at': pd.Timestamp.now().isoformat(), 'source': 'carbon_intensity_api',
    }])

    land_carbon_intensity_data(day_one)
    land_carbon_intensity_data(day_two)

    dt = DeltaTable(table_uri("bronze", "carbon_intensity"))
    result = dt.to_pandas()
    assert sorted(result['data_date'].unique()) == ['2026-06-01', '2026-06-02']
    assert result.set_index('data_date').loc['2026-06-01', 'intensity_forecast'] == 100
    assert result.set_index('data_date').loc['2026-06-02', 'intensity_forecast'] == 200

def test_validate_regional_mix_data():
    sample_data_path = os.path.join(os.path.dirname(__file__), 'sample_carbon_intensity.json')
    df = load_sample_regional_mix_data(sample_data_path)

    assert validate_regional_mix_data(df) == True

    empty_df = pd.DataFrame()
    assert validate_regional_mix_data(empty_df) == False

    incomplete_df = df.drop(columns=['fuel'])
    assert validate_regional_mix_data(incomplete_df) == False


def test_land_regional_mix_data_is_idempotent_per_date(tmp_path, monkeypatch):
    # Same idempotent-overwrite-by-date behaviour as carbon_intensity
    # itself (see test_land_carbon_intensity_data_is_idempotent_per_date)
    # -- this table is landed the same way, for the same reason.
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    sample_data_path = os.path.join(os.path.dirname(__file__), 'sample_carbon_intensity.json')
    df = load_sample_regional_mix_data(sample_data_path)

    land_regional_mix_data(df)
    land_regional_mix_data(df)  # re-run: same data_date(s), should not double the row count

    dt = DeltaTable(table_uri("bronze", "carbon_intensity_regional_mix"))
    assert len(dt.to_pandas()) == len(df)


# Usage: To run the tests, use the command `pytest` in the terminal. Make sure you have the sample JSON file `sample_carbon_intensity.json` in the same directory as this test file.
