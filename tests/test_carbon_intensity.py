# Here I want to test the carbon intensity data extraction, validation, and saving process for the west midlands region (region id 13) for the last 7 days.
"""
This test feeds a saved sample JSON into your validate function and checks it accepts good data and rejects an empty payload. 

I will run this with uv run pytest.
"""

import os
import json
import pandas as pd
import pytest
from lakehouse.extractors.carbon_intensity import validate_carbon_intensity_data, save_carbon_intensity_data 

def load_sample_data(file_path: str) -> pd.DataFrame:
    """
    Load sample JSON data from a file and convert it to a DataFrame.

    Args:
        file_path (str): The path to the JSON file. 
    """
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    records = []
    for entry in data['data']['data']:
        record = {
            'from': entry['from'],
            'to': entry['to'],
            'intensity_actual': entry['intensity'].get('actual'),
            'intensity_forecast': entry['intensity']['forecast'],
            'region_id': 13,  # West Midlands region id
            'loaded_at': pd.Timestamp.now().isoformat(),
            'source': 'carbon_intensity_api'
        }
        records.append(record)

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

def test_save_carbon_intensity_data(tmp_path):
    # Load sample data
    sample_data_path = os.path.join(os.path.dirname(__file__), 'sample_carbon_intensity.json')
    df = load_sample_data(sample_data_path)

    # Save the data to a temporary directory
    date_str = pd.Timestamp.now().strftime('%Y-%m-%d')
    save_carbon_intensity_data(df, date_str, base_dir=str(tmp_path / "data"))

    # Check if the file was created
    output_dir = tmp_path / f"data/bronze/carbon_intensity/date={date_str}"
    output_file = output_dir / f"carbon_intensity_{date_str}.parquet"
    
    assert output_file.exists()

# Check if the saved file can be read back into a DataFrame
    saved_df = pd.read_parquet(output_file)
    assert not saved_df.empty

# Usage: To run the tests, use the command `pytest` in the terminal. Make sure you have the sample JSON file `sample_carbon_intensity.json` in the same directory as this test file.

