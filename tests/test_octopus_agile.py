"""
This test feeds saved sample JSON (a real 4-row response, see
sample_octopus_agile.json's own header) into validate/land, the same
shape as test_carbon_intensity.py: no live network call in the test
suite itself, fetch_agile_prices() was verified against the real API by
hand (see octopus_agile.py's module docstring for what that confirmed).
"""

import json
import os

import pandas as pd
from deltalake import DeltaTable

from lakehouse.extractors.octopus_agile import (
    land_agile_prices_data,
    validate_agile_prices_data,
)
from lakehouse.io.storage import table_uri

SOURCE = "octopus_agile"


def load_sample_data(file_path: str) -> pd.DataFrame:
    """
    Load sample JSON data and shape it the same way
    fetch_agile_prices() does.

    Args:
        file_path (str): The path to the JSON file.
    """
    with open(file_path) as f:
        data = json.load(f)

    loaded_at = pd.Timestamp.now(tz="UTC")
    rows = []
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

    df = pd.DataFrame(rows)
    df["valid_from"] = pd.to_datetime(df["valid_from"], utc=True, format="ISO8601")
    df["valid_to"] = pd.to_datetime(df["valid_to"], utc=True, format="ISO8601")
    df["data_date"] = df["valid_from"].dt.date.astype(str)
    return df


def test_validate_agile_prices_data():
    sample_data_path = os.path.join(os.path.dirname(__file__), "sample_octopus_agile.json")
    df = load_sample_data(sample_data_path)

    assert validate_agile_prices_data(df) is True

    empty_df = pd.DataFrame()
    assert validate_agile_prices_data(empty_df) is False

    incomplete_df = df.drop(columns=["value_inc_vat"])
    assert validate_agile_prices_data(incomplete_df) is False


def test_validate_agile_prices_data_rejects_backwards_half_hours():
    df = pd.DataFrame(
        [
            {
                "valid_from": pd.Timestamp("2026-08-16 02:00:00", tz="UTC"),
                "valid_to": pd.Timestamp("2026-08-16 01:30:00", tz="UTC"),  # backwards
                "value_exc_vat": 24.63,
                "value_inc_vat": 25.86,
                "loaded_at": pd.Timestamp.now(tz="UTC"),
                "source": SOURCE,
            }
        ]
    )
    assert validate_agile_prices_data(df) is False


def test_land_agile_prices_data(tmp_path, monkeypatch):
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    sample_data_path = os.path.join(os.path.dirname(__file__), "sample_octopus_agile.json")
    df = load_sample_data(sample_data_path)

    land_agile_prices_data(df)

    dt = DeltaTable(table_uri("bronze", "octopus_agile_prices"))
    saved = dt.to_pandas()
    assert len(saved) == len(df)


def test_land_agile_prices_data_is_idempotent_per_date(tmp_path, monkeypatch):
    # Same idempotent partition-scoped overwrite as carbon_intensity.py,
    # for the same reason: Octopus does not revise a published rate, so
    # re-landing a date should replace it, not duplicate it.
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    sample_data_path = os.path.join(os.path.dirname(__file__), "sample_octopus_agile.json")
    df = load_sample_data(sample_data_path)

    land_agile_prices_data(df)
    land_agile_prices_data(df)  # re-run: same data_date, should not double the row count

    dt = DeltaTable(table_uri("bronze", "octopus_agile_prices"))
    assert len(dt.to_pandas()) == len(df)


def test_land_agile_prices_data_does_not_touch_other_dates(tmp_path, monkeypatch):
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    day_one = pd.DataFrame(
        [
            {
                "valid_from": pd.Timestamp("2026-08-15 23:30:00", tz="UTC"),
                "valid_to": pd.Timestamp("2026-08-16 00:00:00", tz="UTC"),
                "value_exc_vat": 10.0,
                "value_inc_vat": 10.5,
                "loaded_at": pd.Timestamp.now(tz="UTC"),
                "source": SOURCE,
                "data_date": "2026-08-15",
            }
        ]
    )
    day_two = pd.DataFrame(
        [
            {
                "valid_from": pd.Timestamp("2026-08-16 00:00:00", tz="UTC"),
                "valid_to": pd.Timestamp("2026-08-16 00:30:00", tz="UTC"),
                "value_exc_vat": 20.0,
                "value_inc_vat": 21.0,
                "loaded_at": pd.Timestamp.now(tz="UTC"),
                "source": SOURCE,
                "data_date": "2026-08-16",
            }
        ]
    )

    land_agile_prices_data(day_one)
    land_agile_prices_data(day_two)

    dt = DeltaTable(table_uri("bronze", "octopus_agile_prices"))
    result = dt.to_pandas()
    assert sorted(result["data_date"].unique()) == ["2026-08-15", "2026-08-16"]
