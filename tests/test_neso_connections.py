"""
This test feeds a saved sample CSV into load_sample_data() the same way
fetch_connections_data() parses a real download (BOM strip, drop the
trailing blank row, stamp as_of_date/loaded_at/source), then checks
validate and land the same way the other extractors' tests do.

resolve_csv_url() and the network calls inside fetch_connections_data()
are not mocked here: this project has no existing precedent for mocking
requests (see test_carbon_intensity.py, test_elexon_generation_by_fuel.py),
and the URL-resolution behaviour this file's own docstring describes was
verified directly against the live CKAN API while writing neso_connections.py,
not left to a test double to assert.
"""

import os

import pandas as pd
from deltalake import DeltaTable

from lakehouse.extractors.neso_connections import (
    MIN_EXPECTED_ROWS,
    land_connections_data,
    validate_connections_data,
)
from lakehouse.io.storage import table_uri


def load_sample_data(file_path: str) -> pd.DataFrame:
    """
    Load the sample CSV the same way fetch_connections_data() parses a
    real download: utf-8-sig to strip the BOM, drop the trailing blank
    row, stamp as_of_date/loaded_at/source.

    Args:
        file_path (str): The path to the sample CSV file.
    """
    df = pd.read_csv(file_path, encoding="utf-8-sig")
    df = df.dropna(how="all").reset_index(drop=True)

    df["as_of_date"] = "2026-08-18"
    df["loaded_at"] = pd.Timestamp.now(tz="UTC")
    df["source"] = "neso_tec_register"
    return df


def test_bom_and_blank_row_are_handled():
    # The raw sample file has a BOM on the header and one fully blank
    # trailing row (see the CSV itself); both must be gone by the time
    # this function returns, the same way a real download is handled.
    sample_data_path = os.path.join(os.path.dirname(__file__), "sample_neso_connections.csv")
    df = load_sample_data(sample_data_path)

    assert "Project Name" in df.columns
    assert "﻿Project Name" not in df.columns
    assert df["Project ID"].isna().sum() == 0
    assert len(df) == 3


def test_validate_connections_data_rejects_missing_columns_and_empty_data():
    sample_data_path = os.path.join(os.path.dirname(__file__), "sample_neso_connections.csv")
    df = load_sample_data(sample_data_path)

    incomplete_df = df.drop(columns=["Project Status"])
    assert validate_connections_data(incomplete_df) is False

    empty_df = pd.DataFrame()
    assert validate_connections_data(empty_df) is False


def test_validate_connections_data_rejects_a_register_this_small():
    # This fixture is 3 rows; the real register has run 2,100-2,200 for
    # months. Below MIN_EXPECTED_ROWS is treated as a truncated or broken
    # download, not a genuine shrinking queue, see the module docstring.
    sample_data_path = os.path.join(os.path.dirname(__file__), "sample_neso_connections.csv")
    df = load_sample_data(sample_data_path)

    assert len(df) < MIN_EXPECTED_ROWS
    assert validate_connections_data(df) is False


def test_land_connections_data(tmp_path, monkeypatch):
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    sample_data_path = os.path.join(os.path.dirname(__file__), "sample_neso_connections.csv")
    df = load_sample_data(sample_data_path)

    land_connections_data(df)

    dt = DeltaTable(table_uri("bronze", "neso_connections"))
    saved_df = dt.to_pandas()
    assert not saved_df.empty
    assert len(saved_df) == len(df)


def test_land_connections_data_does_not_overwrite_previous_landing(tmp_path, monkeypatch):
    # Append-only, like the Elexon sources and for a related reason (see
    # neso_connections.py's module docstring): landing the same as_of_date
    # twice, or a new as_of_date, must leave every earlier landing in the
    # table rather than replacing it.
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    sample_data_path = os.path.join(os.path.dirname(__file__), "sample_neso_connections.csv")

    df_first = load_sample_data(sample_data_path)
    land_connections_data(df_first)

    df_second = load_sample_data(sample_data_path)
    df_second["as_of_date"] = "2026-08-21"
    land_connections_data(df_second)

    dt = DeltaTable(table_uri("bronze", "neso_connections"))
    saved_df = dt.to_pandas()

    assert len(saved_df) == 2 * len(df_first)
    assert sorted(saved_df["as_of_date"].unique()) == ["2026-08-18", "2026-08-21"]
