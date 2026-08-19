"""
Tests fetch/validate/land for the manual usage CSV extractor. No real
network call to test against (there's no API here, the whole point),
so these write a small CSV to tmp_path and read it back, the same idea
as feeding sample JSON into the other extractors' tests.
"""

import pandas as pd
from deltalake import DeltaTable

from lakehouse.extractors.manual_usage import (
    fetch_usage_data,
    land_usage_data,
    validate_usage_data,
)
from lakehouse.io.storage import table_uri

SAMPLE_CSV = """period_start,period_end,day_kwh,night_kwh,estimated_cost_gbp
2026-07-01,2026-07-31,172.21,107.50,52.38
2026-08-10,2026-08-16,38.29,23.83,11.63
"""


def test_fetch_usage_data_reads_a_real_csv(tmp_path):
    csv_path = tmp_path / "usage.csv"
    csv_path.write_text(SAMPLE_CSV)

    df = fetch_usage_data(str(csv_path))

    assert len(df) == 2
    assert df["source"].unique().tolist() == ["manual_entry"]
    assert "loaded_at" in df.columns
    # Dates normalised to plain YYYY-MM-DD, not whatever format pandas
    # happened to infer from the CSV (see the function's own comment).
    assert df["period_start"].tolist() == ["2026-07-01", "2026-08-10"]


def test_fetch_usage_data_missing_file_returns_empty_df(tmp_path):
    df = fetch_usage_data(str(tmp_path / "does_not_exist.csv"))
    assert df.empty


def test_validate_usage_data():
    df = pd.DataFrame(
        [
            {
                "period_start": "2026-07-01",
                "period_end": "2026-07-31",
                "day_kwh": 172.21,
                "night_kwh": 107.50,
                "estimated_cost_gbp": 52.38,
            }
        ]
    )
    assert validate_usage_data(df) is True

    empty_df = pd.DataFrame()
    assert validate_usage_data(empty_df) is False

    incomplete_df = df.drop(columns=["estimated_cost_gbp"])
    assert validate_usage_data(incomplete_df) is False


def test_validate_usage_data_rejects_period_end_before_start():
    df = pd.DataFrame(
        [
            {
                "period_start": "2026-07-31",
                "period_end": "2026-07-01",  # backwards
                "day_kwh": 10,
                "night_kwh": 5,
                "estimated_cost_gbp": 3.5,
            }
        ]
    )
    assert validate_usage_data(df) is False


def test_land_usage_data(tmp_path, monkeypatch):
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    csv_path = tmp_path / "usage.csv"
    csv_path.write_text(SAMPLE_CSV)
    df = fetch_usage_data(str(csv_path))

    land_usage_data(df)

    dt = DeltaTable(table_uri("bronze", "electricity_usage"))
    saved = dt.to_pandas()
    assert len(saved) == 2


def test_land_usage_data_overwrites_the_whole_table(tmp_path, monkeypatch):
    # Not append-only, unlike Elexon: a hand-edit (fixing a typo,
    # removing a bad row) must actually take effect, not just add to
    # what's already there. See the module's own comment for why.
    monkeypatch.setenv("TARGET", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))

    csv_path = tmp_path / "usage.csv"
    csv_path.write_text(SAMPLE_CSV)
    land_usage_data(fetch_usage_data(str(csv_path)))

    # Now the CSV is hand-edited down to one row.
    csv_path.write_text(
        "period_start,period_end,day_kwh,night_kwh,estimated_cost_gbp\n2026-07-01,2026-07-31,172.21,107.50,52.38\n"
    )
    land_usage_data(fetch_usage_data(str(csv_path)))

    dt = DeltaTable(table_uri("bronze", "electricity_usage"))
    saved = dt.to_pandas()
    assert len(saved) == 1
    assert saved.iloc[0]["period_start"] == "2026-07-01"
