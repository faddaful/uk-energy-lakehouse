"""
Feeds synthetic gold-mart rows into a throwaway DuckDB file (same idea
as test_revision_report.py) and checks _clean()'s real dtype gotchas
(tz-naive vs tz-aware Timestamps, numpy scalars, pd.NA) directly, then
the full fetch-render-write pipeline end to end.
"""

import json
import re

import duckdb
import numpy as np
import pandas as pd
import pytest

from lakehouse.products.api_export import (
    _clean,
    generate_products,
    render_greenest_hours,
    render_price_anomalies,
    write_json,
)


def test_clean_converts_a_tz_naive_timestamp_to_a_utc_iso_string():
    # half_hour_start_utc comes back from DuckDB tz-naive despite its own
    # name (see the function's own comment): tz_localize, not tz_convert.
    naive = pd.Timestamp("2026-08-19 13:00:00")
    assert _clean(naive) == "2026-08-19T13:00:00Z"


def test_clean_converts_a_tz_aware_timestamp_to_utc():
    # current_timestamp-derived columns come back tz-aware, in whatever
    # zone the local session is in, not UTC: tz_convert, not tz_localize.
    aware = pd.Timestamp("2026-08-19 13:00:00", tz="Europe/London")
    assert _clean(aware) == "2026-08-19T12:00:00Z"  # BST is UTC+1

    aware_winter = pd.Timestamp("2026-01-19 13:00:00", tz="Europe/London")
    assert _clean(aware_winter) == "2026-01-19T13:00:00Z"  # GMT is UTC+0


def test_clean_handles_missing_values_of_every_flavour():
    assert _clean(pd.NaT) is None
    assert _clean(np.float64("nan")) is None
    assert _clean(pd.NA) is None
    assert _clean(None) is None


def test_clean_converts_numpy_scalars_to_native_python_types():
    # json.dumps raises outright on a bare numpy scalar; .item() is what
    # actually fixes it, confirmed against a real gold-mart row before
    # writing the function this way (see its own comment).
    assert _clean(np.int32(72)) == 72
    assert isinstance(_clean(np.int32(72)), int)
    assert _clean(np.int64(3)) == 3
    assert _clean(np.float64(112.5)) == 112.5


def test_clean_leaves_ordinary_values_alone():
    assert _clean("large_swing") == "large_swing"
    assert _clean(8) == 8


def test_render_greenest_hours_produces_a_json_serialisable_payload():
    df = pd.DataFrame(
        [
            {
                "half_hour_start_utc": pd.Timestamp("2026-08-19 13:00:00"),
                "intensity_forecast_gco2_per_kwh": np.int32(72),
                "system_sell_price_gbp_per_mwh": np.float64("nan"),
                "greenness_rank": np.int64(1),
                "cheapness_rank": pd.NA,
                "gold_built_at": pd.Timestamp("2026-08-19 12:00:00", tz="UTC"),
            }
        ]
    )
    payload = render_greenest_hours(df)

    # The real point of this test: json.dumps must not raise. It would,
    # on the raw numpy/pd.NA values above, before _clean() runs.
    encoded = json.dumps(payload)
    decoded = json.loads(encoded)

    assert decoded["schema_version"] == 1
    assert decoded["region"] == {"region_id": 8, "region_short_name": "West Midlands"}
    hour = decoded["hours"][0]
    assert hour["half_hour_start_utc"] == "2026-08-19T13:00:00Z"
    assert hour["intensity_forecast_gco2_per_kwh"] == 72
    assert hour["system_sell_price_gbp_per_mwh"] is None
    assert hour["cheapness_rank"] is None


def test_render_price_anomalies_produces_a_json_serialisable_payload():
    df = pd.DataFrame(
        [
            {
                "settlement_period_start_utc": pd.Timestamp("2026-08-19 09:00:00"),
                "system_sell_price_gbp_per_mwh": np.float64(-12.3),
                "price_change_gbp_per_mwh": np.float64(-65.2),
                "anomaly_type": "large_swing",
                "gold_built_at": pd.Timestamp("2026-08-19 12:00:00", tz="UTC"),
            }
        ]
    )
    payload = render_price_anomalies(df)
    decoded = json.loads(json.dumps(payload))

    assert decoded["window_days"] == 30
    assert decoded["anomalies"][0]["anomaly_type"] == "large_swing"
    assert decoded["anomalies"][0]["system_sell_price_gbp_per_mwh"] == pytest.approx(-12.3)


def test_write_json_writes_pretty_printed_valid_json(tmp_path):
    path = write_json({"a": 1}, "test.json", api_dir=tmp_path)

    assert path == tmp_path / "test.json"
    assert json.loads(path.read_text()) == {"a": 1}
    assert "\n" in path.read_text()  # pretty-printed, not one unbroken line


def test_generate_products_end_to_end(tmp_path, monkeypatch):
    db_path = tmp_path / "test.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("create schema main_gold")
    con.execute("""
        create table main_gold.mart_greenest_hours_next_48h as
        select
            timestamp '2026-08-19 13:00:00' as half_hour_start_utc,
            'W Midlands' as region_short_name,
            72 as intensity_forecast_gco2_per_kwh,
            cast(null as double) as system_sell_price_gbp_per_mwh,
            1 as greenness_rank,
            cast(null as integer) as cheapness_rank,
            current_timestamp as generated_at
    """)
    con.execute("""
        create table main_gold.mart_latest_price_anomalies as
        select
            timestamp '2026-08-19 09:00:00' as settlement_period_start_utc,
            112.6 as system_sell_price_gbp_per_mwh,
            -75.4 as price_change_gbp_per_mwh,
            'large_swing' as anomaly_type,
            current_timestamp as generated_at
    """)
    con.close()

    from lakehouse.products import api_export

    monkeypatch.setattr(api_export, "DB_PATH", str(db_path))
    monkeypatch.setattr(api_export, "API_DIR", tmp_path / "api")

    paths = generate_products()

    assert len(paths) == 2
    for path in paths:
        payload = json.loads(path.read_text())  # must parse cleanly
        assert payload["schema_version"] == 1
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", payload["generated_at"])

    greenest = json.loads((tmp_path / "api" / "greenest_hours_next_48h.json").read_text())
    assert greenest["hours"][0]["intensity_forecast_gco2_per_kwh"] == 72

    anomalies = json.loads((tmp_path / "api" / "latest_price_anomalies.json").read_text())
    assert anomalies["anomalies"][0]["anomaly_type"] == "large_swing"
