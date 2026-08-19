"""
Turn two gold marts into the public data product this project ships:
`api/greenest_hours_next_48h.json` and `api/latest_price_anomalies.json`,
committed and pushed (see lakehouse.dagster_defs.products) so that, once
GitHub Pages serves them (docs.yml copies api/*.json alongside the dbt
docs site), a plain `curl` against a stable URL is the whole API. No
server to run, no auth, no uptime to own.

Same fetch/render/write split as revision_report.py, for the same
testability reason. Reads the same DuckDB file the Streamlit dashboard
and the revision report both do, read-only, no separate export
warehouse.

Both JSON payloads carry schema_version and generated_at: a public
contract with no version marker cannot ever change shape without
silently breaking whoever is consuming it, and a data product that
cannot say how stale it is has failed at its main job, the same
reasoning mart_greenest_hours_next_48h.sql's own generated_at column
already carries.
"""
import datetime
import json
import logging
import os
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]

# Same convention dashboard.py and revision_report.py use to pick the
# right DuckDB file for TARGET=local|azure.
DB_NAME = "lakehouse_azure.duckdb" if os.environ.get("TARGET") == "azure" else "lakehouse.duckdb"
DB_PATH = REPO_ROOT / "data" / DB_NAME

API_DIR = REPO_ROOT / "api"

SCHEMA_VERSION = 1

# Matches mart_greenest_hours_next_48h.sql's own {% set home_region_id %}.
HOME_REGION_ID = 8
HOME_REGION_NAME = "West Midlands"

# mart_latest_price_anomalies.sql's own 30-day filter, restated here so
# the JSON payload can say explicitly what window it covers rather than
# leaving a consumer to guess from the data alone.
ANOMALY_WINDOW_DAYS = 30


def _clean(value: Any) -> Any:
    """
    NaN/NaT/pd.NA to None, a Timestamp to a UTC ISO 8601 string, a numpy
    scalar to the equivalent native Python type, everything else
    unchanged. Needed because none of DuckDB's fetchdf() output is
    directly JSON-safe: pandas' NaN survives json.dumps() as the bare
    token NaN (not valid JSON; every browser's JSON.parse rejects it),
    and json.dumps() raises outright on a bare numpy.int32/int64
    ("Object of type int32 is not JSON serializable"). Both confirmed by
    actually running json.dumps() against a real row pulled from gold,
    not assumed from the pandas/numpy docs.

    Timestamp columns need two different fixes, not one: checked against
    real rows before writing this, a plain TIMESTAMP column like
    half_hour_start_utc comes back tz-naive despite its own name (needs
    tz_localize), while current_timestamp-derived columns like
    generated_at come back tz-aware, in DuckDB's session-local zone, not
    UTC (needs tz_convert instead). Calling the wrong one raises --
    tz_localize on an already-aware Timestamp, or tz_convert on a naive
    one, are both real errors, not just theoretical ones.

    Args:
        value (Any): One cell from a gold mart DataFrame.
    """
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        value = value.tz_localize("UTC") if value.tzinfo is None else value.tz_convert("UTC")
        # Z suffix, not +00:00: the more common convention for a public
        # JSON API to publish UTC timestamps in.
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if pd.isna(value):
        return None
    if isinstance(value, np.generic):
        return value.item()
    return value


def _records(df: pd.DataFrame) -> list[dict]:
    """Every row of df as a plain dict, cleaned with _clean(). One place
    for the NaN/Timestamp handling every export function needs, rather
    than repeated in each one."""
    return [{col: _clean(row[col]) for col in df.columns} for _, row in df.iterrows()]


def fetch_greenest_hours(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    mart_greenest_hours_next_48h, minus region_short_name (already
    published once in the payload's top-level "region" object, see
    render_greenest_hours(); repeating it on every row is redundant, not
    more informative) and with generated_at renamed to gold_built_at.
    The rename matters, not just cosmetic: this job does not trigger a
    dbt build (see products.py's own docstring), so "when gold was last
    built" and "when this JSON was generated" are two genuinely
    different moments, and calling both of them generated_at in the same
    payload would hide that gap instead of surfacing it.
    """
    return con.execute("""
        select
            half_hour_start_utc,
            intensity_forecast_gco2_per_kwh,
            system_sell_price_gbp_per_mwh,
            greenness_rank,
            cheapness_rank,
            generated_at as gold_built_at
        from main_gold.mart_greenest_hours_next_48h
        order by half_hour_start_utc
    """).fetchdf()


def fetch_price_anomalies(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """mart_latest_price_anomalies, already filtered to the last 30 days
    and ordered by the model itself, with generated_at renamed to
    gold_built_at, see fetch_greenest_hours()'s own comment for why."""
    return con.execute("""
        select
            settlement_period_start_utc,
            system_sell_price_gbp_per_mwh,
            price_change_gbp_per_mwh,
            anomaly_type,
            generated_at as gold_built_at
        from main_gold.mart_latest_price_anomalies
        order by settlement_period_start_utc desc
    """).fetchdf()


def render_greenest_hours(df: pd.DataFrame) -> dict:
    """
    Args:
        df (pd.DataFrame): From fetch_greenest_hours().
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "region": {"region_id": HOME_REGION_ID, "region_short_name": HOME_REGION_NAME},
        "hours": _records(df),
    }


def render_price_anomalies(df: pd.DataFrame) -> dict:
    """
    Args:
        df (pd.DataFrame): From fetch_price_anomalies().
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "window_days": ANOMALY_WINDOW_DAYS,
        "anomalies": _records(df),
    }


def write_json(payload: dict, filename: str, api_dir: Path | None = None) -> Path:
    """
    Write one payload to api/<filename>, pretty-printed: these files are
    meant to be opened directly in a browser as much as fetched by code,
    and a single unbroken line of JSON is unreadable either way.

    api_dir defaults to None, not the API_DIR module constant directly,
    the same reason write_report() in revision_report.py does: a default
    parameter value is bound once, at def time, so a test that
    monkeypatches api_export.API_DIR after import would be silently
    ignored if this took the constant itself as the default. Resolving
    it inside the function body picks up whatever it is set to when
    this actually runs.

    Args:
        payload (dict): From render_greenest_hours() or render_price_anomalies().
        filename (str): e.g. "greenest_hours_next_48h.json".
        api_dir (Path | None): Where the API's files live. Defaults to API_DIR.
    """
    if api_dir is None:
        api_dir = API_DIR
    api_dir.mkdir(parents=True, exist_ok=True)
    path = api_dir / filename
    path.write_text(json.dumps(payload, indent=2) + "\n")
    return path


def generate_products() -> list[Path]:
    """Fetch, render and write both JSON products. Returns the paths written."""
    con = duckdb.connect(str(DB_PATH), read_only=True)
    try:
        greenest_hours = render_greenest_hours(fetch_greenest_hours(con))
        price_anomalies = render_price_anomalies(fetch_price_anomalies(con))
    finally:
        con.close()

    paths = [
        write_json(greenest_hours, "greenest_hours_next_48h.json"),
        write_json(price_anomalies, "latest_price_anomalies.json"),
    ]
    for path in paths:
        logger.info(f"Wrote {path}")
    return paths


def main() -> None:
    generate_products()


if __name__ == "__main__":
    main()
