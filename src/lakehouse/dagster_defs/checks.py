"""Asset check: today's bronze file exists, is non-empty, has expected columns."""

from pathlib import Path

import pandas as pd
from dagster import AssetCheckResult, asset_check

from lakehouse.dagster_defs.assets import bronze_carbon_intensity, today

# These are the expected columns in the parquet file. If you add or remove columns in your extractor, update this set to match.
EXPECTED_COLUMNS = {
    "from",
    "to",
    "intensity_actual",
    "intensity_forecast",
    "region_id",
    "loaded_at",
    "source",
}
# The root directory where the bronze parquet files are stored. This should match the path used in your extractor's save function.
BRONZE_ROOT = Path("data/bronze/carbon_intensity")


@asset_check(
    asset=bronze_carbon_intensity,
    blocking=True,  # a failure blocks downstream assets from materialising
    description="Bronze file for today is non-empty and has the expected columns.",
)
def bronze_carbon_intensity_schema_check() -> AssetCheckResult:
    day_dir = BRONZE_ROOT / f"date={today()}"
    files = sorted(day_dir.glob("*.parquet")) if day_dir.exists() else []

    if not files:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": f"no parquet file found in {day_dir}"},
        )

    df = pd.read_parquet(files[-1])

    if df.empty:
        return AssetCheckResult(passed=False, metadata={"reason": "file is empty"})

    missing = EXPECTED_COLUMNS - set(df.columns)
    if missing:
        return AssetCheckResult(
            passed=False,
            metadata={"missing_columns": sorted(missing)},
        )

    return AssetCheckResult(passed=True, metadata={"rows": len(df)})