"""Asset checks: today's bronze file exists, is non-empty, has expected columns."""

from pathlib import Path

import pandas as pd
from dagster import AssetCheckResult, asset_check

from lakehouse.dagster_defs.assets import (
    bronze_carbon_intensity,
    bronze_elexon_system_prices,
    today,
)

# These are the expected columns in the parquet file.
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


# Elexon system prices bronze keeps every landing rather than overwriting
# (see README), so a single date directory can hold many files, one per
# past run of this asset. Checking the most recently written one is the
# equivalent of "today's file" for a source that never has just one.
ELEXON_SYSTEM_PRICES_EXPECTED_COLUMNS = {
    "settlementDate",
    "settlementPeriod",
    "systemSellPrice",
    "systemBuyPrice",
    "loaded_at",
    "source",
}
ELEXON_SYSTEM_PRICES_BRONZE_ROOT = Path("data/bronze/elexon_system_prices")


@asset_check(
    asset=bronze_elexon_system_prices,
    blocking=True,
    description="Today's Elexon system prices landing is non-empty and has the expected columns.",
)
def bronze_elexon_system_prices_schema_check() -> AssetCheckResult:
    day_dir = ELEXON_SYSTEM_PRICES_BRONZE_ROOT / f"date={today()}"
    files = sorted(day_dir.glob("*.parquet")) if day_dir.exists() else []

    if not files:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": f"no parquet file found in {day_dir}"},
        )

    # Most recently landed file for today, by file name, which sorts
    # correctly because the loaded_at tag is a zero-padded timestamp.
    df = pd.read_parquet(files[-1])

    if df.empty:
        return AssetCheckResult(passed=False, metadata={"reason": "file is empty"})

    missing = ELEXON_SYSTEM_PRICES_EXPECTED_COLUMNS - set(df.columns)
    if missing:
        return AssetCheckResult(
            passed=False,
            metadata={"missing_columns": sorted(missing)},
        )

    return AssetCheckResult(passed=True, metadata={"rows": len(df)})