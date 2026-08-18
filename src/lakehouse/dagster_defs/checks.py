"""Asset checks: today's bronze landing is non-empty and has the expected columns."""

from dagster import AssetCheckResult, asset_check
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from lakehouse.dagster_defs.assets import (
    bronze_carbon_intensity,
    bronze_carbon_intensity_regional_mix,
    bronze_elexon_generation_by_fuel,
    bronze_elexon_system_prices,
    bronze_neso_connections,
    today,
)
from lakehouse.io.storage import storage_options, table_uri

# These are the expected columns in the Delta table.
CARBON_INTENSITY_EXPECTED_COLUMNS = {
    "data_date",
    "from",
    "to",
    "intensity_actual",
    "intensity_forecast",
    "intensity_index",
    "region_id",
    "loaded_at",
    "source",
}


@asset_check(
    asset=bronze_carbon_intensity,
    blocking=True,  # a failure blocks downstream assets from materialising
    description="Today's bronze landing is non-empty and has the expected columns.",
)
def bronze_carbon_intensity_schema_check() -> AssetCheckResult:
    uri = table_uri("bronze", "carbon_intensity")
    try:
        dt = DeltaTable(uri, storage_options=storage_options())
    except TableNotFoundError:
        return AssetCheckResult(passed=False, metadata={"reason": f"no Delta table found at {uri}"})

    # Read only today's partition, not the whole table: the whole point
    # of partition_by=["data_date"] on the write side.
    df = dt.to_pandas(partitions=[("data_date", "=", today())])

    if df.empty:
        return AssetCheckResult(passed=False, metadata={"reason": f"no rows for data_date={today()}"})

    missing = CARBON_INTENSITY_EXPECTED_COLUMNS - set(df.columns)
    if missing:
        return AssetCheckResult(
            passed=False,
            metadata={"missing_columns": sorted(missing)},
        )

    return AssetCheckResult(passed=True, metadata={"rows": len(df), "table_version": dt.version()})


CARBON_INTENSITY_REGIONAL_MIX_EXPECTED_COLUMNS = {
    "data_date",
    "from",
    "to",
    "region_id",
    "fuel",
    "perc",
    "loaded_at",
    "source",
}


@asset_check(
    asset=bronze_carbon_intensity_regional_mix,
    blocking=True,
    description="Today's bronze regional mix landing is non-empty and has the expected columns.",
)
def bronze_carbon_intensity_regional_mix_schema_check() -> AssetCheckResult:
    uri = table_uri("bronze", "carbon_intensity_regional_mix")
    try:
        dt = DeltaTable(uri, storage_options=storage_options())
    except TableNotFoundError:
        return AssetCheckResult(passed=False, metadata={"reason": f"no Delta table found at {uri}"})

    df = dt.to_pandas(partitions=[("data_date", "=", today())])

    if df.empty:
        return AssetCheckResult(passed=False, metadata={"reason": f"no rows for data_date={today()}"})

    missing = CARBON_INTENSITY_REGIONAL_MIX_EXPECTED_COLUMNS - set(df.columns)
    if missing:
        return AssetCheckResult(
            passed=False,
            metadata={"missing_columns": sorted(missing)},
        )

    return AssetCheckResult(passed=True, metadata={"rows": len(df), "table_version": dt.version()})


# Elexon system prices bronze is append-only (see README): every landing
# for a settlement_date is still in the table, not just the latest one.
# Filtering to today's partition and checking it is non-empty is enough:
# it does not need to be exactly one landing's worth of rows.
ELEXON_SYSTEM_PRICES_EXPECTED_COLUMNS = {
    "settlementDate",
    "settlementPeriod",
    "systemSellPrice",
    "systemBuyPrice",
    "loaded_at",
    "source",
}


@asset_check(
    asset=bronze_elexon_system_prices,
    blocking=True,
    description="Today's Elexon system prices landing is non-empty and has the expected columns.",
)
def bronze_elexon_system_prices_schema_check() -> AssetCheckResult:
    uri = table_uri("bronze", "elexon_system_prices")
    try:
        dt = DeltaTable(uri, storage_options=storage_options())
    except TableNotFoundError:
        return AssetCheckResult(passed=False, metadata={"reason": f"no Delta table found at {uri}"})

    df = dt.to_pandas(partitions=[("settlementDate", "=", today())])

    if df.empty:
        return AssetCheckResult(passed=False, metadata={"reason": f"no rows for settlementDate={today()}"})

    missing = ELEXON_SYSTEM_PRICES_EXPECTED_COLUMNS - set(df.columns)
    if missing:
        return AssetCheckResult(
            passed=False,
            metadata={"missing_columns": sorted(missing)},
        )

    return AssetCheckResult(passed=True, metadata={"rows": len(df), "table_version": dt.version()})


# Same append-only bronze, same reason a partition filter replaces a
# "most recent file" lookup here too.
ELEXON_GENERATION_BY_FUEL_EXPECTED_COLUMNS = {
    "settlementDate",
    "settlementPeriod",
    "fuelType",
    "generation",
    "loaded_at",
    "source",
}


@asset_check(
    asset=bronze_elexon_generation_by_fuel,
    blocking=True,
    description="Today's Elexon generation-by-fuel landing is non-empty and has the expected columns.",
)
def bronze_elexon_generation_by_fuel_schema_check() -> AssetCheckResult:
    uri = table_uri("bronze", "elexon_generation_by_fuel")
    try:
        dt = DeltaTable(uri, storage_options=storage_options())
    except TableNotFoundError:
        return AssetCheckResult(passed=False, metadata={"reason": f"no Delta table found at {uri}"})

    df = dt.to_pandas(partitions=[("settlementDate", "=", today())])

    if df.empty:
        return AssetCheckResult(passed=False, metadata={"reason": f"no rows for settlementDate={today()}"})

    missing = ELEXON_GENERATION_BY_FUEL_EXPECTED_COLUMNS - set(df.columns)
    if missing:
        return AssetCheckResult(
            passed=False,
            metadata={"missing_columns": sorted(missing)},
        )

    return AssetCheckResult(passed=True, metadata={"rows": len(df), "table_version": dt.version()})


# Raw CSV headers, same as neso_connections.py's EXPECTED_COLUMNS: bronze
# keeps NESO's own column names verbatim, see that module's docstring.
NESO_CONNECTIONS_EXPECTED_COLUMNS = {
    "Project Name",
    "Customer Name",
    "Connection Site",
    "Project Status",
    "Agreement Type",
    "HOST TO",
    "Project ID",
    "as_of_date",
    "loaded_at",
    "source",
}

# A sanity floor, not a strict count, same reasoning and same number as
# neso_connections.py's MIN_EXPECTED_ROWS: a genuine truncated download
# would still pass a "non-empty" check, so that alone is not enough here.
NESO_CONNECTIONS_MIN_ROWS = 500


@asset_check(
    asset=bronze_neso_connections,
    blocking=True,
    description="Today's NESO connections landing is non-empty, has the expected columns, and isn't suspiciously small.",
)
def bronze_neso_connections_schema_check() -> AssetCheckResult:
    uri = table_uri("bronze", "neso_connections")
    try:
        dt = DeltaTable(uri, storage_options=storage_options())
    except TableNotFoundError:
        return AssetCheckResult(passed=False, metadata={"reason": f"no Delta table found at {uri}"})

    df = dt.to_pandas(partitions=[("as_of_date", "=", today())])

    if df.empty:
        return AssetCheckResult(passed=False, metadata={"reason": f"no rows for as_of_date={today()}"})

    missing = NESO_CONNECTIONS_EXPECTED_COLUMNS - set(df.columns)
    if missing:
        return AssetCheckResult(
            passed=False,
            metadata={"missing_columns": sorted(missing)},
        )

    if len(df) < NESO_CONNECTIONS_MIN_ROWS:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": f"only {len(df)} rows, expected at least {NESO_CONNECTIONS_MIN_ROWS}"},
        )

    return AssetCheckResult(passed=True, metadata={"rows": len(df), "table_version": dt.version()})
