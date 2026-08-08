"""Dagster assets: bronze layer landing for carbon intensity."""

import datetime

from dagster import MaterializeResult, MetadataValue, asset

from lakehouse.extractors.carbon_intensity import (
    fetch_carbon_intensity_data,
    save_carbon_intensity_data,
    validate_carbon_intensity_data,
)

# Verify this ID in your browser first:
# https://api.carbonintensity.org.uk/regional/regionid/8 -> check the "shortname"
REGION_ID = "8"
REGION_NAME = "West Midlands"


def today() -> str:
    """Today's date as YYYY-MM-DD."""
    return datetime.datetime.now(tz=datetime.UTC).date().isoformat()


@asset(
    name="bronze_carbon_intensity",
    description="Fetch, validate and land today's regional carbon intensity data in bronze.",
    metadata={"source": MetadataValue.text("carbon_intensity_api")},
)
def bronze_carbon_intensity() -> MaterializeResult:
    """No ins/out declared: this asset has no upstream dependencies and
    Dagster infers the output automatically."""
    run_date = today()
    next_date = (datetime.date.fromisoformat(run_date) + datetime.timedelta(days=1)).isoformat()

    df = fetch_carbon_intensity_data(start_date=run_date, end_date=next_date, region=REGION_ID)

    if not validate_carbon_intensity_data(df):
        raise ValueError(f"Invalid carbon intensity data for {run_date}")

    save_carbon_intensity_data(df, date=run_date)

    # Runtime metadata: evaluated on every run, visible per-run in the UI.
    return MaterializeResult(
        metadata={
            "date": MetadataValue.text(run_date),
            "region": MetadataValue.text(REGION_NAME),
            "rows": MetadataValue.int(len(df)),
        }
    )