"""Dagster assets: bronze layer landing for carbon intensity and Elexon."""

import datetime

from dagster import MaterializeResult, MetadataValue, asset

from lakehouse.extractors.carbon_intensity import (
    fetch_carbon_intensity_data,
    save_carbon_intensity_data,
    validate_carbon_intensity_data,
)
from lakehouse.extractors.elexon_system_prices import (
    fetch_system_prices_data,
    land_system_prices_data,
    validate_system_prices_data,
)

# Verify this ID in your browser first:
# https://api.carbonintensity.org.uk/regional/regionid/8 -> check the "shortname"
REGION_ID = "8"
REGION_NAME = "West Midlands"

# Elexon settlement runs land within roughly a day of the settlement period
# on this endpoint (see README), but the trailing window is kept wider than
# that as a defensive margin: it costs one extra weekly pass over a few more
# days of history, and it is the difference between catching a revision and
# silently missing one if a run ever lands later than expected.
REVISION_WINDOW_DAYS = 28


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


@asset(
    name="bronze_elexon_system_prices",
    description=(
        "Re-download the trailing settlement dates for Elexon system prices and land "
        "each date as a new bronze file. This is what catches a revision: it re-fetches "
        "dates bronze already has, and because landing here never overwrites, every "
        "value ever observed for a settlement_date + settlement_period stays in bronze "
        "for silver to compare."
    ),
    metadata={"source": MetadataValue.text("elexon_insights_system_prices")},
)
def bronze_elexon_system_prices() -> MaterializeResult:
    end_date = today()
    start_date = (
        datetime.date.fromisoformat(end_date) - datetime.timedelta(days=REVISION_WINDOW_DAYS - 1)
    ).isoformat()

    df = fetch_system_prices_data(start_date=start_date, end_date=end_date)

    if not validate_system_prices_data(df):
        raise ValueError(f"Invalid Elexon system prices data for {start_date} to {end_date}")

    land_system_prices_data(df)

    return MaterializeResult(
        metadata={
            "start_date": MetadataValue.text(start_date),
            "end_date": MetadataValue.text(end_date),
            "rows": MetadataValue.int(len(df)),
        }
    )