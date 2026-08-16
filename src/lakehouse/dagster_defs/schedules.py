"""Schedules: bronze carbon intensity every 30 minutes, Elexon system prices weekly."""

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

bronze_carbon_intensity_job = define_asset_job(
    name="bronze_carbon_intensity_job",
    selection=AssetSelection.assets("bronze_carbon_intensity"),
    description="Fetch and land the latest regional carbon intensity data.",
)

bronze_carbon_intensity_schedule = ScheduleDefinition(
    job=bronze_carbon_intensity_job,
    cron_schedule="*/30 * * * *",
    execution_timezone="UTC",
)

bronze_elexon_system_prices_job = define_asset_job(
    name="bronze_elexon_system_prices_job",
    selection=AssetSelection.assets("bronze_elexon_system_prices"),
    description="Re-download the trailing 28 days of Elexon system prices to catch revisions.",
)

# Weekly, not daily: this is the revision sweep, not the mechanism that keeps
# bronze fresh day to day (the trailing window covers "day to day" too, since
# it always includes today). Sunday early morning, off-peak for both this
# laptop and Elexon's API.
bronze_elexon_system_prices_schedule = ScheduleDefinition(
    job=bronze_elexon_system_prices_job,
    cron_schedule="0 3 * * 0",
    execution_timezone="UTC",
)