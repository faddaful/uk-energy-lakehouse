"""Schedules: bronze carbon intensity every 30 minutes, Elexon revision sweeps weekly."""

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

bronze_carbon_intensity_regional_mix_job = define_asset_job(
    name="bronze_carbon_intensity_regional_mix_job",
    selection=AssetSelection.assets("bronze_carbon_intensity_regional_mix"),
    description="Fetch and land the latest regional generation mix.",
)

# Same 30-minute cadence as bronze_carbon_intensity, not staggered like the
# weekly Elexon sweeps below: this is a second, independent request to the
# same live endpoint (see carbon_intensity.py's fetch_regional_mix_data
# docstring for why it isn't shared with the intensity fetch), so there is
# no shared resource for the two schedules to contend over.
bronze_carbon_intensity_regional_mix_schedule = ScheduleDefinition(
    job=bronze_carbon_intensity_regional_mix_job,
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

bronze_elexon_generation_by_fuel_job = define_asset_job(
    name="bronze_elexon_generation_by_fuel_job",
    selection=AssetSelection.assets("bronze_elexon_generation_by_fuel"),
    description="Re-download the trailing 28 days of Elexon generation by fuel to catch revisions.",
)

# Offset 30 minutes from the system prices sweep so the two weekly jobs
# don't compete for the same run slot / API rate-limit window.
bronze_elexon_generation_by_fuel_schedule = ScheduleDefinition(
    job=bronze_elexon_generation_by_fuel_job,
    cron_schedule="30 3 * * 0",
    execution_timezone="UTC",
)