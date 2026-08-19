"""Schedules: bronze carbon intensity every 30 minutes, Elexon revision
sweeps weekly, NESO connections twice weekly, the revision report
monthly, the public data product every few hours."""

from dagster import AssetSelection, ScheduleDefinition, define_asset_job

from lakehouse.dagster_defs.products import data_product_job
from lakehouse.dagster_defs.reports import revision_report_job

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

bronze_neso_connections_job = define_asset_job(
    name="bronze_neso_connections_job",
    selection=AssetSelection.assets("bronze_neso_connections"),
    description="Fetch and land the current NESO TEC connections register.",
)

# NESO republishes the TEC register twice a week, Tuesdays and Fridays
# (confirmed against the live data portal, not assumed from the plan this
# follows). Landing on the same two days, a few hours after NESO's own
# publish, is what actually catches every real change rather than a
# monthly cadence that would silently miss most of them.
bronze_neso_connections_schedule = ScheduleDefinition(
    job=bronze_neso_connections_job,
    cron_schedule="0 12 * * 2,5",
    execution_timezone="UTC",
)

# 1st of the month, comfortably after every other schedule on this list
# and after `mart_revision_summary` for the month that just closed has
# had days to settle: revision_report_job reports on the PREVIOUS month
# (see revision_report.py's target_month()), so this does not need to
# race the month rollover itself.
revision_report_schedule = ScheduleDefinition(
    job=revision_report_job,
    cron_schedule="0 5 1 * *",
    execution_timezone="UTC",
)

# Every 3 hours, not on the 30-minute cadence the underlying carbon
# intensity forecast actually updates on: each refresh that changes
# anything is a new, permanent git commit (see products.py's own
# docstring for why, deliberately not amended), and a 30-minute cadence
# would add up to 48 commits a day forever for a JSON refresh, not a
# real historical record worth that cost. 3 hours is a compromise
# between "the public product looks reasonably live" and "this repo's
# history doesn't fill up with bot commits."
data_product_schedule = ScheduleDefinition(
    job=data_product_job,
    cron_schedule="0 */3 * * *",
    execution_timezone="UTC",
)

bronze_octopus_agile_prices_job = define_asset_job(
    name="bronze_octopus_agile_prices_job",
    selection=AssetSelection.assets("bronze_octopus_agile_prices"),
    description="Fetch and land the latest Octopus Agile half-hourly unit rates.",
)

# Daily, not half-hourly like the carbon intensity forecast: Octopus
# publishes a whole day's rates in one go, once a day, typically mid-
# afternoon UK time (a day-ahead auction result, not a rolling forecast
# that revises through the day the way carbon intensity's does). 17:00
# UTC is comfortably after that for every time of year (16:00-18:00
# local depending on BST), catching that day's publish the same day
# rather than waiting until tomorrow's run.
bronze_octopus_agile_prices_schedule = ScheduleDefinition(
    job=bronze_octopus_agile_prices_job,
    cron_schedule="0 17 * * *",
    execution_timezone="UTC",
)