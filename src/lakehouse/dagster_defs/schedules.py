"""Schedule: run the bronze carbon intensity asset every 30 minutes."""

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