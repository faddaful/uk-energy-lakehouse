"""On-demand Dagster job: lands data/manual/electricity_usage.csv into
bronze. Not scheduled, like dashboard.py's streamlit_dashboard_job: this
data only changes when you hand-edit the CSV after checking your
supplier's app, there is no cadence to run it on, so it sits outside
every cron schedule in schedules.py and is triggered by hand from the
Dagster UI whenever you've actually updated the file.
"""

from dagster import OpExecutionContext, job, op

from lakehouse.extractors.manual_usage import (
    fetch_usage_data,
    land_usage_data,
    validate_usage_data,
)


@op
def land_manual_usage(context: OpExecutionContext) -> None:
    df = fetch_usage_data()
    if not validate_usage_data(df):
        raise ValueError(
            "Invalid or missing data/manual/electricity_usage.csv. "
            "See lakehouse.extractors.manual_usage's module docstring for the format."
        )
    land_usage_data(df)
    context.log.info(f"Landed {len(df)} usage periods from data/manual/electricity_usage.csv")


@job(description="Land data/manual/electricity_usage.csv into bronze, on demand.")
def manual_usage_job() -> None:
    land_manual_usage()
