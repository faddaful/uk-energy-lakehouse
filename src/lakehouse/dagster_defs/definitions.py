"""The Dagster entry point: registers everything so `dagster dev` can find it.

Run with:
    uv run dagster dev -m lakehouse.dagster_defs.definitions

Or add to pyproject.toml so plain `uv run dagster dev` works:
    [tool.dagster]
    module_name = "lakehouse.dagster_defs.definitions"
"""

from dagster import Definitions

from lakehouse.dagster_defs.assets import (
    bronze_carbon_intensity,
    bronze_carbon_intensity_regional_mix,
    bronze_elexon_generation_by_fuel,
    bronze_elexon_system_prices,
    bronze_neso_connections,
)
from lakehouse.dagster_defs.checks import (
    bronze_carbon_intensity_regional_mix_schema_check,
    bronze_carbon_intensity_schema_check,
    bronze_elexon_generation_by_fuel_schema_check,
    bronze_elexon_system_prices_schema_check,
    bronze_neso_connections_schema_check,
)
from lakehouse.dagster_defs.dashboard import streamlit_dashboard_job
from lakehouse.dagster_defs.reports import revision_report_job
from lakehouse.dagster_defs.schedules import (
    bronze_carbon_intensity_job,
    bronze_carbon_intensity_regional_mix_job,
    bronze_carbon_intensity_regional_mix_schedule,
    bronze_carbon_intensity_schedule,
    bronze_elexon_generation_by_fuel_job,
    bronze_elexon_generation_by_fuel_schedule,
    bronze_elexon_system_prices_job,
    bronze_elexon_system_prices_schedule,
    bronze_neso_connections_job,
    bronze_neso_connections_schedule,
    revision_report_schedule,
)

defs = Definitions(
    assets=[
        bronze_carbon_intensity,
        bronze_carbon_intensity_regional_mix,
        bronze_elexon_system_prices,
        bronze_elexon_generation_by_fuel,
        bronze_neso_connections,
    ],
    asset_checks=[
        bronze_carbon_intensity_schema_check,
        bronze_carbon_intensity_regional_mix_schema_check,
        bronze_elexon_system_prices_schema_check,
        bronze_elexon_generation_by_fuel_schema_check,
        bronze_neso_connections_schema_check,
    ],
    jobs=[
        bronze_carbon_intensity_job,
        bronze_carbon_intensity_regional_mix_job,
        bronze_elexon_system_prices_job,
        bronze_elexon_generation_by_fuel_job,
        bronze_neso_connections_job,
        streamlit_dashboard_job,
        revision_report_job,
    ],
    schedules=[
        bronze_carbon_intensity_schedule,
        bronze_carbon_intensity_regional_mix_schedule,
        bronze_elexon_system_prices_schedule,
        bronze_elexon_generation_by_fuel_schedule,
        bronze_neso_connections_schedule,
        revision_report_schedule,
    ],
)
