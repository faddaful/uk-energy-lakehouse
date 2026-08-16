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
    bronze_elexon_system_prices,
)
from lakehouse.dagster_defs.checks import (
    bronze_carbon_intensity_schema_check,
    bronze_elexon_system_prices_schema_check,
)
from lakehouse.dagster_defs.schedules import (
    bronze_carbon_intensity_job,
    bronze_carbon_intensity_schedule,
    bronze_elexon_system_prices_job,
    bronze_elexon_system_prices_schedule,
)

defs = Definitions(
    assets=[bronze_carbon_intensity, bronze_elexon_system_prices],
    asset_checks=[bronze_carbon_intensity_schema_check, bronze_elexon_system_prices_schema_check],
    jobs=[bronze_carbon_intensity_job, bronze_elexon_system_prices_job],
    schedules=[bronze_carbon_intensity_schedule, bronze_elexon_system_prices_schedule],
)