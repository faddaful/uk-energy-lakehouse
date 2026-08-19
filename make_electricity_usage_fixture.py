# Creates a tiny sample Delta table mirroring the bronze electricity
# usage schema, so CI has something to build stg/silver_electricity_usage
# and mart_tariff_comparison against. Run this LOCALLY from your repo
# root:
#   uv run python make_electricity_usage_fixture.py
#
# No live API to fetch a real sample from here, unlike every other
# fixture script (see manual_usage.py: this source is a hand-maintained
# CSV, not a call to anything). Made-up numbers, on purpose, not a copy
# of anyone's real bill: this file is committed to a public repo, and
# real personal usage/cost figures never belong in one (see README's own
# reasoning for why the Streamlit dashboard itself stays off the open
# internet).
#
# The single period below (2026-08-14) deliberately overlaps
# make_octopus_agile_fixture.py's own date, so CI's build actually
# exercises a real, non-empty mart_tariff_comparison join, not just an
# empty table that happens to pass its tests vacuously.

import pandas as pd
from deltalake import write_deltalake

sample = pd.DataFrame(
    [
        {
            "period_start": "2026-08-14",
            "period_end": "2026-08-14",
            "day_kwh": 10.0,
            "night_kwh": 5.0,
            "estimated_cost_gbp": 3.5,
            "loaded_at": pd.Timestamp.now(tz="UTC"),
            "source": "manual_entry",
        }
    ]
)

write_deltalake("tests/fixtures/electricity_usage", sample, mode="overwrite")
print(f"wrote {len(sample)} rows to tests/fixtures/electricity_usage")
print(sample)
