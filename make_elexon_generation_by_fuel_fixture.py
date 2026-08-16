# Creates a tiny sample Delta table mirroring the bronze Elexon
# generation-by-fuel schema, so CI has something to build the
# revision-resolution models against. Run this LOCALLY from your repo root:
#   uv run python make_elexon_generation_by_fuel_fixture.py
#
# Same reasoning as make_elexon_system_prices_fixture.py: real bronze may
# have zero revisions in it yet, and the point of this fixture is to
# exercise the revision path in CI. Fetches a few real rows for authentic
# columns and dtypes, then deliberately re-lands one of them a second time
# with a different generation figure and a later loaded_at.

import pandas as pd
from deltalake import write_deltalake

from lakehouse.extractors.elexon_generation_by_fuel import fetch_generation_by_fuel_data

df = fetch_generation_by_fuel_data("2026-08-14", "2026-08-14").head(3).reset_index(drop=True)

# Row 0: revise it. A second landing an hour later with different generation.
revised = df.iloc[[0]].copy()
revised["generation"] = revised["generation"] + 500
revised["loaded_at"] = revised["loaded_at"] + pd.Timedelta(hours=1)

# Rows 1 and 2 (different fuel types, same settlement period) are left as
# single landings: most fuel-type/period combinations are never revised,
# and the resolution/audit logic needs to handle that correctly too.
sample = pd.concat([df, revised], ignore_index=True)

write_deltalake("tests/fixtures/elexon_generation_by_fuel", sample, mode="overwrite")
print(f"wrote {len(sample)} rows to tests/fixtures/elexon_generation_by_fuel")
print(sample[["settlementDate", "settlementPeriod", "fuelType", "generation", "loaded_at"]])
