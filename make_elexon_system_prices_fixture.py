# Creates a tiny sample Delta table mirroring the bronze Elexon system
# prices schema, so CI has something to build the revision-resolution
# models against. Run this LOCALLY from your repo root:
#   uv run python make_elexon_system_prices_fixture.py
#
# Unlike make_fixture.py, this does not just grab whatever real bronze
# happens to exist: real bronze may have zero revisions in it yet (a fresh
# backfill lands each date once), and the whole point of this fixture is
# to exercise the revision path in CI. So this fetches a few real rows for
# authentic columns and dtypes, then deliberately re-lands one of them a
# second time with a different price and a later loaded_at, synthesising
# the one scenario that matters most: the same settlement_date +
# settlement_period observed twice, with the second value replacing the
# first.

import pandas as pd
from deltalake import write_deltalake

from lakehouse.extractors.elexon_system_prices import fetch_system_prices_data

df = fetch_system_prices_data("2026-08-14", "2026-08-14").head(3).reset_index(drop=True)

# Row 0: revise it. A second landing an hour later with a different price.
revised = df.iloc[[0]].copy()
revised["systemSellPrice"] = revised["systemSellPrice"] + 5.0
revised["systemBuyPrice"] = revised["systemBuyPrice"] + 5.0
revised["loaded_at"] = revised["loaded_at"] + pd.Timedelta(hours=1)

# Rows 1 and 2 are left as single landings: most settlement periods are
# never revised, and the resolution/audit logic needs to handle that
# correctly too, not just the revised case.
sample = pd.concat([df, revised], ignore_index=True)

write_deltalake("tests/fixtures/elexon_system_prices", sample, mode="overwrite")
print(f"wrote {len(sample)} rows to tests/fixtures/elexon_system_prices")
print(sample[["settlementDate", "settlementPeriod", "systemSellPrice", "loaded_at"]])
