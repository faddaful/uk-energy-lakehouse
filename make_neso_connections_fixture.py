# Creates a tiny sample Delta table mirroring the bronze NESO connections
# schema, so CI has something to build the staging/snapshot/gold models
# against. Run this LOCALLY from your repo root:
#   uv run python make_neso_connections_fixture.py
#
# Same reasoning as make_elexon_generation_by_fuel_fixture.py: the point
# of a fixture is to exercise a real path CI needs covered even when the
# live source doesn't happen to contain that exact scenario right now.
# Fetches a few real rows for authentic columns and dtypes, then
# deliberately crafts the "already built, adding new generation" two-row
# pattern stg_neso_connections.sql's own comment describes: real, but
# rare (one pair out of 2,198 rows in the register as landed on
# 2026-08-18), so CI should not depend on it still being there by chance.

import pandas as pd
from deltalake import write_deltalake

from lakehouse.extractors.neso_connections import fetch_connections_data

df = fetch_connections_data().head(3).reset_index(drop=True)

# Row 0 becomes the duplicate pair: same Project ID, one Built (no
# effective date left to come) and one Scoping (a future effective date
# still pending). Stage is left as whatever row 0 already had (NaN for
# most real rows): reassigning it with a bare `= None` here would undo
# fetch_connections_data()'s own float64 cast and reintroduce the exact
# void-type bug that cast exists to prevent, just one step downstream.
duplicate_pair = pd.concat([df.iloc[[0]], df.iloc[[0]]], ignore_index=True)
duplicate_pair.loc[0, "Project Status"] = "Built"
duplicate_pair.loc[0, "MW Effective From"] = None
duplicate_pair.loc[1, "Project Status"] = "Scoping"
duplicate_pair.loc[1, "MW Effective From"] = "2029-01-31"

# Rows 1 and 2 are left as ordinary, already-unique tranches: most
# projects are never part of a collision, and the dedup logic needs to
# handle that correctly too, not just the pair it exists for.
sample = pd.concat([duplicate_pair, df.iloc[1:]], ignore_index=True)

write_deltalake("tests/fixtures/neso_connections", sample, mode="overwrite", partition_by=["as_of_date"])
print(f"wrote {len(sample)} rows to tests/fixtures/neso_connections")
print(sample[["Project ID", "Stage", "Project Status", "MW Effective From", "as_of_date"]])
