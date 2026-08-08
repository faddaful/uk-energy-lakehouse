# Creates a tiny sample parquet mirroring the bronze carbon intensity schema,
# so CI has something to build against. Run this LOCALLY from your repo root:
#   uv run python make_fixture.py
# It reads a few real rows from the bronze and writes them to tests/fixtures/.

import glob
import os

import pandas as pd

# grab whatever bronze parquet you already have
files = sorted(glob.glob("data/bronze/carbon_intensity/*/*.parquet"))
assert files, "no bronze parquet found; run your extractor first"
df = pd.read_parquet(files[-1]).head(3)   # 3 rows is plenty

os.makedirs("tests/fixtures/carbon_intensity", exist_ok=True)
df.to_parquet("tests/fixtures/carbon_intensity/sample.parquet", index=False)
print(f"wrote {len(df)} rows to tests/fixtures/carbon_intensity/sample.parquet")
print(df)