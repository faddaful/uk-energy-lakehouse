# Creates a tiny sample Delta table mirroring the bronze carbon intensity
# schema, so CI has something to build against. Run this LOCALLY from your
# repo root:
#   uv run python make_fixture.py
# Fetches a few real rows from the live API rather than reading local
# bronze, so this works even before you have ever run the extractor, and
# so the fixture always matches whatever the extractor's schema is today.

from deltalake import write_deltalake

from lakehouse.extractors.carbon_intensity import fetch_carbon_intensity_data

df = fetch_carbon_intensity_data("2026-08-14", "2026-08-15", "8").head(3).reset_index(drop=True)

write_deltalake("tests/fixtures/carbon_intensity", df, mode="overwrite")
print(f"wrote {len(df)} rows to tests/fixtures/carbon_intensity")
print(df)
