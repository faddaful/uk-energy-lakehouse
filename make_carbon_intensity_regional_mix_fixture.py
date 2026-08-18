# Creates a tiny sample Delta table mirroring the bronze regional
# generation mix schema, so CI has something to build against. Run this
# LOCALLY from your repo root:
#   uv run python make_carbon_intensity_regional_mix_fixture.py
# Same idea as make_fixture.py: fetches a few real rows from the live API
# rather than reading local bronze, so this works before you've ever run
# the extractor, and always matches whatever the extractor's schema is
# today.

from deltalake import write_deltalake

from lakehouse.extractors.carbon_intensity import fetch_regional_mix_data

df = fetch_regional_mix_data("2026-08-14", "2026-08-15", "8").head(27).reset_index(drop=True)

write_deltalake("tests/fixtures/carbon_intensity_regional_mix", df, mode="overwrite")
print(f"wrote {len(df)} rows to tests/fixtures/carbon_intensity_regional_mix")
print(df)
