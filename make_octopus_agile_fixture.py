# Creates a tiny sample Delta table mirroring the bronze Octopus Agile
# schema, so CI has something to build stg/silver/fct_agile_prices and
# mart_tariff_comparison against. Run this LOCALLY using:
#   uv run python make_octopus_agile_fixture.py

from deltalake import write_deltalake

from lakehouse.extractors.octopus_agile import fetch_agile_prices

df = fetch_agile_prices("2026-08-14", "2026-08-15")

write_deltalake("tests/fixtures/octopus_agile_prices", df, mode="overwrite", partition_by=["data_date"])
print(f"wrote {len(df)} rows to tests/fixtures/octopus_agile_prices")
print(df[["valid_from", "value_inc_vat"]].head())
