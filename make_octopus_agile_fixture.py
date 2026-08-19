# Creates a tiny sample Delta table mirroring the bronze Octopus Agile
# schema, so CI has something to build stg/silver/fct_agile_prices and
# mart_tariff_comparison against. Run this LOCALLY from your repo root:
#   uv run python make_octopus_agile_fixture.py
#
# One real day (48 half hours), not a handful of rows: the day/night
# rate_band classification and mart_tariff_comparison's join both need a
# genuine spread across the day to mean anything, not just enough rows
# to satisfy a not_null test.

from deltalake import write_deltalake

from lakehouse.extractors.octopus_agile import fetch_agile_prices

df = fetch_agile_prices("2026-08-14", "2026-08-15")

write_deltalake("tests/fixtures/octopus_agile_prices", df, mode="overwrite", partition_by=["data_date"])
print(f"wrote {len(df)} rows to tests/fixtures/octopus_agile_prices")
print(df[["valid_from", "value_inc_vat"]].head())
