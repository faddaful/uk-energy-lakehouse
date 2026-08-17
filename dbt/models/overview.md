{% docs __overview__ %}

# UK Energy Market Lakehouse

Half-hourly GB electricity data from Elexon and the Carbon Intensity API,
ingested to Delta on ADLS Gen2, modelled into a dimensional warehouse with dbt.

The project exists to answer one question properly: how often do published
electricity prices turn out to be wrong, and by how much? Elexon republishes
corrected settlement prices for weeks after the fact. Most pipelines overwrite
and forget. This one keeps every version and reports the difference.

## Layers

- **Bronze** — immutable raw landings, Delta format, every download retained
- **Silver** — typed, deduplicated, with revisions resolved and audited
- **Gold** — star schema: 4 dimensions, 3 facts, 3 marts

## Where to start

- `mart_revision_summary` — the headline: how often published prices change
- `fct_settlement_period` — half-hourly prices with settlement run provenance
- `dim_settlement_period` — why some days have 46 or 50 periods, not 48

{% enddocs %}
