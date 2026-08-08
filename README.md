# UK Energy Market Lakehouse

A production-grade data engineering platform that ingests open UK energy data, transforms it through a medallion lakehouse, and serves it to dashboards and alerts. Built in personal time on personal equipment using free, public data.

The project has two aims: to demonstrate modern analytics engineering practice end to end (orchestration, lakehouse modelling, data quality, CI/CD), and to be genuinely useful, surfacing the cleanest and cheapest times to use electricity in my region.

## Status

Phase 1 is complete: a scheduled pipeline lands regional carbon intensity data every 30 minutes, transforms it through tested staging and silver models, and is guarded by continuous integration on every change. Later phases (Elexon market data with settlement revision handling, dimensional modelling, dashboards, and the connections-queue analysis) are in progress. See the roadmap below.

## Architecture

![Architecture](images/uk-energy-lakehouse-architecture-current.png)

Data flows from open APIs through Python extractors into a bronze, silver, gold medallion lakehouse, orchestrated by Dagster, transformed and tested by dbt, and served to dashboards.

## What works today

- Idempotent Python extractor for the Carbon Intensity API, landing raw parquet in a partitioned bronze layer.
- Dagster orchestration: the extractor runs as a scheduled asset every 30 minutes, with a blocking asset check that validates the landed data.
- dbt transformations on DuckDB: a staging model that types and cleans the raw data, and a silver model that deduplicates to one trustworthy row per half hour per region.
- Data quality: dbt tests (not null, uniqueness of a column combination, accepted values, and a range check via dbt-expectations) covering the models.
- Continuous integration: GitHub Actions runs linting, unit tests, and a full dbt build against committed sample fixtures on every push and pull request.

## Tech stack

Python, Dagster (orchestration), dbt with DuckDB (transformation and testing), parquet and DuckDB (storage and query), GitHub Actions (CI), uv (packaging and environments). Azure, Delta Lake, Terraform, Power BI, and Streamlit arrive in later phases.

## Data sources

All free and public.

- Carbon Intensity API: regional generation mix and carbon intensity, half-hourly with a 48-hour forecast. In use now.
- Elexon Insights (BMRS): wholesale prices, generation by fuel type, balancing. Public, no key required. Phase 2.
- NESO Data Portal: demand forecasts, interconnector flows, and the transmission connections queue. Later phases.

## Repository layout

```
src/lakehouse/        Python: extractors, Dagster definitions, alerts
dbt/                  dbt project: staging, silver, (gold to come), tests
tests/                pytest unit tests and CI fixtures
.github/workflows/    CI pipeline
infra/                Terraform (from Phase 2)
apps/                 Streamlit dashboard (from Phase 3)
```

## Running it locally

Prerequisites: Python and [uv](https://docs.astral.sh/uv/).

```bash
# install dependencies and the project itself
uv sync

# fetch some data (creates data/bronze/... on first run)
uv run python -m lakehouse.extractors.carbon_intensity --date 2026-08-06

# run the orchestrator UI (persists run history to .dagster_home/)
make dev
# then open http://localhost:3001

# build and test the transformations
cd dbt
uv run dbt deps
uv run dbt build
```

## Roadmap

- Phase 2: Elexon ingestion, with correct resolution of settlement-run revisions in the silver layer, and migration of storage to Azure Delta Lake.
- Phase 3: dimensional gold models, published dbt docs, a Power BI dashboard, and a personal Streamlit dashboard reachable on mobile.
- Phase 4 and uniqueness layer: NESO demand forecast accuracy, a connections-queue observatory built on the public TEC register, published analysis of price revisions, a small public data product, and a personal dynamic-tariff cost comparison.

## Notes

Built entirely on personal equipment, in personal time, using open public data. Source data remains under each provider's own open data terms.

## License

MIT (see LICENSE).