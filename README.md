# UK Energy Market Lakehouse

A production-grade data engineering platform that ingests open UK energy data, transforms it through a medallion lakehouse, and serves it to dashboards and alerts. Built in personal time on personal equipment using free, public data.

The project has two aims: to demonstrate modern analytics engineering practice end to end (orchestration, lakehouse modelling, data quality, CI/CD), and to be genuinely useful, surfacing the cleanest and cheapest times to use electricity in my region.

## Status

Phase 1 is complete: a scheduled pipeline lands regional carbon intensity data every 30 minutes, transforms it through tested staging and silver models, and is guarded by continuous integration on every change.

Phase 2 is in progress. Elexon system prices ingestion and revision resolution are done: bronze lands every settlement date's prices, a weekly Dagster job re-downloads the trailing 28 days to catch anything that changes, and silver resolves each settlement period to one trustworthy value with a full audit trail of every change observed. Generation by fuel type is ingested but not yet revision-resolved, and storage is still local, not Azure. See the roadmap below.

## Architecture

![Architecture](images/uk-energy-lakehouse-architecture-current.png)

Data flows from open APIs through Python extractors into a bronze, silver, gold medallion lakehouse, orchestrated by Dagster, transformed and tested by dbt, and served to dashboards.

## What works today

- Idempotent Python extractor for the Carbon Intensity API, landing raw parquet in a partitioned bronze layer.
- Python extractors for Elexon system prices and generation by fuel type, at settlement-period grain, correctly handling the 46/50-period clock-change days (computed from the timezone database, not hardcoded). No API key needed; both are good citizens of the public API with a descriptive User-Agent, rate limiting, and backoff on errors.
- Elexon system prices revision resolution: bronze is append-only (see [Why bronze never overwrites for Elexon](#why-bronze-never-overwrites-for-elexon) below), a weekly Dagster job re-downloads the trailing 28 days, and an incremental silver model resolves each settlement_date + settlement_period to its latest-seen value, with a full audit table logging every change ever observed.
- Dagster orchestration: carbon intensity runs as a scheduled asset every 30 minutes; Elexon system prices re-download weekly. Both have a blocking asset check that validates the landed data.
- dbt transformations on DuckDB: staging models that type and clean the raw data, and silver models that resolve each source to one trustworthy row per natural key.
- Data quality: dbt tests (not null, uniqueness of a column combination, accepted values, range checks via dbt-expectations, and custom tests asserting resolved values trace back to a real bronze row and that revision counts stay sane) covering the models. A dedicated pytest also exercises the revision-resolution SQL idiom directly against synthetic data, independent of dbt.
- Continuous integration: GitHub Actions runs linting, unit tests, and a full dbt build against committed sample fixtures (including a synthetic revision scenario) on every push and pull request.

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

## Why bronze never overwrites for Elexon

Carbon Intensity's bronze extractor overwrites the same file when re-run for a date it has already landed: that data source has no concept of revision, so the latest fetch is always the correct fetch, and idempotent overwrite is the simpler, correct design.

Elexon system prices are different. Elexon can revise a price after it is first published, so a value fetched today is not guaranteed to still be the current value next week. If bronze overwrote on every re-download, the earlier value would be gone before anything downstream could notice it had changed. So bronze here is append-only: the file name embeds the `loaded_at` of the landing (`elexon_system_prices_{date}_{loaded_at}.parquet`), and a weekly Dagster job re-downloads the trailing 28 days specifically to catch anything that changed since the last landing. Nothing is ever deleted from bronze; silver is where a single trustworthy value gets picked out of however many landings exist for a given settlement_date + settlement_period.

**One correction to how this is usually described.** The standard explanation of Elexon settlement is that a price is published as an early estimate and then corrected over a formal reconciliation calendar spanning weeks to months (II → SF → R1 → R2 → R3 → RF, the last one landing roughly 14 months after the settlement date). Before building the resolution logic, this was checked directly against the live API rather than assumed: `/balancing/settlement/system-prices/{date}` was queried for settlement dates 1 day, 2.5 months, 7 months, and 15 months old, and in every case `createdDateTime` sat within about a day of the original settlement date, with no trace of the later reconciliation runs touching it. That endpoint also exposes no settlement-run-type field at all, so there is nothing to rank a "most authoritative run" against, only `loaded_at`.

The practical upshot: this specific endpoint appears to reflect only the fast II → SF settling (within roughly a day of the settlement period), not the multi-month reconciliation cycle that applies to metered volumes elsewhere in BSC settlement. The design above (bronze keeps everything, weekly re-download, resolve on latest `loaded_at`) is still the right architecture regardless — it costs one extra weekly pass over a bit more history, and it is what would catch a later revision if this endpoint's behaviour is ever wrong or changes. But expect `silver__price_revisions` to be sparse or empty most weeks. That is the healthy state for this data source, not a sign the pipeline is broken.

## Running it locally

Prerequisites: Python and [uv](https://docs.astral.sh/uv/).

```bash
# install dependencies and the project itself
uv sync

# fetch some data (creates data/bronze/... on first run)
uv run python -m lakehouse.extractors.carbon_intensity --date 2026-08-06
uv run python -m lakehouse.extractors.elexon_system_prices --start-date 2026-05-18 --end-date 2026-08-15
uv run python -m lakehouse.extractors.elexon_generation_by_fuel --start-date 2026-05-18 --end-date 2026-08-15

# run the orchestrator UI (persists run history to .dagster_home/)
make dev
# then open http://localhost:3001

# build and test the transformations
cd dbt
uv run dbt deps
uv run dbt build
```

If `uv run python -m lakehouse...` fails with `ModuleNotFoundError: No module named 'lakehouse'`, the editable-install `.pth` file has had macOS's hidden flag set on it again (a recurring quirk in this environment, not a real packaging bug): run `chflags nohidden .venv/lib/python3.12/site-packages/*.pth` and retry. `make dev` clears this automatically before every launch.

## Roadmap

- Phase 2 remaining: revision resolution for generation by fuel type (system prices is done), and migration of storage to Azure Delta Lake.
- Phase 3: dimensional gold models, published dbt docs, a Power BI dashboard, and a personal Streamlit dashboard reachable on mobile.
- Phase 4 and uniqueness layer: NESO demand forecast accuracy, a connections-queue observatory built on the public TEC register, published analysis of price revisions, a small public data product, and a personal dynamic-tariff cost comparison.

## Notes

Built entirely on personal equipment, in personal time, using open public data. Source data remains under each provider's own open data terms.

## License

MIT (see LICENSE).