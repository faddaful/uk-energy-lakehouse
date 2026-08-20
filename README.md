# UK Energy Market Lakehouse

A production-grade data engineering platform for UK energy data. It ingests public data, stores it in a Delta Lake medallion architecture, transforms it with dbt, and serves it through Streamlit, GitHub Pages, and automated reports.

The project demonstrates end-to-end data engineering: ingestion, orchestration, lakehouse storage, dimensional modelling, revision handling, data quality, CI/CD, infrastructure as code, and public data products.

## Status

### Phase 1: Carbon Intensity
Complete.

- Regional carbon intensity ingested every 30 minutes.
- Bronze and silver models use Delta Lake and dbt.
- Data quality checks run in CI.

### Phase 2: Elexon
Complete.

- System prices and generation by fuel type ingested at settlement-period grain.
- Bronze is append-only to preserve source revisions.
- A weekly job re-downloads the previous 28 days.
- Silver resolves each natural key to the latest observed value and keeps a revision audit trail.
- Handles 46-period spring clock-change days and 50-period autumn clock-change days.
- Local Delta storage and Azure ADLS Gen2 are supported through a single target switch.

### Phase 3: Gold layer and dashboard
Complete.

- Five dimensions, four incremental facts, and three marts.
- Streamlit dashboard reads directly from the gold layer.
- dbt documentation is published to GitHub Pages.
- Power BI was removed from the plan; Streamlit is the dashboard platform.

### Phase 4: Observability and public products
Complete.

- NESO connections queue history using dbt snapshots.
- Monthly revision reports generated and pushed automatically.
- Public JSON data products refreshed every three hours.
- Octopus Agile comparison using available day/night usage data.

## Architecture

![Architecture](images/uk-energy-lakehouse-architecture-current.png)

## What works today

### Ingestion

- Carbon Intensity API extractor with idempotent, partition-scoped writes.
- Elexon system price and generation extractors.
- NESO TEC connections register extractor.
- Octopus Agile price extractor.
- Correct handling of UK settlement periods and daylight-saving clock changes.
- API rate limiting and retry/backoff for Elexon.
- Bronze data stored as Delta tables.

### Revision handling

Elexon data can change after publication, so its bronze tables are append-only.

A weekly Dagster job re-downloads the previous 28 days. Silver models resolve the latest value for each natural key while retaining an audit trail of observed changes.

This design preserves the raw history and allows later revisions to be detected without overwriting previous landings.

### Transformations and data quality

dbt runs on DuckDB.

- Staging models clean and type bronze data.
- Silver models resolve source records.
- Gold contains dimensions, facts, and marts.
- dbt tests cover nulls, uniqueness, accepted values, ranges, revision integrity, and source traceability.
- Pytest covers revision-resolution logic and pipeline behaviour.
- CI runs linting, unit tests, and a full dbt build against committed Delta fixtures.

### Gold layer

Gold contains:

- `dim_date`
- `dim_settlement_period`
- `dim_region`
- `dim_fuel_type`
- `dim_ci_fuel_type`
- `fct_settlement_period`
- `fct_generation`
- `fct_regional_intensity`
- `fct_regional_generation_mix`
- Dashboard marts

Settlement-period timestamps use Elexon's published UTC `startTime`.

The model explicitly handles 46-period spring clock-change days and 50-period autumn clock-change days.

The Carbon Intensity and Elexon generation classifications remain separate because they use different fuel taxonomies and grains.

### NESO connections queue

The TEC register is published twice weekly.

The pipeline:

1. Resolves the current CSV URL through the NESO CKAN API.
2. Lands each publication in bronze.
3. Uses a dbt snapshot to maintain row-level SCD Type 2 history.
4. Builds a periodic snapshot fact and queue-evolution mart.
5. Exposes queue size, technology mix, connection-year trends, and month-on-month movement in Streamlit.

The source grain is project tranche rather than project. `(Project ID, Stage)` is not always unique, so the staging model creates a deterministic connection key.

### Revision observatory

A monthly Dagster job:

1. Reads `mart_revision_summary`.
2. Generates `reports/revision-summary-YYYY-MM.md`.
3. Commits the report.
4. Pushes it to GitHub.

The report covers revision size, largest changes, and the share of settlement periods revised.

The job does not trigger dbt, so the report reflects the latest completed gold build.

### Public data product

Two JSON files are published through GitHub Pages:

- `greenest_hours_next_48h.json`: next 48 hours for the West Midlands, ranked by forecast carbon intensity and available settled price.
- `latest_price_anomalies.json`: negative system prices and half-hour price movements of £50/MWh or more over the previous 30 days.

Both include:

- `schema_version`
- `generated_at`
- `gold_built_at`

The product refreshes every three hours. An unchanged result produces no new commit.

### Money story

The project compares Octopus Agile rates with actual household usage.

Available usage data is limited to manually recorded day/night totals, so the comparison estimates Agile cost by applying average Agile rates to each usage band.

For the two periods recorded so far:

- July: £60.28 Agile vs £52.38 actual
- Second week of August: £17.18 Agile vs £11.63 actual
- Difference: Agile £13.45 higher

This cannot determine whether shifting consumption to the dashboard's cheapest hours reduced actual costs because half-hourly consumption data is unavailable.

## Tech stack

| Area | Technology |
|---|---|
| Language | Python |
| Orchestration | Dagster |
| Storage | Delta Lake |
| Query engine | DuckDB |
| Transformation | dbt |
| Dashboard | Streamlit + Plotly |
| Cloud storage | Azure ADLS Gen2 |
| Infrastructure | Terraform |
| CI/CD | GitHub Actions |
| Packaging | uv |

## Data sources

All sources are public and free to access.

- Carbon Intensity API: regional intensity and generation mix.
- Elexon Insights / BMRS: system prices and generation by fuel type.
- NESO Data Portal: TEC connections register.
- Octopus public Agile price feed.
- GOV.UK bank holidays API.

## Repository layout

```text
src/lakehouse/        Python extractors, storage, Dagster definitions, reports
dbt/                  Staging, silver, snapshots, gold, seeds, tests, macros
tests/                Pytest tests and CI fixtures
apps/                 Streamlit dashboard
infra/terraform/      Azure infrastructure and RBAC
.github/workflows/    CI and dbt documentation publishing
```

## Local and Azure storage

`TARGET` controls where Python extractors write data.

```bash
TARGET=local
TARGET=azure
```

Local:

- Delta tables under `data/`.
- No credentials required.

Azure:

- Delta tables in ADLS Gen2.
- Azure CLI authentication is the default.
- Terraform provisions the storage account, container, and RBAC.

dbt has separate `local` and `azure` targets and separate DuckDB files so incremental state cannot mix data from different storage environments.

## Azure authentication

Local development uses the Azure CLI identity.

GitHub Actions uses OIDC with a federated Azure identity. The workflow receives short-lived credentials rather than a client secret.

CI has separate permissions for:

- ADLS data access.
- Storage account state required by Terraform.
- Resource-group read access.
- Subscription-level cost-management read access.

Permissions are scoped to the resources required by each job.

## Terraform state

Terraform state is stored remotely in a dedicated `tfstate` container in the project's Azure storage account.

The backend uses Azure AD authentication rather than a storage account key.

## Budget protection

Terraform creates a £1.50/month subscription budget with alerts at 80% and 100% of actual spend.

## CI

The standard CI build is cloud-free:

- Linting.
- Unit tests.
- dbt build against committed Delta fixtures.

Azure validation runs separately on pushes to `main`:

- `dbt build --target azure`
- `terraform plan`

Azure credentials are never exposed to pull-request workflows.

## Running locally

Prerequisites: Python and [uv](https://docs.astral.sh/uv/).

```bash
uv sync

# Fetch data
uv run python -m lakehouse.extractors.carbon_intensity --date 2026-08-06
uv run python -m lakehouse.extractors.elexon_system_prices --start-date 2026-05-18 --end-date 2026-08-15
uv run python -m lakehouse.extractors.elexon_generation_by_fuel --start-date 2026-05-18 --end-date 2026-08-15
uv run python -m lakehouse.extractors.neso_connections

# Start Dagster
make dev
# http://localhost:3001

# Build and test dbt
make dbt-build

# Start Streamlit
make streamlit
# http://localhost:8501
```

For Azure:

```bash
az login
export TARGET=azure
make build-azure
```

`TARGET` controls Python extraction; dbt's `--target` controls the transformation environment. `make build-azure` sets both consistently.

## Dashboard

`apps/streamlit/dashboard.py` provides:

- Regional carbon intensity and fuel mix.
- Greenest and available cheapest upcoming hours.
- GB generation mix.
- Recent price events.
- NESO connections queue.
- Agile tariff comparison.

The dashboard reads directly from the DuckDB gold layer.

For local access:

```bash
make streamlit
```

For phone access on the private network:

```text
http://<tailscale-ip>:8501
```

The dashboard is intentionally private because the money story uses personal financial-adjacent data.

## Project audit

`dbt_project_evaluator` checks model structure, naming, tests, and documentation.

The current audit reports:

- 100% model-level test and documentation coverage for the 18 staging, silver, and gold models.
- Several findings documented as deliberate design exceptions.
- One open finding: bronze is accessed through `delta_scan()` paths rather than dbt `source()` definitions, so bronze does not appear in dbt's dependency graph.

Run the evaluator with:

```bash
uv run dbt build   --select package:dbt_project_evaluator dbt_project_evaluator_exceptions   --vars 'run_evaluator: true'
```

## Known design decisions

### Bronze overwrite vs append

Carbon Intensity bronze uses partition-scoped overwrite because the source does not publish revisions.

Elexon bronze is append-only because published values can change.

### Delta Lake vs Parquet reads

Delta tables are always queried through Delta-aware readers. Direct `read_parquet()` against Delta directories could include files that Delta has logically removed.

### UTC timestamps

All tz-aware timestamps are normalised through the project's UTC macro before conversion to timestamp values. This prevents DuckDB session timezone settings from shifting UTC data.

### Gold storage

Gold remains in DuckDB because it is derived and inexpensive to rebuild. There is no need to duplicate it in ADLS for the current consumers.

## Roadmap

- Add a `make teardown` target for Terraform.
- Demonstrate a real Elexon revision using Delta Lake time travel once one is captured.
- Replace literal bronze `delta_scan()` paths with dbt `source()` definitions.
- Add NESO demand forecast accuracy analysis.
- Consider FastAPI if the static JSON product later requires query parameters, real-time access, or other API behaviour.

## License

MIT. See `LICENSE`.
