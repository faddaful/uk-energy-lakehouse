# UK Energy Market Lakehouse

A production-grade data engineering platform that ingests open UK energy data, transforms it through a medallion lakehouse, and serves it to dashboards and alerts. Built in personal time on personal equipment using free, public data.

The project has two aims: to demonstrate modern analytics engineering practice end to end (orchestration, lakehouse modelling, data quality, CI/CD), and to be genuinely useful, surfacing the cleanest and cheapest times to use electricity in my region.

## Status

Phase 1 is complete: a scheduled pipeline lands regional carbon intensity data every 30 minutes, transforms it through tested staging and silver models, and is guarded by continuous integration on every change.

Phase 2 is complete. Elexon ingestion and revision resolution are done for both system prices and generation by fuel type: bronze lands every settlement date, a weekly Dagster job re-downloads the trailing 28 days to catch anything that changes, and silver resolves each natural key to one trustworthy value with a full audit trail of every change observed. Bronze is Delta Lake, not plain parquet, and storage can point at either a local disk or a real Azure ADLS Gen2 account (provisioned by Terraform) via one environment variable.

Phase 3 is complete: a dimensional gold layer (four dimensions, three incremental facts, three dashboard-shaped marts), a personal Streamlit dashboard reading straight from it, and published dbt docs. Power BI was dropped from the plan in favour of Streamlit alone — one dashboard, kept genuinely useful, beats two built to tick a box. See [The gold layer](#the-gold-layer) below for the gold-layer design decisions and [Dashboard](#dashboard) for how the app is run and reached from a phone.

## Architecture

![Architecture](images/uk-energy-lakehouse-architecture-current.png)

Data flows from open APIs through Python extractors into a bronze, silver, gold medallion lakehouse, orchestrated by Dagster, transformed and tested by dbt, and served to dashboards.

## What works today

- Idempotent Python extractor for the Carbon Intensity API, landing bronze as two Delta tables from one endpoint call (intensity + index, and a per-fuel regional generation mix — a different, coarser 9-fuel taxonomy from Elexon's), one partition-scoped overwrite per data_date each.
- Python extractors for Elexon system prices and generation by fuel type, at settlement-period grain, correctly handling the 46/50-period clock-change days (computed from the timezone database, not hardcoded). No API key needed; both are good citizens of the public API with a descriptive User-Agent, rate limiting, and backoff on errors.
- Elexon revision resolution, for both system prices and generation by fuel type: bronze is append-only for both (see [Why bronze never overwrites for Elexon](#why-bronze-never-overwrites-for-elexon) below), a weekly Dagster job re-downloads the trailing 28 days for each, and an incremental silver model resolves each natural key (settlement_date + settlement_period for prices; settlement_date + settlement_period + fuel_type for generation, since one row there is one fuel type within one period) to its latest-seen value, with a full audit table logging every change ever observed.
- Storage on Delta Lake, locally or on Azure ADLS Gen2 behind one `TARGET` switch (see [Local vs Azure storage](#local-vs-azure-storage) below) — atomic writes, a real transaction log, and time travel to query bronze as it stood before a revision landed.
- Infrastructure as code: `infra/terraform/main.tf` provisions the resource group, ADLS Gen2 storage account (hierarchical namespace on), and container, plus an Azure AD role assignment so the pipeline authenticates via the Azure CLI's cached login rather than a long-lived key.
- Dagster orchestration: carbon intensity runs as a scheduled asset every 30 minutes; the two Elexon sources re-download weekly, staggered 30 minutes apart. All three have a blocking asset check that validates the landed data.
- dbt transformations on DuckDB: staging models that read bronze Delta tables via `delta_scan()`, type and clean the raw data, and silver models that resolve each source to one trustworthy row per natural key.
- Data quality: dbt tests (not null, uniqueness of a column combination, accepted values, range checks via dbt-expectations, and custom tests asserting resolved values trace back to a real bronze row and that revision counts stay sane) covering the models. Dedicated pytests also exercise the revision-resolution SQL idiom directly against synthetic data for both Elexon sources, independent of dbt.
- Continuous integration: GitHub Actions runs linting, unit tests, and a full dbt build against committed sample fixtures — small Delta tables, not flat parquet files, including a synthetic revision scenario for each Elexon source — on every push and pull request.
- A dimensional gold layer: five dimensions (`dim_date`, `dim_settlement_period`, `dim_region`, `dim_fuel_type`, `dim_ci_fuel_type`), four incremental facts (`fct_settlement_period`, `fct_generation`, `fct_regional_intensity`, `fct_regional_generation_mix`), and three dashboard-shaped marts, all materialised as tables inside the same DuckDB file silver already lives in. Correctly handles the 46/50-period clock-change days end to end, not just at ingestion — see [The gold layer](#the-gold-layer) below.
- Published dbt docs: a public lineage graph and column-level catalog, built from committed fixtures (no Azure credentials needed) and deployed to GitHub Pages on every push to `main`.
- Self-audited with `dbt_project_evaluator`: DAG shape, naming, folder structure, and test/documentation coverage, configured for this project's actual staging/silver/gold layering rather than left on the package's default assumptions. Every finding is either fixed, or documented as a deliberate exception with real reasoning, or left visibly open as a genuine gap — see [Auditing the project](#auditing-the-project) below.
- A personal Streamlit dashboard reading straight from gold, startable on demand from Dagster, reachable from a phone over Tailscale — see [Dashboard](#dashboard) below.

## Tech stack

Python, Dagster (orchestration), Delta Lake with DuckDB's `delta` extension (storage and query), dbt with DuckDB (transformation, gold-layer dimensional modelling, and testing), Streamlit with Plotly (dashboard), Azure ADLS Gen2 with Terraform (cloud storage and infrastructure as code), GitHub Actions (CI and dbt docs publishing), uv (packaging and environments).

## Data sources

All free and public.

- Carbon Intensity API: regional generation mix and carbon intensity (forecast, gCO2/kWh, and the API's own qualitative band), half-hourly with a 48-hour forecast. In use now.
- Elexon Insights (BMRS): wholesale prices, generation by fuel type, balancing. Public, no key required. Phase 2.
- NESO Data Portal: demand forecasts, interconnector flows, and the transmission connections queue. Later phases.

## Repository layout

```
src/lakehouse/        Python: extractors, io/storage (local vs Azure), Dagster definitions, alerts
dbt/                  dbt: staging, silver, gold (dimensions/facts/marts), seeds, tests, macros
tests/                pytest unit tests and CI fixtures (Delta tables)
.github/workflows/    CI pipeline, dbt docs publish
infra/terraform/      Resource group, ADLS Gen2 storage account, container, RBAC role assignment
apps/                 Streamlit dashboard (from Phase 3)
```

## Why bronze never overwrites for Elexon

Carbon Intensity's bronze extractor overwrites the same file when re-run for a date it has already landed: that data source has no concept of revision, so the latest fetch is always the correct fetch, and idempotent overwrite is the simpler, correct design.

Elexon is different, for both system prices and generation by fuel type. Elexon can revise a published value after the fact, so a value fetched today is not guaranteed to still be the current value next week. If bronze overwrote on every re-download, the earlier value would be gone before anything downstream could notice it had changed. So bronze is append-only for both: every write uses `mode="append"` on the Delta table, which only ever adds rows and never replaces or removes one, and a weekly Dagster job re-downloads the trailing 28 days for each, specifically to catch anything that changed since the last landing. Nothing is ever deleted from bronze; silver is where a single trustworthy value gets picked out of however many landings exist for a given key. Carbon Intensity, in contrast, uses `mode="overwrite"` scoped to the one data_date being re-landed (a `predicate` on the write, matching Delta's partitioning) — idempotent by design, and every other date already in the table is left untouched.

**One correction to how this is usually described.** The standard explanation of Elexon settlement is that a value is published as an early estimate and then corrected over a formal reconciliation calendar spanning weeks to months (II → SF → R1 → R2 → R3 → RF, the last one landing roughly 14 months after the settlement date). Before building the resolution logic, this was checked directly against the live API rather than assumed, for both sources: `/balancing/settlement/system-prices/{date}` and `/datasets/FUELHH` were both queried for settlement dates 1 day, 2.5 months, 7 months, and 15 months old, and in every case `createdDateTime` / `publishTime` sat within about a day of the original settlement date, with no trace of the later reconciliation runs touching either. Neither endpoint exposes a settlement-run-type field at all, so there is nothing to rank a "most authoritative run" against on either source, only `loaded_at`.

The practical upshot: both endpoints appear to reflect only the fast initial settling (within roughly a day of the settlement period), not the multi-month reconciliation cycle that applies to metered volumes elsewhere in BSC settlement. The design above (bronze keeps everything, weekly re-download, resolve on latest `loaded_at`) is still the right architecture regardless — it costs one extra weekly pass over a bit more history, and it is what would catch a later revision if either endpoint's behaviour is ever wrong or changes. But expect `silver__price_revisions` and `silver__generation_revisions` to be sparse or empty most weeks. That is the healthy state for these data sources, not a sign the pipeline is broken.

## The gold layer

Five dimensions, four incremental facts, three marts, all in `dbt/models/gold/`, all materialised as DuckDB tables inside the same file silver already lives in — gold is derived and cheap to rebuild, so there is no reason to write it back out to ADLS. The Streamlit dashboard reads this same file directly (see [Dashboard](#dashboard) below); a separate export step only becomes necessary the day a consumer can't reach this file itself, e.g. a publicly hosted dashboard. `+schema: gold` puts them in their own schema in the catalog (`main_gold`); staging and silver stay unscoped in `main`, where Phase 1/2 already built them.

**Settlement periods are numbered in local clock time, not UTC, so two days a year don't have 48.** The last Sunday in March (clocks forward, the 01:00-02:00 hour doesn't happen) has **46**. The last Sunday in October (clocks back, that hour happens twice) has **50**. Not "49 or 50" — a mistake worth correcting explicitly, since it's an easy one to repeat. `dim_date.settlement_periods_in_day` carries the right number for every day, computed by walking back from 31 March/October to the preceding Sunday and verified against the real 2026 BST dates (29 March, 25 October) before being trusted. Two tests check it: `tests/assert_clock_change_day_period_counts.sql` checks dim_date's entire deterministic date range on every build (it doesn't need a real clock-change day to have passed through the pipeline — dim_date depends on nothing but today's date and a static seed), and `tests/assert_period_count_matches_calendar.sql` checks real ingested data once it has a materially complete day to check.

**`fct_settlement_period.settlement_period_start_utc` is Elexon's own `startTime` field, not something derived from the period number.** An earlier plan for this layer assumed the authoritative timestamp would have to be built from period arithmetic and a clock-change formula, the same way `dim_settlement_period.nominal_start_time_local` is. Checked against the real bronze schema before writing the model: Elexon already publishes a genuine UTC `startTime` per settlement period, so the fact table just carries it through — one less place for the clock-change logic to be re-implemented and one less place it could disagree with itself.

**`fct_generation.share_of_mix_pct` includes interconnectors in its denominator** (net-mix semantics), a choice checked against 90 days of this project's own real bronze data before picking a test tolerance for it, not assumed: domestic (non-interconnector) fuels' combined share ranged from 0% to ~144% of the net-mix total across 4,352 real settlement periods, hitting exactly 0% for two consecutive periods on 2026-07-07 when every domestic fuel type was reported as literal zero while interconnector flows were already populated — a genuine late-publish gap in Elexon's feed, not a modelling bug. `tests/assert_mix_shares_within_expected_range.sql` encodes this as a `severity: warn` test with a low warn threshold and a much higher error threshold: an isolated gap like that one should surface, not fail the build; a systemic problem still would.

**The two reference seeds were verified against independent sources, not typed in from the plan this layer follows.** `seed_fuel_type`'s 20 FUELHH codes were checked against `select distinct fuelType` over this project's own 90 days of real bronze data — an exact match. `seed_region`'s GSP group letter mapping (`_A` for UKPN East, and so on) was checked against Sheffield Solar's public PV_Live API (`https://api.pvlive.uk/pvlive/api/v4/pes_list`), an independent source that ships the same table — also an exact match. Both seeds are named `seed_<x>`, not `dim_<x>`: dbt currently allows a seed and a model to share a name (as a deprecation warning, not an error — confirmed by actually running `dbt parse`), but that is explicitly flagged as being removed, so the seeds were named to not depend on it.

`uk_bank_holidays` (England & Wales, Scotland, Northern Ireland, 2023-2028) comes from the gov.uk bank holidays API, the canonical source. Only England & Wales feeds `dim_date.is_working_day` today.

**`fct_regional_generation_mix` and `intensity_index` closed a gap this layer originally shipped with.** The regional Carbon Intensity endpoint returns both a `generationmix` breakdown and a qualitative `index` label (e.g. "low", "moderate") on every call — checked directly against the live API, not assumed — but the extractor originally only kept `intensity.forecast`/`intensity.actual`/`region_id`. Both are now landed: `index` as a new column carried through `fct_regional_intensity` unchanged (its numeric thresholds are not published anywhere this project found, so it is never recomputed, only passed through), and `generationmix` as its own bronze table and fact, `fct_regional_generation_mix`, one row per half hour + region + fuel. That fact is deliberately not merged into `fct_generation`: the Carbon Intensity API's own 9-fuel taxonomy (`seed_ci_fuel_type` — notably including solar, which Elexon's transmission-metered FUELHH does not) is a different, coarser classification from Elexon's 20 FUELHH codes, on a different grain (regional forecast percentage, not GB-wide metered MW), so the two mixes are never joined to each other.

`bronze_carbon_intensity_regional_mix` lands from a second request to the same endpoint `bronze_carbon_intensity` calls, not a shared fetch — the endpoint has no documented rate limit, and one extra call every 30 minutes is negligible next to Elexon's courtesy throttling (see `elexon_common.py`), so keeping the two fetch functions independent was simpler than restructuring both around a shared raw response.

Every gold model and non-obvious column has a description; run `cd dbt && uv run dbt docs generate --static` to build the lineage graph and column catalog locally, or browse the published version at **[faddaful.github.io/uk-energy-lakehouse](https://faddaful.github.io/uk-energy-lakehouse/)** — built from committed fixtures on every push to `main` (`.github/workflows/docs.yml`), no Azure credentials needed, same principle as CI's own build job.

## Dashboard

`apps/streamlit/dashboard.py`: five tabs — how green your home region's electricity is right now and over the visible forecast (the API's own index band, plus the true regional fuel mix behind it, from `fct_regional_generation_mix`), the greenest (and, where a settled price exists, cheapest) hours in the next 24 hours from `mart_best_hours_today`, GB's whole-transmission-system generation mix with a date/period drill-down, recent price events (negative prices and large half-hour-on-half-hour swings) with an event-type filter, and a placeholder for the Phase 5 tariff comparison. It opens a read-only connection straight to the DuckDB file dbt already builds — no export step, no separate data path to keep in sync.

Run it with `make streamlit` (or `uv run streamlit run apps/streamlit/dashboard.py --server.address 0.0.0.0`), then open it from your phone over Tailscale, the same way as any other self-hosted dashboard: `http://<tailscale-ip>:8501`. It can also be started on demand from the Dagster UI — `streamlit_dashboard_job` launches it as a detached background process and no-ops if it's already running.

Runs entirely on the local network by design, same as a bank dashboard: this is personal financial-adjacent data, not something to put on the open internet. Streamlit Community Cloud is a genuine free option if a public, portfolio-facing version is ever wanted instead — but that needs the gold marts published somewhere Streamlit's servers can reach (small parquet snapshots committed by CI, in the spirit of `.github/workflows/docs.yml`), not this DuckDB file or any Azure credential. Standing up Azure App Service or Container Apps for this instead was considered and rejected: real operational surface (a container build, a registry, ingress rules to get right) for a dashboard whose only intended audience is one phone on Tailscale.

## Auditing the project

[`dbt_project_evaluator`](https://github.com/dbt-labs/dbt-project-evaluator) audits the *project*, not the data: DAG shape, naming conventions, folder structure, and test/documentation coverage. It is opt-in, not part of a plain `dbt build`:

```bash
uv run dbt build --select package:dbt_project_evaluator dbt_project_evaluator_exceptions --vars 'run_evaluator: true'
```

Its ~60 models are disabled by default (`dbt_project.yml`: `models: dbt_project_evaluator: +enabled: "{{ var('run_evaluator', false) }}"`) and only come alive with that flag. This isn't just about keeping ordinary `dbt build` runs smaller -- disabled nodes never enter the manifest at all, so a plain `dbt docs generate` (what publishes to GitHub Pages) no longer includes any of them either. Without this, the published site's first screen was `dbt_project_evaluator`'s own project tab instead of this one's, because with more than one package's models in the manifest, the docs site has more than one project to land on and doesn't reliably pick the root project first. Fixed alongside a real `models/overview.md` (`{% docs __overview__ %}`), since this project didn't have one of its own to prefer over the noise either.

It needs configuration, not just installation, to say anything useful here. Its defaults assume the standard `staging -> intermediate -> marts` layering; this project is `staging -> silver -> gold` (with gold split into `dimensions`/`facts`/`marts`), so `dbt_project.yml` maps every one of those five folders and their real prefixes (`stg_`, `silver_`, `dim_`, `fct_`, `mart_`) into the package's `model_types` variable explicitly. Left on defaults, it would have flagged every model in the project as being in the wrong folder for a layering this project never had.

**What it actually found**, run against real data, not just the empty-project case:

- **Model-level test and documentation coverage: 100%.** Every one of the 18 gold+silver+staging models has at least one test and a description (the package measures this per-model, not per-column — see [The gold layer](#the-gold-layer) above for the column-level pass, which found and fixed real gaps this coverage check wouldn't have caught).
- **`fct_missing_primary_key_tests`, `fct_model_fanout`, `fct_rejoining_of_upstream_concepts`: all real findings, all legitimate by design.** Documented as exceptions in `dbt/seeds/dbt_project_evaluator_exceptions.csv` rather than silently ignored: the three staging models and the two revision-audit-log silver models genuinely have no single-row-per-key grain (staging is an append-only bronze pass-through; the revision tables log every change, not one row per key, by design); `fct_settlement_period` genuinely does fan out to all three marts directly, because it's the central fact in a project this size; `mart_daily_summary` genuinely rejoins `dim_date` and `dim_fuel_type` directly even though both are reachable through `fct_generation`, because the mart needs the raw dimension attributes for its own aggregation, not just whatever the fact already carries. Each row in the exceptions seed has the real reasoning in its `comment` column, the same way `is_renewable`/`is_interconnector` being separate flags is explained in the seed itself rather than just applied.
- **`fct_root_models`: one real, un-excepted finding.** The three staging models (`stg_elexon_system_prices`, `stg_elexon_generation_by_fuel`, `stg_carbon_intensity`) show up with zero DAG parents. This is real, not a false positive: `macros/bronze.sql` resolves straight to a literal `delta_scan('...')` path string, never through dbt's own `source()` function, so bronze is genuinely invisible to dbt's dependency graph -- and to the published lineage graph in [The gold layer](#the-gold-layer) above, which currently shows staging models with nothing feeding into them. Left as an open, visible finding rather than added to the exceptions seed, since suppressing it would hide a real gap rather than document a legitimate design choice. Fixing it properly means turning bronze into real dbt sources with per-target (local/Azure) path resolution, which is more than a one-line change to `bronze.sql` -- noted here rather than done silently.

Two things worth knowing if this package is ever added to a similar DuckDB project:

- **DuckDB needs an explicit `dispatch` block in `dbt_project.yml`** (`search_order: ["dbt_project_evaluator", "dbt"]`). Without it the package's macros silently resolve to dbt's own no-op defaults and the audit models build empty -- a failure mode that looks exactly like "a clean project" rather than "a missing config line". Confirmed by testing without it first, not assumed from the docs.
- **A seed cannot be redocumented under its own name.** The package ships a blank `dbt_project_evaluator_exceptions` seed; the documented way to override it is a same-named seed in this project plus `+enabled: false` on the package's version. That much works. But giving the override seed its own entry in `seeds/_seeds.yml` -- even a bare `- name: dbt_project_evaluator_exceptions` with no other keys -- makes dbt 1.12 refuse to parse at all ("dbt found two schema.yml entries for the same resource"), because the package's own `seeds/seeds.yml` already describes a seed of that exact name and dbt does not scope that particular check by package. Reproduced directly (removing the property-file entry down to nothing fixed it) rather than assumed. Column types for that one seed are set via `dbt_project.yml`'s seed config block instead, which needs no property-file entry to work.

## Local vs Azure storage

Every extractor and every dbt staging model goes through `src/lakehouse/io/storage.py` rather than building a path or a credential itself, so there is exactly one place that knows the difference between local and Azure. Two functions: `table_uri(layer, name)` resolves where a table lives, `storage_options()` resolves how to authenticate to it. Both read one environment variable, `TARGET`, set in `.env`:

- **`TARGET=local`** (the default): tables live under `LOCAL_DATA_ROOT` (default `data`) as plain directories on disk — each one a real Delta table (parquet files plus a `_delta_log/` of JSON commits), not a bare parquet file. No credentials needed.
- **`TARGET=azure`**: tables live in the ADLS Gen2 container Terraform created, addressed as `abfss://<container>@<account>.dfs.core.windows.net/bronze/<name>`.

**Credentials, in order of preference:**
1. **Azure CLI identity** (the default): `storage_options()` returns `{"azure_use_azure_cli": "true", ...}`, and `deltalake`'s own Rust-native Azure client shells out to your cached `az login` session for a token on every write or read. Nothing secret ever touches disk. This is backed by the `Storage Blob Data Contributor` role `infra/terraform/main.tf` assigns to your signed-in identity, and it is the answer you want to be able to give in an interview.
2. **Account key fallback**: if `AZURE_STORAGE_ACCOUNT_KEY` is set in `.env`, it is used instead. Get it with `az storage account keys list --account-name <name> --query "[0].value" -o tsv`. This works reliably even if the CLI path misbehaves, but it is a long-lived secret sitting in a file — a stopgap, not the default.

Both paths were verified against the real Terraform-provisioned storage account, not just locally: a table was written, read back, and deleted again through `azure_use_azure_cli` before this was trusted.

**One thing this is not:** `deltalake` does not go through `adlfs` (an fsspec library) to talk to Azure. It has its own built-in Azure client and only recognises its own `azure_*`-prefixed option keys (`azure_storage_account_name`, `azure_use_azure_cli`, etc. — pulled directly from the compiled library rather than assumed, since the option names are easy to get subtly wrong). `adlfs` and `azure-identity` are still project dependencies but currently unused by anything the pipeline writes or reads; they were added ahead of settling on this design and may be useful later for something that specifically wants fsspec-style access.

**A schema gotcha worth knowing about:** Carbon Intensity's `intensity_actual` column is `None` for every row (the regional API is forecast-only). A pandas column that is `None` in every row has no non-null value for pyarrow to infer a concrete type from, so it gets typed as Arrow's `null`/void type — and DuckDB's `delta_scan()` (and most other Delta readers) reject void-typed columns outright. `carbon_intensity.py` now force-casts that column to `float64` before it ever reaches `write_deltalake`, rather than leaving it to type inference. This was caught by actually running `delta_scan()` against a real written table, not by reading the `deltalake` or DuckDB docs — worth remembering if a future column is ever all-null in a real fetch.

**dbt has the same local/Azure split, as its own `--target` flag.** `profiles.yml` defines two dbt targets, `local` and `azure`, each pointing at its own DuckDB file (`lakehouse.duckdb` / `lakehouse_azure.duckdb` — deliberately separate, see below). Every staging model reads bronze through one macro, `macros/bronze.sql`, the dbt-side equivalent of `table_uri()`: it switches on `target.name` and resolves to `delta_scan('<local path>')` or `delta_scan('abfss://...')`. `read_parquet()` is never used over a Delta directory anywhere in this project: files Delta has logically deleted are still physically present until a vacuum runs, so `read_parquet()` would silently include rows the table no longer contains — exactly the class of silent wrongness this whole project exists to catch.

- `local` target: `extensions: [delta]`, no credentials, same as always.
- `azure` target: `extensions: [delta, azure]`. DuckDB's `azure` extension is a *different* Rust crate from `deltalake`'s own Azure client, with its own separate credential resolution — the two don't share a credential path just because both say "Azure CLI." This was a real, reproduced failure, not a theoretical one: with an unpinned credential chain, `delta_scan()` tried the Azure Instance Metadata Service (managed identity) first, retried it ten times, and never got to the CLI credential at all. Fix: `profiles.yml`'s `secrets:` block pins `chain: cli` explicitly, so there is exactly one credential source, named, not a chain of fallbacks. `settings: azure_transport_option_type: curl` is also set, for a cross-platform HTTP transport rather than a platform-default one.
- **Why separate DuckDB files per target, not one shared file:** tried sharing one first. Silver's incremental models merge into whatever table already exists rather than rebuilding from scratch, so running `--target local` (90 days of bronze) and then `--target azure` (bronze with far less history landed so far) against the *same* file left 90 days of resolved rows from local bronze sitting in `silver__system_prices` with no matching row in Azure's bronze — and `resolved_values_match_bronze_source` correctly failed loudly on around 4,300 of them. That is the test doing its job, not a bug in the test. Separate files mean each target's incremental state can only ever reconcile against its own bronze, which is what "two independent environments" has to actually mean.

Both targets were run for real, not just described: `cd dbt && uv run dbt build --target local` and `--target azure` each pass all 56 tests, the azure one reading live data out of the real ADLS container.

## CI's own Azure access

CI is cloud-free by default: the `build` job (lint, unit tests, `dbt build` against committed fixtures) runs on every push and every pull request, including from forks, with no Azure credentials anywhere in reach.

A separate `azure` job runs `dbt build --target azure` and `terraform plan` against the real subscription, but only on push to `main`, never on `pull_request`. That's deliberate, not an oversight: this is a public repo, and a fork's PR runs with that PR's own (possibly modified) workflow file, so granting cloud credentials on `pull_request` would let any external contributor author a step that uses them. Push to `main` only runs code that has already been merged and is already trusted.

Authentication is OIDC via `azure/login`, backed by an `azuread_application_federated_identity_credential` (`infra/terraform/main.tf`) whose subject claim is pinned to `repo:<owner>/<repo>:ref:refs/heads/main`. There is no client secret anywhere in this chain for a leaked repo secret to expose, on either side — GitHub mints a short-lived OIDC token per run, Azure trusts it because the subject matches, and that's the whole credential. The three values in `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_SUBSCRIPTION_ID` (GitHub repo secrets) aren't sensitive on their own — they identify the app, they don't authenticate as it — they're kept as secrets anyway purely because that's the universal convention for `azure/login`'s inputs.

CI's identity gets the same `Storage Blob Data Contributor` scope as the human identity above (data-plane only, on the storage account), plus `Reader` on the resource group for `terraform plan`. That was the design going in — `plan` only reads current state to compute a diff, it never creates or changes anything, so `Contributor` seemed unnecessary. A real CI run proved that incomplete, not wrong: `Reader` doesn't include `Microsoft.Storage/storageAccounts/listKeys/action`, and `azurerm_storage_account`'s own computed attributes (`primary_access_key` etc.) get refreshed via `listKeys` on every plan regardless of whether this config reads them, so plan 403'd. `storage_use_azuread` on the provider was tried first, on the theory it would make the provider use Azure AD instead of ever calling `listKeys` — confirmed with `TF_LOG=DEBUG` that it does not, for this specific attribute refresh. Fix: `Storage Account Contributor`, the narrowest built-in role that includes `listKeys`, scoped to just this one storage account, not the resource group. Separately, the budget resource lives at subscription scope, entirely outside the resource group `Reader` covers, so reading it needed its own grant: `Cost Management Reader` at subscription scope, nothing broader. Four scoped roles now, still no `Contributor` anywhere, still each one traceable to a specific real failure rather than granted pre-emptively.

## Terraform state

Remote, in a `tfstate` container in the same storage account this project provisions (kept separate from the `lakehouse` container bronze data lives in) — not the Terraform default of a local file. This isn't a style preference: with local state, CI's fresh checkout has no state at all, so every `terraform output` and `terraform plan` there would see "nothing exists yet" regardless of what is actually deployed. That surfaced as empty-string `abfss://` URLs in dbt's `bronze()` macro, not as an obviously state-related error, before the backend existed. A human and CI running Terraform against the same infrastructure need to be looking at the same state, or neither one's view of it can be trusted.

Authenticated with `use_azuread_auth = true`, not a storage account key: the backend uses whatever Azure identity is already active (the Azure CLI session locally, `azure/login`'s OIDC-derived session in CI), the same "no stored secrets" choice as everything else here. No new RBAC grant was needed for this — the `Storage Blob Data Contributor` role both identities already have is scoped to the whole storage account, not just the `lakehouse` container, so it already covers `tfstate` too.

## Budget alert

`infra/terraform/main.tf` provisions an `azurerm_consumption_budget_subscription` at £1.50/month with two notifications, at 80% and 100% of actual spend, both emailing the account owner. This is the actual cost tripwire for the whole project, not a manual step to remember: it's created and destroyed along with everything else by `terraform apply` / `make teardown`.

## Running it locally

Prerequisites: Python and [uv](https://docs.astral.sh/uv/).

```bash
# install dependencies and the project itself
uv sync

# fetch some data (TARGET defaults to local -- creates data/bronze/... on first run)
uv run python -m lakehouse.extractors.carbon_intensity --date 2026-08-06
uv run python -m lakehouse.extractors.elexon_system_prices --start-date 2026-05-18 --end-date 2026-08-15
uv run python -m lakehouse.extractors.elexon_generation_by_fuel --start-date 2026-05-18 --end-date 2026-08-15

# run the orchestrator UI (persists run history to .dagster_home/)
make dev
# then open http://localhost:3001

# build and test the transformations
make dbt-build

# view it
make streamlit
# then open http://localhost:8501
```

To land against the real Azure storage account instead of local disk, `export TARGET=azure` (or set it in `.env`) before running an extractor -- see [Local vs Azure storage](#local-vs-azure-storage) above. `az login` needs to be current; nothing else changes.

To build and test against Azure-hosted bronze instead of local: `make build-azure` (plain `make dbt-build` / `make build-local` stay pointed at local). `TARGET` (what the Python extractors read) and dbt's `--target` (what these two make targets set) are independent switches -- `make build-azure` sets both together so a run is internally consistent, but landing data with the Python extractors and building with dbt are still two separate commands, so it is possible to point them at different places by accident. If dbt's numbers look wrong, check both are pointed at the same place before anything else.

If `uv run python -m lakehouse...` fails with `ModuleNotFoundError: No module named 'lakehouse'`, the editable-install `.pth` file has had macOS's hidden flag set on it again (a recurring quirk in this environment, not a real packaging bug): run `chflags nohidden .venv/lib/python3.12/site-packages/*.pth` and retry. `make dev` clears this automatically before every launch; `make dbt-build`/`make dbt-deps` don't need it, since dbt itself never imports the `lakehouse` package.

## Roadmap

- Phase 2 remaining: a `make teardown` target to `terraform destroy` on demand; and once a real Elexon revision has actually been captured in the wild (not just the synthetic fixture scenario), a small script showing the same settlement period at two Delta table versions with different prices, using Delta's time travel -- the actual "my pipeline noticed the official price changed" demo.
- Phase 3 remaining: the one open finding from [Auditing the project](#auditing-the-project) -- turning `macros/bronze.sql` into real dbt `source()` definitions so bronze actually appears in the DAG and the published lineage graph, instead of being a literal `delta_scan()` path string dbt can't see. (Power BI was dropped from the plan in favour of Streamlit alone; see [Status](#status).)
- Phase 4 and uniqueness layer: NESO demand forecast accuracy, a connections-queue observatory built on the public TEC register, published analysis of price revisions, a small public data product, and a personal dynamic-tariff cost comparison.

## Notes

All data are open public data. Source data remains under each provider's own open data terms.

## License

MIT (see LICENSE).