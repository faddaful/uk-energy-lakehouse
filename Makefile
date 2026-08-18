dev:
	chflags nohidden .venv/lib/python3.12/site-packages/*.pth 2>/dev/null || true
	PYTHONPATH=$(PWD)/src DAGSTER_HOME=$(PWD)/.dagster_home uv run dagster dev -p 3001

# --server.address 0.0.0.0, not the default localhost-only bind: this needs
# to be reachable over Tailscale from your phone, not just from this laptop.
streamlit:
	uv run streamlit run apps/streamlit/dashboard.py --server.address 0.0.0.0

# dbt must be run with the dbt/ folder as the working directory: profiles.yml
# lives there instead of ~/.dbt, and its `path: ../data/lakehouse.duckdb`
# resolves relative to whatever directory the shell was in when dbt started,
# not relative to profiles.yml itself. Running `dbt` from anywhere else
# silently resolves that path somewhere else too. These targets fix the
# working directory so that never depends on where you happen to be.
dbt-deps:
	cd dbt && uv run dbt deps

dbt-build: dbt-deps
	rm -f data/lakehouse.duckdb
	cd dbt && uv run dbt build

TF := terraform -chdir=infra/terraform

.PHONY: infra-init infra-plan infra-apply teardown cost-check build-local build-azure rebuild

infra-init:
	$(TF) init

infra-plan:
	$(TF) plan

infra-apply:
	$(TF) apply

teardown:
	@echo "This destroys all Azure resources including lakehouse data."
	@read -p "Type 'destroy' to confirm: " ans; [ "$$ans" = "destroy" ] || exit 1
	$(TF) destroy

cost-check:
	az consumption usage list --output table 2>/dev/null | head -20 || \
		echo "Check the portal: Cost Management > Cost analysis"

# --project-dir dbt (without cd) hits the same profiles-dir/relative-path
# bug dbt-build works around above: reproduced it here too (Error:
# Invalid value for '--profiles-dir': Path '~/.dbt' does not exist),
# fixed the same way.
build-local:
	cd dbt && TARGET=local uv run dbt build --target local

build-azure:
	cd dbt && TARGET=azure uv run dbt build --target azure

# Full reset: destroy all Azure infra, recreate it, wipe local bronze/
# duckdb, land a week of fresh data, rebuild dbt against it. A week, not
# a full historical backfill: this is meant to be run whenever, to prove
# the whole pipeline still works end to end, not a one-time seed of 90
# days of history (that's still the longer commands in the README).
#
# Deliberately does NOT also re-land and rebuild against Azure. teardown
# destroys the GitHub Actions app registration along with everything
# else; re-applying mints a brand new AZURE_CLIENT_ID, which silently
# breaks the GitHub repo secret you already set until it's updated by
# hand -- there is no `gh` CLI here to do that for you, and it needs your
# browser regardless. So this stops after printing the new values rather
# than pretending the Azure side is back to working when it isn't yet:
# update .env and the GitHub secret, then run `make build-azure` yourself
# once that's done.
#
# Two separate confirmations, deliberately: this destroys real cloud
# infrastructure (teardown's own prompt, below) and then does a chain of
# consequential follow-on work automatically, so there is a second,
# distinct gate in front of the whole sequence, not just the first one.
rebuild:
	@echo "This tears down ALL Azure infrastructure, recreates it, then wipes local bronze/duckdb and lands a week of fresh data. Real cost, real API calls."
	@read -p "Type 'rebuild' to confirm: " ans; [ "$$ans" = "rebuild" ] || exit 1
	$(MAKE) teardown
	$(TF) apply -auto-approve
	@echo ""
	@echo "=================================================================="
	@echo "New storage account:          $$($(TF) output -raw storage_account_name)"
	@echo "  -> update AZURE_STORAGE_ACCOUNT_NAME in .env to match"
	@echo "New GitHub Actions client ID: $$($(TF) output -raw github_actions_client_id)"
	@echo "  -> update the AZURE_CLIENT_ID repo secret on GitHub to match,"
	@echo "     or the azure CI job will fail auth on the next push to main"
	@echo "=================================================================="
	@echo ""
	rm -rf data/bronze data/lakehouse.duckdb data/lakehouse_azure.duckdb
	uv run python -m lakehouse.extractors.carbon_intensity --date $$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d yesterday +%Y-%m-%d)
	uv run python -m lakehouse.extractors.elexon_system_prices --start-date $$(date -v-7d +%Y-%m-%d 2>/dev/null || date -d '7 days ago' +%Y-%m-%d) --end-date $$(date +%Y-%m-%d)
	uv run python -m lakehouse.extractors.elexon_generation_by_fuel --start-date $$(date -v-7d +%Y-%m-%d 2>/dev/null || date -d '7 days ago' +%Y-%m-%d) --end-date $$(date +%Y-%m-%d)
	$(MAKE) build-local
	@echo ""
	@echo "Local rebuild complete. Azure side needs the manual update above, then: make build-azure"