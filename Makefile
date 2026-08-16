dev:
	chflags nohidden .venv/lib/python3.12/site-packages/*.pth 2>/dev/null || true
	PYTHONPATH=$(PWD)/src DAGSTER_HOME=$(PWD)/.dagster_home uv run dagster dev -p 3001

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