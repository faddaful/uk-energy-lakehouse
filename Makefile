dev:
	chflags nohidden .venv/lib/python3.12/site-packages/*.pth 2>/dev/null || true
	PYTHONPATH=$(PWD)/src DAGSTER_HOME=$(PWD)/.dagster_home uv run dagster dev -p 3001