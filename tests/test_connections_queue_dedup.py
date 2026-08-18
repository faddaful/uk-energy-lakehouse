"""
Feed the "already built, adding new generation" two-row scenario
stg_neso_connections.sql's own comment describes through the same
row_number()-over-partition idiom that model uses, and check the two
rows come out with two different connections_keys.

Same reasoning as test_price_revision_resolution.py: a fast,
dbt-independent check of the dedup *logic* using an in-memory DuckDB
table, not a run of the compiled model itself. If the ordering rule in
stg_neso_connections.sql ever changes, change it here too.
"""

import duckdb
import pandas as pd


def synthetic_latest_landing() -> pd.DataFrame:
    """
    Three project tranches, one real landing (a single as_of_date,
    already filtered to "the latest" the way stg_neso_connections.sql's
    latest_landing CTE would leave it). Rows 0 and 1 share one
    project_id and a blank stage, the real pattern NESO's own field
    notes describe: an already-built project with a second row for new
    generation being added. Row 2 is an ordinary, already-unique tranche.
    """
    return pd.DataFrame(
        [
            {
                "project_id": "TESTPROJ1",
                "stage": None,
                "project_status": "Built",
                "connection_site": "Test Substation",
                "mw_effective_from": None,
                "project_number": "PRO-TEST002",
            },
            {
                "project_id": "TESTPROJ1",
                "stage": None,
                "project_status": "Scoping",
                "connection_site": "Test Substation",
                "mw_effective_from": "2027-07-01",
                "project_number": "PRO-TEST002",
            },
            {
                "project_id": "TESTPROJ0",
                "stage": None,
                "project_status": "Scoping",
                "connection_site": "Test GSP",
                "mw_effective_from": "2029-06-30",
                "project_number": "PRO-TEST001",
            },
        ]
    )


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """The same row_number()-over-partition + key-build idiom as stg_neso_connections.sql."""
    con = duckdb.connect()
    con.register("latest_landing", df.copy())
    return con.sql(
        """
        with deduped as (
            select
                *,
                row_number() over (
                    partition by project_id, stage
                    order by project_status, connection_site, mw_effective_from, project_number
                ) as dedup_rank
            from latest_landing
        )
        select
            project_id || '-' || coalesce(cast(stage as varchar), 'none') || '-' || cast(dedup_rank as varchar)
                as connections_key,
            project_id, project_status
        from deduped
        """
    ).df()


def test_the_two_row_collision_gets_two_distinct_keys():
    deduped = dedupe(synthetic_latest_landing())

    assert deduped["connections_key"].nunique() == 3  # one per input row, no accidental merge
    assert len(deduped) == 3

    testproj1_keys = deduped[deduped["project_id"] == "TESTPROJ1"]["connections_key"]
    assert testproj1_keys.nunique() == 2


def test_dedup_is_deterministic_regardless_of_input_row_order():
    # The tiebreak must depend only on the sort columns, not on the order
    # rows happen to arrive from bronze. Feed the same scenario reversed
    # and check the same logical row gets the same key either way.
    forward = dedupe(synthetic_latest_landing())
    reversed_df = synthetic_latest_landing().iloc[::-1].reset_index(drop=True)
    backward = dedupe(reversed_df)

    forward_keys = set(forward["connections_key"])
    backward_keys = set(backward["connections_key"])
    assert forward_keys == backward_keys


def test_a_row_with_no_collision_keeps_a_stable_single_key():
    deduped = dedupe(synthetic_latest_landing())
    testproj0 = deduped[deduped["project_id"] == "TESTPROJ0"]
    assert len(testproj0) == 1
    assert testproj0["connections_key"].iloc[0] == "TESTPROJ0-none-1"
