"""
Fetch NESO's TEC (Transmission Entry Capacity) register, check it looks
right, and land it as a Delta table under bronze/neso_connections
(data/bronze/... for TARGET=local, an ADLS container for TARGET=azure,
see lakehouse.io.storage).

The TEC register lists every project holding a transmission connection
contract with NESO: one row per project *tranche*, not per project (a
staged project gets one row per Stage). Published twice a week, Tuesdays
and Fridays, via NESO's CKAN data portal. Confirmed directly against the
live portal before writing this, not assumed from the plan: NESO only
ever publishes the current state, there is no queryable history endpoint,
just the latest CSV. Landing every publish append-only, one partition per
as_of_date, is what actually gives this project the queue history nobody
else keeps public; dbt/snapshots/snapshot_connection_queue.sql turns that
into proper row-level history on top.

The download URL is not stable: NESO's CKAN resource embeds the publish
date in the filename itself (tec-register-18-august-2026.csv), and that
filename changes on every publish. resolve_csv_url() looks it up fresh
from the CKAN API on every run rather than hardcoding a URL, which is
what actually keeps this working past the next Tuesday or Friday.

Same fetch/validate/land shape as the other extractors. Bronze keeps the
CSV's own column names verbatim, spaces, slashes and parentheses
included ("MW Increase / Decrease", "Cumulative Total Capacity (MW)"):
confirmed against a real write that Delta and DuckDB's delta_scan() both
handle these fine, so there is no need to rename anything before it
lands. Staging is where they get renamed to something SQL-friendly (see
stg_neso_connections.sql); bronze stays a faithful copy of what NESO
actually published.
"""
import argparse
import datetime
import io
import logging

import pandas as pd
import requests
from deltalake import write_deltalake

from lakehouse.io.storage import storage_options, table_uri

# Use own logger to identify logging messages by module name.
logger = logging.getLogger(__name__)

CKAN_API_URL = "https://api.neso.energy/api/3/action/package_show"
DATASET_ID = "transmission-entry-capacity-tec-register"
SOURCE = "neso_tec_register"

# Identify this as a personal, non-commercial project, the same courtesy
# as the Elexon extractors, even though NESO's data portal needs no key.
USER_AGENT = "uk-energy-lakehouse/0.1 (personal learning project; contact: olayanjubiodun24@gmail.com)"

# The raw CSV headers, exactly as NESO publishes them. Used only to check
# every expected column actually landed; row order is whatever the CSV has.
EXPECTED_COLUMNS = [
    "Project Name",
    "Customer Name",
    "Connection Site",
    "Stage",
    "MW Connected",
    "MW Increase / Decrease",
    "Cumulative Total Capacity (MW)",
    "MW Effective From",
    "Project Status",
    "Agreement Type",
    "HOST TO",
    "Plant Type",
    "Project ID",
    "Project Number",
    "Gate",
]

# A sanity floor, not a strict expected count: the real register has run
# 2,100-2,200 rows for months. Anything under this is far more likely a
# truncated or broken download than the queue genuinely shrinking this
# fast, so it is treated as invalid rather than landed.
MIN_EXPECTED_ROWS = 500


def resolve_csv_url() -> str:
    """
    Look up today's real TEC register CSV download URL from NESO's CKAN
    API (the same package_show action any CKAN portal exposes), rather
    than guessing a dated filename. Checked directly against the live
    API before writing this: the resource's url changes on every
    publish, so a URL captured on one run is not safe to reuse on the
    next. Matches on format == "CSV" rather than the resource's name,
    since this dataset has exactly one resource and format is the more
    stable field to key off.
    """
    response = requests.get(
        CKAN_API_URL, params={"id": DATASET_ID}, headers={"User-Agent": USER_AGENT}, timeout=30
    )
    response.raise_for_status()
    resources = response.json()["result"]["resources"]

    for resource in resources:
        if resource.get("format") == "CSV":
            return resource["url"]

    raise ValueError(f"No CSV resource found in NESO dataset '{DATASET_ID}'")


def fetch_connections_data() -> pd.DataFrame:
    """
    Download the current TEC register and stamp it with the date this
    landing represents.

    as_of_date is today, the day this extractor actually ran, not
    something parsed out of the CSV: NESO publishes only the latest
    snapshot (see module docstring), so there is no "as of" field in the
    data itself to read it from.
    """
    csv_url = resolve_csv_url()
    response = requests.get(csv_url, headers={"User-Agent": USER_AGENT}, timeout=60)
    response.raise_for_status()

    # utf-8-sig strips the BOM NESO's export puts on the header row.
    # Left as plain utf-8, the BOM lands stuck to the first column's
    # name, producing "﻿Project Name" instead of "Project Name".
    # Confirmed by opening a real download, not assumed.
    df = pd.read_csv(io.BytesIO(response.content), encoding="utf-8-sig")

    # The export ends in one fully blank row (confirmed against a real
    # download). Dropped here rather than left for every downstream
    # model to work around a phantom record with no Project ID.
    # reset_index: dropping a row leaves a gap in the index, and without
    # this, write_deltalake serialises that gappy index as its own
    # __index_level_0__ column in the Delta table. Caught by actually
    # inspecting a landed table, not assumed from the pandas docs.
    df = df.dropna(how="all").reset_index(drop=True)

    # Stage and Gate are both mostly NULL in the real register (about
    # 88% and 63% of rows respectively), and either one landing as
    # entirely NULL in a single fetch is a real possibility, not just a
    # theoretical one: it happened on the very first small CI fixture
    # this project built from real data. A pandas column that is None in
    # every row has no non-null value for pyarrow to infer a type from,
    # so it lands as Arrow's "void" type, and DuckDB's delta_scan()
    # rejects void-typed columns outright. Same failure mode, same fix,
    # as carbon_intensity.py's intensity_actual column (see README):
    # force a concrete type here rather than leave it to inference.
    for column in ("Stage", "Gate"):
        df[column] = df[column].astype("float64")

    df["as_of_date"] = datetime.datetime.now(tz=datetime.UTC).date().isoformat()
    df["loaded_at"] = pd.Timestamp.now(tz="UTC")
    df["source"] = SOURCE
    return df


def validate_connections_data(df: pd.DataFrame) -> bool:
    """
    Checks the required columns are present and the register isn't
    suspiciously small. Deliberately does not check Project ID
    uniqueness: NESO's own field notes document that a project can
    legitimately appear on two rows (an already-built project adding a
    new stage), see stg_neso_connections.sql, so that is not something a
    bronze landing should reject.

    Args:
        df (pd.DataFrame): The DataFrame containing the fetched register.
    """
    if df.empty:
        logger.warning("DataFrame is empty")
        return False

    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        logger.error(f"Missing required columns: {missing}")
        return False

    if len(df) < MIN_EXPECTED_ROWS:
        logger.error(f"Only {len(df)} rows landed, expected at least {MIN_EXPECTED_ROWS}")
        return False

    return True


def land_connections_data(df: pd.DataFrame) -> None:
    """
    Append this landing to the bronze Delta table, partitioned by
    as_of_date. Append-only, like the Elexon sources, and for a related
    reason: NESO publishes only the current state, so every landing has
    to sit side by side rather than overwrite the last one, or there is
    no queue history to snapshot at all.

    Args:
        df (pd.DataFrame): Fetched, already-validated register data for
            exactly one as_of_date.
    """
    write_deltalake(
        table_uri("bronze", "neso_connections"),
        df,
        mode="append",
        partition_by=["as_of_date"],
        schema_mode="merge",
        storage_options=storage_options(),
    )
    logger.info(f"Landed {len(df)} rows for as_of_date={df['as_of_date'].iloc[0]}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch, validate, and save the current NESO TEC connections register."
    )
    parser.parse_args()

    df = fetch_connections_data()
    if validate_connections_data(df):
        land_connections_data(df)


if __name__ == "__main__":
    main()
