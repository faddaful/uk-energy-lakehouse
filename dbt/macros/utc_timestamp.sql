{#
    Bronze TIMESTAMPTZ columns (any column landed from a tz-aware pandas
    Timestamp, e.g. pd.Timestamp.now(tz="UTC") or
    pd.to_datetime(..., utc=True), which every extractor's loaded_at and
    several source-specific fields use) are NOT safe to cast directly
    to a plain TIMESTAMP with cast(col as timestamp). DuckDB's
    TIMESTAMPTZ -> TIMESTAMP cast converts through the connecting
    SESSION's own TimeZone setting, not UTC -- a real, reproduced bug,
    not a theoretical one: this project's own dev machine's DuckDB
    session defaults to TimeZone='Europe/London' (the OS's own local
    zone, inherited automatically, not UTC), so a naive cast was
    silently shifting every Elexon settlement_period_start_utc in gold
    by an hour during BST (7,378 real mismatched rows, found by
    accident while building fct_agile_prices.sql and checked directly
    against bronze before trusting it, not assumed) before this macro
    existed. A CI runner defaulting to UTC would not reproduce the bug
    at all, which is exactly what let it ship unnoticed. See journal.md
    for the full story.

    timezone('UTC', column) forces the conversion target explicitly,
    regardless of session default: verified against known reference
    timestamps under three different session zones (UTC, Europe/London,
    America/New_York) before trusting it, not assumed from the
    function's name alone.

    Use this for every TIMESTAMPTZ bronze column a staging model reads.
    Never cast(column as timestamp) directly on one.
#}
{% macro utc_timestamp(column) %}
    cast(timezone('UTC', {{ column }}) as timestamp)
{% endmacro %}
