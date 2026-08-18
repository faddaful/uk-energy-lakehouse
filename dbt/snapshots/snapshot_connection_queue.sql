{#
    The first dbt snapshot in this project. Everywhere else, revision
    history gets recomputed from an append-only bronze table with a
    window function (see silver__price_revisions.sql) rather than a real
    dbt snapshot, because Elexon's natural key is small and revisions are
    sparse: recomputing the whole history on every run is cheap there.
    The TEC register is a different shape of problem: ~2,200 rows, every
    row potentially changing (status, capacity, effective date) between
    one landing and the next, and rows genuinely appearing and
    disappearing as projects join and leave the queue. That is exactly
    the row-level insert/update/expire pattern dbt snapshot's SCD Type 2
    machinery exists for, so this uses the tool built for it rather than
    reimplementing it a second way.

    strategy='check', not 'timestamp': NESO's register has no reliable
    per-row "this changed at" field to sort on. mw_effective_from is a
    plan date (when a tranche's capacity is due to take effect), not a
    modification timestamp, and it is NULL for a sixth of rows besides.
    check_cols compares the columns that actually describe the queue
    (status, capacity, site, and so on), so any real change in a row is
    what triggers a new snapshot version, regardless of when NESO last
    touched it.

    check_cols is an explicit list, not 'all': as_of_date and loaded_at
    change on every single dbt run (they are stamped by the extractor,
    see neso_connections.py), and source never varies. Leaving those in
    check_cols would make dbt snapshot see a "change" on every row on
    every run, defeating the entire point: capturing the runs where
    something about the project itself actually moved.

    invalidate_hard_deletes=True: a project can genuinely leave the
    register (withdrawn, or the two-row "already built plus new
    generation" pattern merging back into one, see stg_neso_connections.sql).
    Without this, a connections_key that stops appearing in the source
    query would just sit here marked "current" forever, silently stale.
    With it, dbt end-dates that row (dbt_is_deleted=true, a real
    dbt_valid_to) the first run it notices the key is gone, so
    fct_connection_queue's as-of-month logic below can tell "still in the
    queue" from "was in the queue, isn't any more" correctly.

    target_schema='snapshots' puts this in its own schema
    (main_snapshots in the catalog), the same reasoning as gold's
    +schema: gold and seeds' +schema: seeds in dbt_project.yml: one
    schema per layer that isn't staging/silver, not because it needs to
    be scoped away from anything specific, just for the catalog to read
    cleanly.
#}

{% snapshot snapshot_connection_queue %}

{{
    config(
      target_schema='snapshots',
      unique_key='connections_key',
      strategy='check',
      invalidate_hard_deletes=True,
      check_cols=[
        'project_name',
        'customer_name',
        'connection_site',
        'stage',
        'mw_connected',
        'mw_increase_decrease',
        'cumulative_capacity_mw',
        'mw_effective_from',
        'project_status',
        'agreement_type',
        'host_to',
        'plant_type',
        'project_number',
        'gate',
      ],
    )
}}

select * from {{ ref('stg_neso_connections') }}

{% endsnapshot %}
