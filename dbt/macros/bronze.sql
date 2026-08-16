{#
    Resolve a bronze table's read path for the current dbt target, the dbt
    equivalent of what lakehouse.io.storage.table_uri() does for the Python
    extractors. Switches on target.name (dbt's own --target flag), not the
    TARGET env var Python reads: they are two independent switches that
    need to be set together for a consistent run (see README), but dbt has
    no reason to depend on the Python side's variable to know which of its
    own two targets it's running.

    Always delta_scan(), never read_parquet() over a Delta directory:
    parquet files Delta has logically deleted are still physically present
    on disk until a vacuum runs, so read_parquet() would silently return
    rows the table no longer contains. Exactly the class of silent
    wrongness this whole project exists to catch, just aimed at the
    reader instead of the writer.
#}
{% macro bronze(table) %}
  {%- if target.name == 'azure' -%}
    delta_scan('abfss://{{ env_var("AZURE_STORAGE_CONTAINER", "lakehouse") }}@{{ env_var("AZURE_STORAGE_ACCOUNT_NAME") }}.dfs.core.windows.net/bronze/{{ table }}')
  {%- else -%}
    delta_scan('{{ var(table + "_path", "../data/bronze/" + table) }}')
  {%- endif -%}
{% endmacro %}
