# Revision reports

One Markdown file per settlement month, `revision-summary-YYYY-MM.md`: how often Elexon's published system sell price changed after first publication that month, the mean and biggest individual revisions, and the top movers by size. Same numbers as `mart_revision_summary` and `silver__price_revisions` in the gold layer, rendered as prose instead of a table.

Written and committed automatically by `revision_report_job` (`src/lakehouse/dagster_defs/reports.py`), scheduled for the 1st of each month, reporting on the month that just closed. Not something to edit by hand: a re-run for the same month overwrites the file in place rather than adding a second one, so any manual edit here would just be lost on the next scheduled run.

See the main [README](../README.md#the-revision-observatory-goes-public) for the design behind this.
