"""On-demand/monthly Dagster job: render the price-revision report for
the month that just closed (lakehouse.reports.revision_report) and
commit + push it to reports/, so the observatory reads as already
public the next morning, not something needing a manual step every
month. Not an asset: like dashboard.py's streamlit_dashboard_job, this
is a side-effecting action on top of gold, not new lakehouse data
itself, so it sits outside the asset graph as its own job.

This does NOT trigger a dbt build first: nothing in this project
schedules dbt through Dagster today (dbt runs by hand or in CI), so the
report reads whatever gold happened to be last built with, same as the
Streamlit dashboard already does. If gold is stale when this runs, the
report will be stale too; that's a real limitation, not something this
job papers over.

The git commit + push is real repo automation, not a dry run: it
changes what's on the remote without a human in the loop that day. It
runs against whatever branch is checked out where this job executes,
the same as running `git commit && git push` by hand would.
"""

import subprocess
from pathlib import Path

from dagster import OpExecutionContext, job, op

from lakehouse.reports.revision_report import generate_report

REPO_ROOT = Path(__file__).resolve().parents[3]


def _run_git(context: OpExecutionContext, *args: str) -> subprocess.CompletedProcess:
    # check=False: every caller inspects returncode itself (a non-zero
    # exit is sometimes the expected outcome here, e.g. "nothing to
    # commit"), not something subprocess should raise on unprompted.
    result = subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    context.log.info(f"git {' '.join(args)} -> exit {result.returncode}\n{result.stdout}{result.stderr}")
    return result


@op
def render_revision_report(context: OpExecutionContext) -> Path | None:
    path = generate_report()
    if path is None:
        context.log.info("No mart_revision_summary row for last month yet, nothing to report.")
    return path


@op
def commit_and_push_report(context: OpExecutionContext, report_path: Path | None) -> None:
    if report_path is None:
        return

    relative_path = report_path.relative_to(REPO_ROOT)
    _run_git(context, "add", str(relative_path))

    # `git diff --cached --quiet` exits 0 when the staged change is
    # empty: a genuinely common case here, not an edge case to ignore --
    # a re-run for a month that hasn't moved (no new revision landed)
    # regenerates byte-identical content, and that is nothing to commit,
    # not a failure.
    staged_diff = _run_git(context, "diff", "--cached", "--quiet")
    if staged_diff.returncode == 0:
        context.log.info(f"{relative_path} unchanged since the last commit, nothing to push.")
        return

    month_label = report_path.stem.replace("revision-summary-", "")
    commit = _run_git(context, "commit", "-m", f"Add revision report for {month_label}")
    if commit.returncode != 0:
        raise RuntimeError(f"git commit failed:\n{commit.stdout}{commit.stderr}")

    push = _run_git(context, "push")
    if push.returncode != 0:
        raise RuntimeError(f"git push failed:\n{push.stdout}{push.stderr}")


@job(description="Render last month's price-revision report and commit + push it to reports/.")
def revision_report_job() -> None:
    commit_and_push_report(render_revision_report())
