"""On-demand/scheduled Dagster job: render both public JSON products
(lakehouse.products.api_export) and commit + push them to api/, so
GitHub Pages serves the refreshed files (docs.yml copies api/*.json
into the published site on every push to main, see that workflow and
README.md's own "The public data product" section). Not an asset, same
reasoning as dashboard.py's streamlit_dashboard_job and reports.py's
revision_report_job: this is a side-effecting action on top of gold,
not new lakehouse data itself.

Every refresh that actually changed something is a new commit, the same
append-only pattern revision_report_job already uses, not an amended
one. Amending was considered and rejected: this repo has more than one
automated committer now (this job and revision_report_job), plus the
human, all sharing `main`. Amending HEAD assumes HEAD is always this
job's own previous run, which is not a safe assumption on a branch
someone else can also commit to -- amending a commit that turns out to
be a real revision-summary report, or a manual change, would silently
fold unrelated work into one misleading commit. A new commit every
refresh is the only version of this that cannot corrupt someone else's
history, at the real, accepted cost of this repo's commit log growing
by one entry per refresh forever. Kept in check by schedule, not by
git trickery: see the cron in schedules.py, deliberately hours apart,
not minutes.

Does NOT trigger a dbt build first, same limitation as
revision_report_job: nothing in this project schedules dbt through
Dagster today, so both JSON products reflect gold as it was last built,
not necessarily "right now." Documented in README.md's own cadence
section rather than left for a consumer to discover the hard way.
"""

from pathlib import Path

from dagster import OpExecutionContext, job, op

from lakehouse.dagster_defs import git_utils
from lakehouse.products.api_export import generate_products


@op
def render_products(context: OpExecutionContext) -> list[Path]:
    paths = generate_products()
    context.log.info(f"Wrote {[str(p) for p in paths]}")
    return paths


@op
def commit_and_push_products(context: OpExecutionContext, paths: list[Path]) -> None:
    relative_paths = [str(p.relative_to(git_utils.REPO_ROOT)) for p in paths]
    git_utils.run_git(context, "add", *relative_paths)

    # Same "nothing to commit is a real, expected outcome" check as
    # reports.py's commit_and_push_report(): a refresh between two
    # carbon-intensity forecast updates that happened to land identical
    # numbers (or ran while gold itself hadn't been rebuilt yet, see
    # this module's own docstring) produces byte-identical JSON.
    staged_diff = git_utils.run_git(context, "diff", "--cached", "--quiet")
    if staged_diff.returncode == 0:
        context.log.info("Neither JSON product changed since the last commit, nothing to push.")
        return

    commit = git_utils.run_git(context, "commit", "-m", "Refresh public data product JSON")
    if commit.returncode != 0:
        raise RuntimeError(f"git commit failed:\n{commit.stdout}{commit.stderr}")

    push = git_utils.run_git(context, "push")
    if push.returncode != 0:
        raise RuntimeError(f"git push failed:\n{push.stdout}{push.stderr}")


@job(description="Render and publish greenest_hours_next_48h.json and latest_price_anomalies.json to api/.")
def data_product_job() -> None:
    commit_and_push_products(render_products())
