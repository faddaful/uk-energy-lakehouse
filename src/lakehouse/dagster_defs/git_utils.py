"""Shared subprocess git helper for Dagster jobs that commit real
changes to this repo (reports.py, products.py). One place for the
check=False / logging convention and REPO_ROOT, so the two jobs' git
behaviour can't quietly drift out of sync with each other the way the
CI fixture-vars strings already did once (see journal.md)."""

import subprocess
from pathlib import Path

from dagster import OpExecutionContext

REPO_ROOT = Path(__file__).resolve().parents[3]


def run_git(context: OpExecutionContext, *args: str) -> subprocess.CompletedProcess:
    """
    check=False: every caller inspects returncode itself (a non-zero
    exit is sometimes the expected outcome here, e.g. "nothing to
    commit"), not something subprocess should raise on unprompted.

    Args:
        context (OpExecutionContext): For logging the command and its output.
        *args (str): Arguments to `git`, e.g. "add", "reports/foo.md".
    """
    result = subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
    context.log.info(f"git {' '.join(args)} -> exit {result.returncode}\n{result.stdout}{result.stderr}")
    return result
