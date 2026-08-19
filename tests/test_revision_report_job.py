"""
Exercises commit_and_push_report()'s real git subprocess calls against a
throwaway local repo + a throwaway bare "remote", not the real project
repo: proves add/commit/diff --cached/push actually work the way the op
assumes, without ever touching this project's real GitHub remote from a
test run. See reports.py's own module docstring for why this automation
is real, not a dry run, and worth this level of proof before trusting it.
"""

import subprocess
from pathlib import Path

import pytest
from dagster import build_op_context

import lakehouse.dagster_defs.git_utils as git_utils_module
from lakehouse.dagster_defs.reports import commit_and_push_report


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True)


@pytest.fixture
def working_repo(tmp_path: Path) -> Path:
    """
    A local repo with a real bare repo as its "origin", so `git push`
    inside the op under test has somewhere real to push to. Both live
    under tmp_path, cleaned up automatically at the end of the test.
    """
    bare_remote = tmp_path / "remote.git"
    bare_remote.mkdir()
    _git(bare_remote, "init", "--bare", "-b", "main")

    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "remote", "add", "origin", str(bare_remote))

    # A first real commit: an empty repo has no HEAD for `git diff
    # --cached` to compare against, which isn't the state this op ever
    # actually runs in (the real repo always has history already).
    (repo / "README.md").write_text("baseline\n")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "push", "-u", "origin", "main")

    return repo


def test_commits_and_pushes_a_new_report(working_repo, monkeypatch):
    monkeypatch.setattr(git_utils_module, "REPO_ROOT", working_repo)

    reports_dir = working_repo / "reports"
    reports_dir.mkdir()
    report_path = reports_dir / "revision-summary-2026-07.md"
    report_path.write_text("# Revision summary: July 2026\n")

    context = build_op_context()
    commit_and_push_report(context, report_path)

    log = _git(working_repo, "log", "--oneline", "-1").stdout
    assert "2026-07" in log

    # The push actually reached the bare "remote": its main branch tip
    # now matches the working repo's, not still pointing at baseline.
    local_head = _git(working_repo, "rev-parse", "HEAD").stdout.strip()
    remote_head = _git(working_repo, "rev-parse", "origin/main").stdout.strip()
    assert local_head == remote_head


def test_a_second_identical_report_makes_no_new_commit(working_repo, monkeypatch):
    monkeypatch.setattr(git_utils_module, "REPO_ROOT", working_repo)

    reports_dir = working_repo / "reports"
    reports_dir.mkdir()
    report_path = reports_dir / "revision-summary-2026-07.md"
    report_path.write_text("# Revision summary: July 2026\n")

    context = build_op_context()
    commit_and_push_report(context, report_path)
    first_head = _git(working_repo, "rev-parse", "HEAD").stdout.strip()

    # Same content, e.g. a same-day re-run before anything about the
    # month has actually changed: must not error, and must not add a
    # second, pointless commit on top of the first.
    report_path.write_text("# Revision summary: July 2026\n")
    commit_and_push_report(context, report_path)
    second_head = _git(working_repo, "rev-parse", "HEAD").stdout.strip()

    assert first_head == second_head


def test_none_report_path_is_a_no_op(working_repo, monkeypatch):
    # render_revision_report() returns None when there's nothing to
    # report (see revision_report.py); the op downstream must not try to
    # git-add a path that was never written.
    monkeypatch.setattr(git_utils_module, "REPO_ROOT", working_repo)

    context = build_op_context()
    commit_and_push_report(context, None)  # must not raise

    log = _git(working_repo, "log", "--oneline").stdout
    assert log.count("\n") == 1  # still just the one baseline commit
