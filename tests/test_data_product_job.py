"""
Same idea as test_revision_report_job.py: exercises
commit_and_push_products()'s real git subprocess calls against a
throwaway local repo + a throwaway bare "remote", never this project's
actual GitHub remote.
"""

import subprocess
from pathlib import Path

import pytest
from dagster import build_op_context

import lakehouse.dagster_defs.git_utils as git_utils_module
from lakehouse.dagster_defs.products import commit_and_push_products


def _git(cwd: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True, check=True)


@pytest.fixture
def working_repo(tmp_path: Path) -> Path:
    bare_remote = tmp_path / "remote.git"
    bare_remote.mkdir()
    _git(bare_remote, "init", "--bare", "-b", "main")

    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.email", "test@example.com")
    _git(repo, "config", "user.name", "Test")
    _git(repo, "remote", "add", "origin", str(bare_remote))

    (repo / "README.md").write_text("baseline\n")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-m", "baseline")
    _git(repo, "push", "-u", "origin", "main")

    return repo


def test_commits_and_pushes_both_products(working_repo, monkeypatch):
    monkeypatch.setattr(git_utils_module, "REPO_ROOT", working_repo)

    api_dir = working_repo / "api"
    api_dir.mkdir()
    paths = [api_dir / "greenest_hours_next_48h.json", api_dir / "latest_price_anomalies.json"]
    for path in paths:
        path.write_text('{"schema_version": 1}\n')

    context = build_op_context()
    commit_and_push_products(context, paths)

    log = _git(working_repo, "log", "--oneline", "-1").stdout
    assert "Refresh public data product JSON" in log

    local_head = _git(working_repo, "rev-parse", "HEAD").stdout.strip()
    remote_head = _git(working_repo, "rev-parse", "origin/main").stdout.strip()
    assert local_head == remote_head

    committed_files = _git(working_repo, "show", "--stat", "--oneline", "HEAD").stdout
    assert "greenest_hours_next_48h.json" in committed_files
    assert "latest_price_anomalies.json" in committed_files


def test_a_second_identical_refresh_makes_no_new_commit(working_repo, monkeypatch):
    monkeypatch.setattr(git_utils_module, "REPO_ROOT", working_repo)

    api_dir = working_repo / "api"
    api_dir.mkdir()
    paths = [api_dir / "greenest_hours_next_48h.json", api_dir / "latest_price_anomalies.json"]
    for path in paths:
        path.write_text('{"schema_version": 1}\n')

    context = build_op_context()
    commit_and_push_products(context, paths)
    first_head = _git(working_repo, "rev-parse", "HEAD").stdout.strip()

    # Same content, e.g. a refresh between two forecast updates that
    # happened to land identical numbers: must not error, must not add
    # a pointless second commit.
    for path in paths:
        path.write_text('{"schema_version": 1}\n')
    commit_and_push_products(context, paths)
    second_head = _git(working_repo, "rev-parse", "HEAD").stdout.strip()

    assert first_head == second_head


def test_a_change_to_only_one_file_still_commits_both_paths(working_repo, monkeypatch):
    # git add is called with both relative paths every run, regardless
    # of which file actually changed: simpler than diffing per-file, and
    # `git diff --cached --quiet` after staging both already handles
    # "neither changed" correctly (the test above). This test proves the
    # "one changed" case doesn't error out or only half-stage.
    monkeypatch.setattr(git_utils_module, "REPO_ROOT", working_repo)

    api_dir = working_repo / "api"
    api_dir.mkdir()
    paths = [api_dir / "greenest_hours_next_48h.json", api_dir / "latest_price_anomalies.json"]
    for path in paths:
        path.write_text('{"schema_version": 1}\n')

    context = build_op_context()
    commit_and_push_products(context, paths)

    paths[0].write_text('{"schema_version": 1, "changed": true}\n')
    commit_and_push_products(context, paths)

    committed_files = _git(working_repo, "show", "--stat", "--oneline", "HEAD").stdout
    assert "greenest_hours_next_48h.json" in committed_files
