#!/usr/bin/env python3
"""Check that a style-only pass hasn't changed what any code does.

Compares the working tree against a git ref (HEAD by default) for every
tracked .py, .sql and .yml/.yaml file, and fails if anything beyond prose
changed:

- .py: parse both versions with `ast`, strip docstrings, compare `ast.dump()`.
- .sql: strip `--` and /* */ comments, collapse whitespace, compare.
- .yml/.yaml: `yaml.safe_load`, drop every `description` key recursively,
  compare what's left.

Run after each rewrite group, before committing it:

    uv run python scripts/check_semantics_unchanged.py [base_ref]

`base_ref` defaults to HEAD, i.e. "what did this file look like before the
edits I'm about to commit".
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys

import yaml

EXCLUDED_PATH_PARTS = (
    "/target/",
    "/dbt_packages/",
    "/.terraform/",
    "/.venv/",
    "/node_modules/",
    "/tests/fixtures/",
)

SQL_LINE_COMMENT = re.compile(r"--[^\n]*")
SQL_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
WHITESPACE = re.compile(r"\s+")


def is_excluded(path: str) -> bool:
    check = f"/{path}"
    return any(part in check for part in EXCLUDED_PATH_PARTS) or path.startswith(
        "tests/fixtures/"
    )


def git_show(ref: str, path: str) -> str | None:
    result = subprocess.run(
        ["git", "show", f"{ref}:{path}"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        # file didn't exist at that ref -- nothing to compare it against
        return None
    return result.stdout


def tracked_files(*patterns: str) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", *patterns],
        capture_output=True,
        text=True,
        check=True,
    )
    return [p for p in result.stdout.splitlines() if p and not is_excluded(p)]


def strip_docstrings(tree: ast.AST) -> ast.AST:
    for node in ast.walk(tree):
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            body = node.body
            if (
                body
                and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)
            ):
                node.body = body[1:]
    return tree


def check_python(path: str, before: str, after: str) -> str | None:
    try:
        before_tree = strip_docstrings(ast.parse(before))
        after_tree = strip_docstrings(ast.parse(after))
    except SyntaxError as e:
        return f"failed to parse: {e}"
    before_dump = ast.dump(before_tree)
    after_dump = ast.dump(after_tree)
    if before_dump != after_dump:
        return "AST differs after stripping docstrings"
    return None


def strip_sql_comments(sql: str) -> str:
    sql = SQL_BLOCK_COMMENT.sub("", sql)
    sql = SQL_LINE_COMMENT.sub("", sql)
    return WHITESPACE.sub(" ", sql).strip()


def check_sql(path: str, before: str, after: str) -> str | None:
    if strip_sql_comments(before) != strip_sql_comments(after):
        return "code differs after stripping comments"
    return None


def strip_descriptions(value):
    if isinstance(value, dict):
        return {
            k: strip_descriptions(v) for k, v in value.items() if k != "description"
        }
    if isinstance(value, list):
        return [strip_descriptions(v) for v in value]
    return value


def check_yaml(path: str, before: str, after: str) -> str | None:
    try:
        before_data = yaml.safe_load(before)
        after_data = yaml.safe_load(after)
    except yaml.YAMLError as e:
        return f"failed to parse: {e}"
    if strip_descriptions(before_data) != strip_descriptions(after_data):
        return "structure differs after stripping descriptions"
    return None


def read_after(path: str) -> str | None:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return None


def main() -> int:
    base_ref = sys.argv[1] if len(sys.argv) > 1 else "HEAD"
    failures: list[str] = []

    checks = [
        (tracked_files("*.py"), check_python),
        (tracked_files("*.sql"), check_sql),
        (tracked_files("*.yml", "*.yaml"), check_yaml),
    ]

    checked = 0
    for paths, checker in checks:
        for path in paths:
            before = git_show(base_ref, path)
            after = read_after(path)
            if before is None or after is None:
                continue
            if before == after:
                continue
            checked += 1
            problem = checker(path, before, after)
            if problem:
                failures.append(f"{path}: {problem}")

    print(f"checked {checked} changed file(s) against {base_ref}")
    if failures:
        print(f"\n{len(failures)} file(s) changed semantics, not just prose:\n")
        for f in failures:
            print(f"  - {f}")
        return 1

    print("no semantic changes detected")
    return 0


if __name__ == "__main__":
    sys.exit(main())
