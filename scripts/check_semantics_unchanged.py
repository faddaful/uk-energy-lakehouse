#!/usr/bin/env python3
"""Check that a style-only pass hasn't changed what any code does.

Compares the working tree against a git ref (HEAD by default) for every
tracked .py, .sql and .yml/.yaml file, and fails if anything beyond prose
changed:

- .py: parse both versions with `ast`, strip docstrings, blank the wording
  of log/print/Streamlit/exception messages and any `help=`/`description=`
  keyword string (argparse, Dagster asset/job/schedule, …), compare
  `ast.dump()`.
- .sql: strip `--` and /* */ comments, collapse whitespace, compare.
- .yml/.yaml: `yaml.safe_load`, drop every `description` key recursively,
  compare what's left.

The message-string blanking exists because those strings are exactly the
kind of prose this project's house style pass is meant to touch (see
docs/STYLE.md): wording-only edits there aren't a semantic change, so the
checker ignores their text but still compares everything else about the
call, including any interpolated expressions inside an f-string, so a
message that quietly starts interpolating the wrong variable still fails.

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
        # file didn't exist at that ref, nothing to compare it against
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


LOG_LEVELS = {"info", "warning", "error", "debug", "critical", "exception"}
ST_METHODS = {
    "title",
    "header",
    "subheader",
    "caption",
    "markdown",
    "info",
    "warning",
    "error",
    "success",
    "write",
    "text",
    "toast",
}


def _blank_string(node: ast.expr) -> ast.expr:
    # Blanks the wording but not the shape: an f-string keeps its
    # FormattedValue subtrees (the {expr} parts) untouched, only its
    # literal text segments get cleared, so a message that starts
    # interpolating a different variable still shows up as a real diff.
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return ast.Constant(value="")
    if isinstance(node, ast.JoinedStr):
        return ast.JoinedStr(
            values=[
                ast.Constant(value="")
                if isinstance(v, ast.Constant) and isinstance(v.value, str)
                else v
                for v in node.values
            ]
        )
    return node


class MessageBlanker(ast.NodeTransformer):
    """Blanks the text of log/print/Streamlit/exception messages and
    argparse help= strings, so wording-only edits to them don't register
    as a semantic change. See module docstring for why this exists.
    """

    def visit_Call(self, node: ast.Call) -> ast.AST:
        self.generic_visit(node)
        if self._is_message_call(node):
            node.args = [_blank_string(a) for a in node.args]
            node.keywords = [
                ast.keyword(arg=kw.arg, value=_blank_string(kw.value))
                for kw in node.keywords
            ]
        return node

    def visit_Raise(self, node: ast.Raise) -> ast.AST:
        self.generic_visit(node)
        if isinstance(node.exc, ast.Call):
            node.exc.args = [_blank_string(a) for a in node.exc.args]
        return node

    def visit_keyword(self, node: ast.keyword) -> ast.AST:
        self.generic_visit(node)
        if node.arg in {"help", "description"}:
            node.value = _blank_string(node.value)
        return node

    @staticmethod
    def _is_message_call(node: ast.Call) -> bool:
        func = node.func
        if isinstance(func, ast.Name) and func.id == "print":
            return True
        if not isinstance(func, ast.Attribute):
            return False
        # logger.info(...) / context.log.info(...)
        if func.attr in LOG_LEVELS:
            value = func.value
            if isinstance(value, ast.Name) and value.id in {"logger", "log"}:
                return True
            if isinstance(value, ast.Attribute) and value.attr in {"log", "logger"}:
                return True
        # st.info(...) / st.caption(...) / etc.
        return (
            func.attr in ST_METHODS
            and isinstance(func.value, ast.Name)
            and func.value.id == "st"
        )


def check_python(path: str, before: str, after: str) -> str | None:
    try:
        before_tree = MessageBlanker().visit(strip_docstrings(ast.parse(before)))
        after_tree = MessageBlanker().visit(strip_docstrings(ast.parse(after)))
    except SyntaxError as e:
        return f"failed to parse: {e}"
    before_dump = ast.dump(before_tree)
    after_dump = ast.dump(after_tree)
    if before_dump != after_dump:
        return "AST differs after stripping docstrings and message text"
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
