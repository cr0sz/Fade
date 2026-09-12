#!/usr/bin/env python3
"""Static SQL-vs-schema checker for the market-intelligence codebase.

A recurring bug class here is Python code with raw SQL strings that reference
tables or columns that don't exist (typos, renames, never-migrated). These fail
silently until the query runs in production. This checker catches them at dev
time, with no external dependencies (stdlib only: ``ast`` + ``re``).

How it works
------------
1. **Build expected schema** by parsing ``migrations/*.sql`` in sorted filename
   order (the same order the real runner applies them). Handled DDL:
       - CREATE TABLE [IF NOT EXISTS] name (col ..., ...)
       - ALTER TABLE [IF EXISTS] name ADD COLUMN [IF NOT EXISTS] col ...
       - ALTER TABLE name DROP COLUMN [IF EXISTS] col
       - ALTER TABLE name RENAME [COLUMN] old TO new
       - ALTER TABLE name RENAME TO new_name
       - DROP TABLE [IF EXISTS] name
       - CREATE TABLE name AS SELECT ...     (best-effort: table known, cols not)
   CREATE INDEX and constraint-only ALTERs are intentionally ignored.

2. **Extract SQL from Python** by walking the AST of every ``git ls-files '*.py'``
   file and collecting string constants — including the literal pieces of
   f-strings (``JoinedStr``). Using the AST instead of regex over source avoids
   matching comments and keeps line numbers accurate. A string is treated as SQL
   only if it contains a SQL verb (SELECT / INSERT INTO / UPDATE / DELETE FROM /
   a FROM/JOIN clause).

3. **Extract references** from each SQL string: table names from FROM / JOIN /
   INSERT INTO / UPDATE / DELETE FROM, and — where unambiguous — column names
   from INSERT column lists and ``alias.col`` where the alias resolves to a known
   table. The matching is deliberately conservative: anything ambiguous is left
   unflagged to keep false positives near zero.

4. **Report** unknown tables and unknown columns (file:line). ``--strict`` exits
   1 on findings. An allowlist file (``scripts/sql_check_allow.txt``, one
   ``table`` or ``table.column`` per line) suppresses known false positives.

5. **Warn** (never fail) on duplicate migration number prefixes.

Known limitations (by design — we favour silence over false alarms)
-------------------------------------------------------------------
* f-strings whose ``{...}`` substitutions build the *table name* are skipped
  entirely (table is dynamic, can't be verified).
* CTEs (``WITH x AS (...)``) define transient names; we add them as known tables
  so they aren't flagged, but we don't validate their columns.
* ``SELECT *`` and computed/aliased select columns are not column-checked.
* Quoted identifiers, schema-qualified names (``public.foo``) are normalised but
  cross-schema tables aren't modelled.
* Postgres catalog tables (``pg_*``, ``information_schema.*``) are treated as
  known.
"""
from __future__ import annotations

import argparse
import ast
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = REPO_ROOT / "migrations"
ALLOWLIST_PATH = Path(__file__).resolve().parent / "sql_check_allow.txt"

# Identifiers we never want to flag as unknown tables.
_BUILTIN_TABLES = {
    "dual",
}
_BUILTIN_TABLE_PREFIXES = ("pg_", "information_schema")

_IDENT = r'"?([a-zA-Z_][a-zA-Z0-9_$]*)"?'  # optionally double-quoted identifier


# ─────────────────────────────────────────────────────────────────────────────
# Migration parsing → expected schema
# ─────────────────────────────────────────────────────────────────────────────
def _strip_sql_comments(sql: str) -> str:
    """Remove -- line comments and /* */ block comments."""
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", " ", sql)
    return sql


def _split_top_level_columns(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas that are at paren-depth 0."""
    parts, depth, cur = [], 0, []
    for ch in body:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if cur:
        parts.append("".join(cur))
    return parts


# Lines inside CREATE TABLE that are constraints, not columns.
_CONSTRAINT_KEYWORDS = {
    "primary", "foreign", "unique", "check", "constraint", "exclude", "like",
}


def _parse_create_table_columns(body: str) -> set[str]:
    cols: set[str] = set()
    for raw in _split_top_level_columns(body):
        item = raw.strip()
        if not item:
            continue
        first = item.split(None, 1)[0].strip('"').lower()
        if first in _CONSTRAINT_KEYWORDS:
            continue
        m = re.match(_IDENT, item)
        if m:
            cols.add(m.group(1).lower())
    return cols


def build_schema_from_migrations(
    migrations_dir: Path,
) -> tuple[dict[str, set[str]], list[str]]:
    """Return (schema, warnings). schema maps table -> set of column names.

    A table present with an empty/unknown column set is represented by the
    sentinel column ``"*"`` meaning "columns unknown, don't column-check".
    """
    schema: dict[str, set[str]] = {}
    warnings: list[str] = []

    files = sorted(migrations_dir.glob("*.sql"))

    # Duplicate numeric-prefix detection (warn only).
    prefixes: dict[str, list[str]] = {}
    for f in files:
        m = re.match(r"(\d+)_", f.name)
        if m:
            prefixes.setdefault(m.group(1), []).append(f.name)
    for num, names in sorted(prefixes.items()):
        if len(names) > 1:
            warnings.append(
                f"duplicate migration prefix {num}_: " + ", ".join(sorted(names))
            )

    for f in files:
        sql = _strip_sql_comments(f.read_text(encoding="utf-8"))
        _apply_migration(sql, schema)

    return schema, warnings


def _norm(name: str) -> str:
    """Normalise a table identifier: strip quotes and schema qualifier."""
    name = name.strip().strip('"')
    if "." in name:
        name = name.split(".")[-1].strip('"')
    return name.lower()


def _apply_migration(sql: str, schema: dict[str, set[str]]) -> None:
    # --- CREATE TABLE ... AS (best-effort: register table, columns unknown) ---
    for m in re.finditer(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?" + _IDENT + r"\s+AS\b",
        sql, flags=re.IGNORECASE,
    ):
        schema[_norm(m.group(1))] = {"*"}

    # --- CREATE TABLE name ( ... ) ---
    for m in re.finditer(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?" + _IDENT + r"\s*\(",
        sql, flags=re.IGNORECASE,
    ):
        table = _norm(m.group(1))
        body = _extract_paren_body(sql, m.end() - 1)
        if body is None:
            continue
        cols = _parse_create_table_columns(body)
        schema.setdefault(table, set())
        if "*" in schema[table]:
            schema[table] = set()
        schema[table] |= cols

    # --- ALTER TABLE ... (ADD/DROP/RENAME COLUMN, RENAME TO) ---
    for m in re.finditer(
        r"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?" + _IDENT + r"(.*?);",
        sql, flags=re.IGNORECASE | re.DOTALL,
    ):
        table = _norm(m.group(1))
        clause = m.group(2)
        _apply_alter(table, clause, schema)

    # --- DROP TABLE ---
    for m in re.finditer(
        r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?" + _IDENT,
        sql, flags=re.IGNORECASE,
    ):
        schema.pop(_norm(m.group(1)), None)


def _apply_alter(table: str, clause: str, schema: dict[str, set[str]]) -> None:
    # RENAME TO new_table
    rt = re.search(r"\bRENAME\s+TO\s+" + _IDENT, clause, flags=re.IGNORECASE)
    if rt:
        cols = schema.pop(table, set())
        schema[_norm(rt.group(1))] = cols
        return

    # RENAME [COLUMN] old TO new
    rc = re.search(
        r"\bRENAME\s+(?:COLUMN\s+)?" + _IDENT + r"\s+TO\s+" + _IDENT,
        clause, flags=re.IGNORECASE,
    )
    if rc:
        old, new = rc.group(1).lower(), rc.group(2).lower()
        cols = schema.setdefault(table, set())
        cols.discard(old)
        cols.add(new)
        return

    # ADD COLUMN(s) — possibly several comma-separated ADD COLUMN clauses.
    for am in re.finditer(
        r"\bADD\s+(?:COLUMN\s+)?(?:IF\s+NOT\s+EXISTS\s+)?" + _IDENT,
        clause, flags=re.IGNORECASE,
    ):
        name = am.group(1).lower()
        if name in _CONSTRAINT_KEYWORDS:
            continue
        cols = schema.setdefault(table, set())
        if "*" in cols:
            continue
        cols.add(name)

    # DROP COLUMN
    for dm in re.finditer(
        r"\bDROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?" + _IDENT,
        clause, flags=re.IGNORECASE,
    ):
        schema.get(table, set()).discard(dm.group(1).lower())


def _extract_paren_body(sql: str, open_idx: int) -> str | None:
    """Given index of an opening paren, return the balanced inner text."""
    assert sql[open_idx] == "("
    depth, i = 0, open_idx
    while i < len(sql):
        if sql[i] == "(":
            depth += 1
        elif sql[i] == ")":
            depth -= 1
            if depth == 0:
                return sql[open_idx + 1 : i]
        i += 1
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Python AST → candidate SQL strings
# ─────────────────────────────────────────────────────────────────────────────
# A string is treated as SQL only if it contains a *complete-looking* statement.
# Bare "FROM x" / "JOIN x" are deliberately NOT triggers — English prose like
# "read data FROM the cache" matches those and floods the report with noise.
_SQL_STMT_RES = (
    re.compile(r"\bSELECT\b[\s\S]*?\bFROM\b", re.IGNORECASE),
    re.compile(r"\bINSERT\s+INTO\b", re.IGNORECASE),
    re.compile(r"\bUPDATE\b[\s\S]*?\bSET\b", re.IGNORECASE),
    re.compile(r"\bDELETE\s+FROM\b", re.IGNORECASE),
    re.compile(r"\bWITH\b[\s\S]*?\bAS\s*\([\s\S]*?\bSELECT\b", re.IGNORECASE),
)


def _looks_like_sql(s: str) -> bool:
    return any(r.search(s) for r in _SQL_STMT_RES)


def extract_sql_strings(path: Path) -> list[tuple[int, str, bool]]:
    """Return list of (lineno, sql_text, dynamic_table) for SQL-looking strings.

    ``dynamic_table`` is True when an f-string interpolates a value where a table
    name could be (so we should not column/table check it).
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (SyntaxError, UnicodeDecodeError):
        return []

    # Identify docstring Constant nodes (module/class/function/async-function)
    # so we don't mistake prose like "...SELECT rows FROM the table..." for SQL.
    docstring_ids: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(
            node,
            (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            body = getattr(node, "body", None)
            if body and isinstance(body[0], ast.Expr):
                val = body[0].value
                if isinstance(val, ast.Constant) and isinstance(val.value, str):
                    docstring_ids.add(id(val))

    out: list[tuple[int, str, bool]] = []

    class V(ast.NodeVisitor):
        def visit_Constant(self, node: ast.Constant) -> None:
            if id(node) in docstring_ids:
                return
            if isinstance(node.value, str) and _looks_like_sql(node.value):
                out.append((node.lineno, node.value, False))

        def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
            # Reconstruct literal text; mark interpolation points with a sentinel.
            parts: list[str] = []
            dynamic = False
            for v in node.values:
                if isinstance(v, ast.Constant) and isinstance(v.value, str):
                    parts.append(v.value)
                else:
                    parts.append(" \x00 ")  # placeholder token
                    dynamic = True
            text = "".join(parts)
            if _looks_like_sql(text):
                table_dynamic = _interp_near_table(text)
                out.append((node.lineno, text, table_dynamic and dynamic))
            # Do not recurse into f-string children as standalone constants.

    V().visit(tree)
    return out


def _interp_near_table(text: str) -> bool:
    """True if a placeholder sits where a table name would be (FROM/JOIN/INTO)."""
    return bool(
        re.search(
            r"\b(FROM|JOIN|INTO|UPDATE)\s+\x00", text, flags=re.IGNORECASE
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# SQL string → table & column references
# ─────────────────────────────────────────────────────────────────────────────
# SQL functions that use FROM/IN as an argument separator, not a table clause:
#   EXTRACT(EPOCH FROM ts), SUBSTRING(s FROM 2), TRIM(BOTH ' ' FROM s),
#   OVERLAY(s PLACING r FROM i), POSITION(sub IN str).
# We blank the keyword so the table extractor never sees these as "FROM <table>".
_FROM_KEYWORD_FUNC_RE = re.compile(
    r"\b(EXTRACT|SUBSTRING|TRIM|OVERLAY|POSITION)\s*\(", re.IGNORECASE
)


def _mask_from_keyword_constructs(sql: str) -> str:
    """Replace FROM/IN inside EXTRACT/SUBSTRING/TRIM/etc. with spaces."""
    out = list(sql)
    for m in _FROM_KEYWORD_FUNC_RE.finditer(sql):
        # walk the matched function's parenthesised arg list
        i = m.end() - 1  # at '('
        depth = 0
        while i < len(sql):
            ch = sql[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        seg = sql[m.end() : i]
        for km in re.finditer(r"\b(FROM|IN|PLACING|BOTH|LEADING|TRAILING|FOR)\b",
                              seg, re.IGNORECASE):
            start = m.end() + km.start()
            for j in range(start, start + (km.end() - km.start())):
                out[j] = " "
    return "".join(out)


def _collect_cte_names(sql: str) -> set[str]:
    names: set[str] = set()
    for m in re.finditer(
        r"\bWITH\s+(?:RECURSIVE\s+)?" + _IDENT + r"\s+AS\b",
        sql, flags=re.IGNORECASE,
    ):
        names.add(m.group(1).lower())
    # subsequent ", name AS ("
    for m in re.finditer(r",\s*" + _IDENT + r"\s+AS\s*\(", sql, flags=re.IGNORECASE):
        names.add(m.group(1).lower())
    return names


# table reference with optional alias: "FROM tbl t" / "JOIN tbl AS t"
_TABLE_REF = (
    r"\b(?:FROM|JOIN)\s+" + _IDENT
    + r"(?:\s+(?:AS\s+)?(?!ON\b|USING\b|WHERE\b|GROUP\b|ORDER\b|LEFT\b|RIGHT\b"
    r"|INNER\b|OUTER\b|JOIN\b|LIMIT\b|HAVING\b|UNION\b|ON\b|SET\b)([a-zA-Z_][a-zA-Z0-9_]*))?"
)


def extract_references(
    sql: str,
) -> tuple[set[str], dict[str, str], list[tuple[str, str]]]:
    """Return (tables, alias_map, column_refs).

    tables       : set of referenced table names
    alias_map    : alias/short-name -> table (incl. table -> table identity)
    column_refs  : list of (table, column) pairs we are confident about
    """
    tables: set[str] = set()
    alias_map: dict[str, str] = {}
    column_refs: list[tuple[str, str]] = []

    ctes = _collect_cte_names(sql)
    sql = _mask_from_keyword_constructs(sql)

    # FROM / JOIN targets (skip when the target is a subquery "(")
    for m in re.finditer(
        r"\b(FROM|JOIN)\s+(\(|" + _IDENT + r")"
        r"(?:\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?",
        sql, flags=re.IGNORECASE,
    ):
        target = m.group(2)
        if target == "(":
            continue
        if target.lower() in ("lateral", "unnest"):
            continue  # JOIN LATERAL (...) / FROM unnest(...) — not a table
        table = _norm(target)
        alias = m.group(3)
        if alias and alias.lower() in _ALIAS_STOPWORDS:
            alias = None
        if table in ctes:
            # transient name; register alias but don't treat as schema table
            if alias:
                alias_map[alias.lower()] = table
            continue
        tables.add(table)
        alias_map[table] = table
        if alias:
            alias_map[alias.lower()] = table

    # INSERT INTO target (+ optional column list)
    for m in re.finditer(
        r"\bINSERT\s+INTO\s+" + _IDENT + r"\s*(\([^)]*\))?",
        sql, flags=re.IGNORECASE,
    ):
        table = _norm(m.group(1))
        tables.add(table)
        alias_map[table] = table
        if m.group(2):
            inner = m.group(2)[1:-1]
            for col in inner.split(","):
                cm = re.match(r"\s*" + _IDENT, col)
                if cm:
                    column_refs.append((table, cm.group(1).lower()))

    # UPDATE target (+ optional alias)
    for m in re.finditer(
        r"\bUPDATE\s+" + _IDENT
        + r"(?:\s+(?:AS\s+)?([a-zA-Z_][a-zA-Z0-9_]*))?\s+SET\b",
        sql, flags=re.IGNORECASE,
    ):
        table = _norm(m.group(1))
        tables.add(table)
        alias_map[table] = table
        alias = m.group(2)
        if alias and alias.lower() not in _ALIAS_STOPWORDS:
            alias_map[alias.lower()] = table

    # DELETE FROM is already covered by the FROM matcher above.

    # alias.column references -> only checked when alias resolves to a table.
    for m in re.finditer(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\." + _IDENT, sql):
        alias = m.group(1).lower()
        col = m.group(2).lower()
        if alias in alias_map:
            tbl = alias_map[alias]
            if tbl not in ctes:
                column_refs.append((tbl, col))

    return tables, alias_map, column_refs


# words that can never be a table alias in our queries (SQL keywords/funcs)
_ALIAS_STOPWORDS = {
    "on", "using", "where", "group", "order", "left", "right", "inner", "outer",
    "join", "limit", "having", "union", "set", "and", "or", "as", "values",
    "returning", "into", "select", "from", "cross", "natural", "full", "for",
    "offset", "window", "with",
}


# ─────────────────────────────────────────────────────────────────────────────
# Allowlist
# ─────────────────────────────────────────────────────────────────────────────
def load_allowlist(path: Path) -> set[str]:
    allow: set[str] = set()
    if not path.exists():
        return allow
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            allow.add(line.lower())
    return allow


# ─────────────────────────────────────────────────────────────────────────────
# Main check
# ─────────────────────────────────────────────────────────────────────────────
def git_python_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "*.py"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    ).stdout
    return [REPO_ROOT / line for line in out.splitlines() if line.strip()]


def check(strict: bool) -> int:
    schema, mig_warnings = build_schema_from_migrations(MIGRATIONS_DIR)
    allow = load_allowlist(ALLOWLIST_PATH)

    known_tables = set(schema.keys())

    findings: list[str] = []

    for path in git_python_files():
        rel = path.relative_to(REPO_ROOT)
        if rel == Path("scripts/check_sql_schema.py"):
            continue
        for lineno, sql, dynamic_table in extract_sql_strings(path):
            if dynamic_table:
                continue  # table name is interpolated; cannot verify
            tables, alias_map, column_refs = extract_references(sql)

            for tbl in sorted(tables):
                if tbl in known_tables or tbl in _BUILTIN_TABLES:
                    continue
                if tbl.startswith(_BUILTIN_TABLE_PREFIXES):
                    continue
                if tbl in allow:
                    continue
                findings.append(
                    f"{rel}:{lineno}: unknown table '{tbl}'"
                )

            seen_col: set[tuple[str, str]] = set()
            for tbl, col in column_refs:
                if (tbl, col) in seen_col:
                    continue
                seen_col.add((tbl, col))
                if tbl not in known_tables:
                    continue  # table itself already flagged / not ours
                cols = schema[tbl]
                if "*" in cols:
                    continue  # columns unknown (CTAS)
                if col in cols:
                    continue
                if f"{tbl}.{col}" in allow or tbl in allow:
                    continue
                findings.append(
                    f"{rel}:{lineno}: unknown column '{col}' on table '{tbl}'"
                )

    # ── Output ────────────────────────────────────────────────────────────────
    print(f"check_sql_schema: {len(known_tables)} tables parsed from migrations")

    for w in mig_warnings:
        print(f"  warning: {w}")

    if findings:
        print(f"\n{len(findings)} finding(s):")
        for f in findings:
            print(f"  {f}")
    else:
        print("  no table/column mismatches found")

    if strict and findings:
        return 1
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--strict", action="store_true",
        help="exit 1 if any findings (default: report and exit 0)",
    )
    args = ap.parse_args()
    return check(strict=args.strict)


if __name__ == "__main__":
    sys.exit(main())
