#!/usr/bin/env python3
"""
Bare-except audit (Wave C / V9.12 Class 2).

Walks the AST of every Python file under a given root and emits a structured
report of every `except` block. Classifies each block so the conversion Wave
can target the `debug_only` cohort (the V9.12 Class 2 silent-failure pattern).

Background
----------
V9.12 Class 2 ("silent-except-on-debug") names a bug class where `except
Exception` blocks absorb errors and only `logger.debug(...)` them. At debug
log level (filtered in production) the failure is invisible: no metric, no
`cycle_errors` row, no operator visibility. The April 12 cluster outage was
caused by three of these in the orphaned actor_classification chain.

Usage
-----
    python tools/audit/bare_except_audit.py app/                 # CSV to stdout
    python tools/audit/bare_except_audit.py app/collectors/      # subsystem
    python tools/audit/bare_except_audit.py app/ --summary       # summary only
    python tools/audit/bare_except_audit.py app/ --classification debug_only

CI gate
-------
    python tools/audit/bare_except_audit.py app/ \
        --max-debug-only 10 --fail-on-regression

Exit codes:
    0  — all checks passed (no thresholds set, or thresholds met)
    1  — a threshold check failed (CI gate hit)
    2  — usage / IO error
"""

import argparse
import ast
import csv
import os
import sys
from pathlib import Path
from typing import Optional


CLASSIFICATIONS = (
    "debug_only",
    "info_log",
    "warn_log",
    "error_log",
    "cycle_errors_write",
    "cycle_errors_protective_wrapper",  # inner `except: pass` around _record_cycle_error itself
    "reraise",
    "return_error_sentinel",
    "complex",
    "bare_except",          # `except:` or `except BaseException:` — Rule 6
)


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _parent_map(tree: ast.AST) -> dict[int, ast.AST]:
    """Build {id(child): parent} map by walking the tree once."""
    parents: dict[int, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[id(child)] = parent
    return parents


def _enclosing_function(node: ast.AST, parents: dict[int, ast.AST]) -> Optional[ast.AST]:
    """Return the nearest enclosing FunctionDef or AsyncFunctionDef, or None."""
    cur: Optional[ast.AST] = parents.get(id(node))
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return cur
        cur = parents.get(id(cur))
    return None


def _attr_chain_name(node: ast.AST) -> Optional[str]:
    """Return the rightmost attribute name in an attribute chain, or the Name."""
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _is_logger_call(call: ast.Call, level: str) -> bool:
    """True iff the Call looks like `<something>.<level>(...)` — typically
    `logger.debug(...)` or `log.debug(...)`. Also matches bare `<level>(...)`
    if `level` is something like `print`."""
    f = call.func
    if isinstance(f, ast.Attribute) and f.attr == level:
        return True
    if level == "print" and isinstance(f, ast.Name) and f.id == "print":
        return True
    return False


def _is_record_cycle_error_call(call: ast.Call) -> bool:
    """Match any reference to _record_cycle_error / record_cycle_error.
    The function may be imported under either name."""
    f = call.func
    name = _attr_chain_name(f)
    return name in {"_record_cycle_error", "record_cycle_error"}


def _is_sentinel_return(stmt: ast.AST) -> bool:
    """True iff stmt is `return X` where X is None, an empty container,
    a constant zero/False, or a missing return value."""
    if not isinstance(stmt, ast.Return):
        return False
    v = stmt.value
    if v is None:
        return True
    if isinstance(v, ast.Constant) and v.value in (None, 0, 0.0, False, "", b""):
        return True
    if isinstance(v, (ast.List, ast.Tuple, ast.Set)) and not v.elts:
        return True
    if isinstance(v, ast.Dict) and not v.keys:
        return True
    # `return -1` — UnaryOp wrapping a 0/1 constant is sentinel-ish
    if (isinstance(v, ast.UnaryOp) and isinstance(v.op, ast.USub)
            and isinstance(v.operand, ast.Constant)
            and v.operand.value in (0, 1, 0.0, 1.0)):
        return True
    return False


def _body_has(body: list[ast.AST], predicate) -> bool:
    """True iff any top-level statement in body matches predicate.
    Does not descend into nested functions / class bodies."""
    for stmt in body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for node in ast.walk(stmt):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                # don't recurse — but ast.walk already yielded; skip via predicate
                continue
            if predicate(node):
                return True
    return False


def _has_raise(body: list[ast.AST]) -> bool:
    """True iff body re-raises (at top level, not buried in try/except).
    A bare `raise` or `raise X` counts. We allow `raise` inside `if/else`
    blocks in the body, but not inside nested try/except handlers."""
    def walk(stmts: list[ast.AST]) -> bool:
        for s in stmts:
            if isinstance(s, ast.Raise):
                return True
            if isinstance(s, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                continue
            if isinstance(s, ast.If):
                if walk(s.body) or walk(s.orelse):
                    return True
            elif isinstance(s, (ast.For, ast.While, ast.AsyncFor, ast.With, ast.AsyncWith)):
                if walk(s.body):
                    return True
            elif isinstance(s, ast.Try):
                # a raise inside a nested try's handler is not the same as
                # this except block re-raising. only count raises in the try body.
                if walk(s.body):
                    return True
        return False
    return walk(body)


def _has_cycle_errors_write(body: list[ast.AST]) -> bool:
    for stmt in body:
        for node in ast.walk(stmt):
            if isinstance(node, ast.Call) and _is_record_cycle_error_call(node):
                return True
    return False


def _logger_call_levels(body: list[ast.AST]) -> set[str]:
    """Return set of logger levels used at the top level of body."""
    levels = set()
    for stmt in body:
        for node in ast.walk(stmt):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            if isinstance(f, ast.Attribute):
                if f.attr in {"debug", "info", "warning", "warn",
                              "error", "exception", "critical"}:
                    levels.add(f.attr)
            elif isinstance(f, ast.Name) and f.id == "print":
                levels.add("print")
    return levels


def _is_only_pass_or_comment(body: list[ast.AST]) -> bool:
    """True iff body is empty, only `pass`, or only ellipsis."""
    if not body:
        return True
    for stmt in body:
        if isinstance(stmt, ast.Pass):
            continue
        if (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant)
                and stmt.value.value is Ellipsis):
            continue
        return False
    return True


def _statement_kind(stmt: ast.AST) -> str:
    """Tag a single body statement so we can compose a body classification."""
    if isinstance(stmt, ast.Pass):
        return "pass"
    if (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant)
            and stmt.value.value is Ellipsis):
        return "pass"
    if isinstance(stmt, ast.Return):
        return "sentinel_return" if _is_sentinel_return(stmt) else "value_return"
    if isinstance(stmt, ast.Raise):
        return "raise"
    if isinstance(stmt, ast.Continue):
        return "continue"
    if isinstance(stmt, ast.Break):
        return "break"
    if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
        f = stmt.value.func
        if isinstance(f, ast.Attribute):
            if f.attr in {"debug", "info", "warning", "warn",
                          "error", "exception", "critical"}:
                return f"log_{f.attr}"
            if _is_record_cycle_error_call(stmt.value):
                return "cycle_errors_write"
        if isinstance(f, ast.Name):
            if f.id == "print":
                return "log_debug"
            if f.id in {"_record_cycle_error", "record_cycle_error"}:
                return "cycle_errors_write"
        return "expr_call"
    return "other"


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def _is_protective_cycle_error_wrapper(handler: ast.ExceptHandler,
                                       parents: dict[int, ast.AST]) -> bool:
    """Detect the spec-mandated defensive wrapper pattern around
    ``_record_cycle_error`` itself:

        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(...)
        except Exception:
            pass

    The inner ``except Exception: pass`` is intentional — error logging must
    never break the calling function. Without this exemption, every Wave-C
    conversion would inflate the ``debug_only`` count by one and tip over
    the CI gate.

    Match condition:
      - except body is exactly ``pass`` (or ``...``)
      - parent ast.Try's body contains a call to ``_record_cycle_error``
    """
    body = handler.body
    if len(body) != 1:
        return False
    only = body[0]
    is_pass = isinstance(only, ast.Pass)
    is_ellipsis = (
        isinstance(only, ast.Expr)
        and isinstance(only.value, ast.Constant)
        and only.value.value is Ellipsis
    )
    if not (is_pass or is_ellipsis):
        return False
    parent = parents.get(id(handler))
    if not isinstance(parent, ast.Try):
        return False
    return _has_cycle_errors_write(parent.body)


def classify(handler: ast.ExceptHandler,
             parents: dict[int, ast.AST]) -> str:
    """Apply the rules from the audit spec to one ExceptHandler."""
    body = handler.body

    # Rule 6: bare `except:` or `except BaseException:` — always flag.
    exc_type = handler.type
    if exc_type is None:
        return "bare_except"
    if isinstance(exc_type, ast.Name) and exc_type.id == "BaseException":
        return "bare_except"

    # cycle_errors_write takes precedence over everything else.
    if _has_cycle_errors_write(body):
        return "cycle_errors_write"

    # reraise next.
    if _has_raise(body):
        return "reraise"

    # Spec-mandated protective wrapper around _record_cycle_error itself.
    # Detected before the pass-body check so it doesn't get miscounted as
    # debug_only — the wrapper is intentional defensive code, not a V9.12
    # Class 2 silent absorber.
    if _is_protective_cycle_error_wrapper(handler, parents):
        return "cycle_errors_protective_wrapper"

    # Empty / pass body — debug_only (silent absorber).
    if _is_only_pass_or_comment(body):
        return "debug_only"

    levels = _logger_call_levels(body)
    kinds = [_statement_kind(s) for s in body]
    non_trivial = [
        k for k in kinds
        if k not in {"pass", "sentinel_return", "continue", "break"}
    ]

    # Body that's purely sentinel-returning (no logging at all) →
    # return_error_sentinel.
    if not levels and all(
            k in {"pass", "sentinel_return", "continue", "break"} for k in kinds):
        # If the body is JUST a sentinel return / continue / break, that is the
        # explicit "swallow and produce a default" pattern.
        if any(k in {"sentinel_return", "continue", "break"} for k in kinds):
            return "return_error_sentinel"
        return "debug_only"

    # Single-level logger usage with optional sentinel return / pass.
    log_only_kinds = {
        f"log_{lv}" for lv in {"debug", "info", "warning", "warn",
                               "error", "exception", "critical"}
    } | {"sentinel_return", "pass", "continue", "break"}

    if all(k in log_only_kinds for k in kinds):
        # Pick the highest-severity logger level present.
        if "debug" in levels and not (levels - {"debug"}):
            return "debug_only"
        # If only print() is used, treat as debug_only.
        if levels == {"print"}:
            return "debug_only"
        if "debug" in levels and levels.issubset({"debug", "print"}):
            return "debug_only"
        if "info" in levels and levels.issubset({"info", "debug", "print"}):
            return "info_log"
        if levels & {"warning", "warn"} and not (levels & {"error", "exception", "critical"}):
            return "warn_log"
        if levels & {"error", "exception", "critical"}:
            return "error_log"

    # Anything more elaborate (manual cleanup, fallback logic, retries, etc.)
    # needs human review.
    return "complex"


# ---------------------------------------------------------------------------
# File walk
# ---------------------------------------------------------------------------

def _body_preview(handler: ast.ExceptHandler, src_lines: list[str]) -> str:
    """One-line preview of the first 1-2 statements of the except body."""
    if not handler.body:
        return ""
    first = handler.body[0]
    end = getattr(handler.body[-1], "end_lineno", handler.body[-1].lineno)
    start = first.lineno
    snippet_lines = src_lines[start - 1: min(end, start + 1)]
    snippet = " ".join(s.strip() for s in snippet_lines)
    snippet = snippet.replace("\n", " ").replace("\r", " ")
    if len(snippet) > 160:
        snippet = snippet[:157] + "..."
    return snippet


def audit_file(path: Path) -> list[dict]:
    """Return a list of finding dicts for one file."""
    try:
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src, filename=str(path))
    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"# audit: skip {path}: {e}", file=sys.stderr)
        return []

    parents = _parent_map(tree)
    src_lines = src.splitlines()
    findings = []

    for node in ast.walk(tree):
        if not isinstance(node, ast.ExceptHandler):
            continue
        fn = _enclosing_function(node, parents)
        is_async = isinstance(fn, ast.AsyncFunctionDef)
        fn_name = fn.name if fn is not None else "<module>"
        err_var = node.name or ""
        classification = classify(node, parents)
        first_is_return = (
            bool(node.body)
            and isinstance(node.body[0], ast.Return)
        )
        findings.append({
            "path": str(path),
            "line": node.lineno,
            "function": fn_name,
            "classification": classification,
            "is_async": "1" if is_async else "0",
            "error_var": err_var,
            "first_is_return": "1" if first_is_return else "0",
            "body_preview": _body_preview(node, src_lines),
        })
    return findings


def walk_root(root: Path) -> list[dict]:
    findings = []
    if root.is_file() and root.suffix == ".py":
        return audit_file(root)
    for path in sorted(root.rglob("*.py")):
        # Skip __pycache__ and similar.
        if any(part.startswith(".") or part == "__pycache__" for part in path.parts):
            continue
        findings.extend(audit_file(path))
    return findings


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_csv(findings: list[dict], out) -> None:
    fieldnames = [
        "path", "line", "function", "classification",
        "is_async", "error_var", "first_is_return", "body_preview",
    ]
    writer = csv.DictWriter(out, fieldnames=fieldnames)
    writer.writeheader()
    for f in findings:
        writer.writerow(f)


def summary(findings: list[dict]) -> dict[str, int]:
    counts = {c: 0 for c in CLASSIFICATIONS}
    for f in findings:
        counts[f["classification"]] = counts.get(f["classification"], 0) + 1
    counts["TOTAL"] = len(findings)
    return counts


def print_summary(counts: dict[str, int], out=sys.stdout) -> None:
    print("# bare-except audit summary", file=out)
    print(f"#   total: {counts['TOTAL']}", file=out)
    for c in CLASSIFICATIONS:
        print(f"#   {c:<24} {counts.get(c, 0)}", file=out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description="Audit bare-except blocks (V9.12 Class 2).")
    p.add_argument("root", help="Directory or .py file to audit (e.g. app/, app/collectors/)")
    p.add_argument("--summary", action="store_true",
                   help="Print summary counts only (no CSV body)")
    p.add_argument("--classification", default=None,
                   help="Filter to one classification (e.g. debug_only)")
    p.add_argument("--max-debug-only", type=int, default=None,
                   help="CI gate: fail if debug_only count exceeds this number")
    p.add_argument("--fail-on-regression", action="store_true",
                   help="CI gate: combined with --max-debug-only, exit 1 if exceeded")
    p.add_argument("--baseline", default=None,
                   help="Path to a previous audit CSV; report deltas relative to it")
    args = p.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"audit: root '{root}' not found", file=sys.stderr)
        return 2

    findings = walk_root(root)

    if args.classification:
        if args.classification not in CLASSIFICATIONS:
            print(
                f"audit: unknown classification '{args.classification}'. "
                f"valid: {', '.join(CLASSIFICATIONS)}",
                file=sys.stderr,
            )
            return 2
        findings = [f for f in findings if f["classification"] == args.classification]

    counts = summary(findings)

    if args.summary:
        print_summary(counts)
    else:
        write_csv(findings, sys.stdout)
        print_summary(counts, out=sys.stderr)

    # Baseline regression report (printed to stderr; doesn't affect exit code
    # on its own — combine with --fail-on-regression for CI use).
    if args.baseline:
        try:
            with open(args.baseline) as fh:
                base_rows = list(csv.DictReader(fh))
            base_counts = {c: 0 for c in CLASSIFICATIONS}
            for r in base_rows:
                base_counts[r["classification"]] = base_counts.get(r["classification"], 0) + 1
            print("# baseline delta", file=sys.stderr)
            for c in CLASSIFICATIONS:
                delta = counts.get(c, 0) - base_counts.get(c, 0)
                if delta:
                    sign = "+" if delta > 0 else ""
                    print(f"#   {c:<24} {sign}{delta}", file=sys.stderr)
        except OSError as e:
            print(f"audit: could not read baseline: {e}", file=sys.stderr)

    # CI gate.
    if args.max_debug_only is not None:
        debug_only = counts.get("debug_only", 0)
        if debug_only > args.max_debug_only:
            print(
                f"audit: debug_only={debug_only} exceeds threshold "
                f"{args.max_debug_only}",
                file=sys.stderr,
            )
            if args.fail_on_regression:
                return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
