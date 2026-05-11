#!/usr/bin/env python3
"""
Audit for silent-failure patterns that produced Waves 1-7 on 2026-05-11.

Three checks
------------
A. Bare ``except Exception`` (or ``except:`` or ``except BaseException``)
   without BOTH a log statement AND an ``attest_state`` call inside the
   handler. Allow if the exception variable is unused and a comment
   ``# rethrown`` or ``# expected`` is on the ``except`` line. This is
   the family of bugs where an inner failure swallowed the attest call
   (Wave 5a, where attest_data_batch was buried in an outer
   try/except).

B. ``attest_state(...)`` or ``attest_data_batch(...)`` called with an
   argument that is a list comprehension or a name typed as a list,
   without a same-line comment ``# guaranteed non-empty`` justifying.
   This is the ``attest_state([])`` silent-early-return foot-gun
   (lesson 3).

C. String-literal domain arguments to ``attest_state`` /
   ``attest_data_batch`` longer than 30 characters. Migration 107
   widened the storage column to TEXT, but this lint prevents a
   regression if anyone re-introduces a VARCHAR(30) column elsewhere
   in the chain (component_batch_hashes, discovery_signals, etc).

Usage
-----
    python scripts/audit_silent_failures.py           # human-readable
    python scripts/audit_silent_failures.py --quiet   # CI mode, exits 1 on findings
    python scripts/audit_silent_failures.py --json    # JSON output

Exit codes (in CI / --quiet mode)
---------------------------------
    0 — no findings
    1 — at least one finding

Currently wired as ADVISORY in .github/workflows/audit.yml (``|| true``).
Promotion plan: after 48h of clean CI, flip to blocking. Record the
flip date in the workflow.

Scope
-----
Scans `app/`, `main.py`, `keeper/` Python files only. Skips
`scripts/`, `tests/`, generated/legacy code, and the audit scripts
themselves.
"""
from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path
from typing import Iterable

# Same scope envelope as audit_await_in_args.py
SCAN_ROOTS = ["app", "main.py"]
SKIP_DIRS = {"scripts", "tests", "node_modules", "frontend", ".git", "migrations"}

ATTEST_FUNCS = {"attest_state", "attest_data_batch"}
LOGGER_METHODS = {"info", "debug", "warning", "warn", "error", "critical", "exception", "log"}

EXCEPT_ALLOW_MARKERS = ("# rethrown", "# expected", "# silenced-by-design")
ATTEST_NONEMPTY_MARKER = "# guaranteed non-empty"

DOMAIN_MAX_LEN = 30


class Finding:
    __slots__ = ("path", "lineno", "code", "message")

    def __init__(self, path: str, lineno: int, code: str, message: str) -> None:
        self.path = path
        self.lineno = lineno
        self.code = code
        self.message = message

    def as_dict(self) -> dict:
        return {
            "path": self.path,
            "lineno": self.lineno,
            "code": self.code,
            "message": self.message,
        }

    def __str__(self) -> str:
        return f"{self.path}:{self.lineno}: [{self.code}] {self.message}"


def _line_text(source_lines: list[str], lineno: int) -> str:
    if 1 <= lineno <= len(source_lines):
        return source_lines[lineno - 1]
    return ""


def _is_logger_call(node: ast.AST) -> bool:
    """True if node is e.g. logger.info(...) or self.logger.error(...)."""
    if not isinstance(node, ast.Call):
        return False
    fn = node.func
    if isinstance(fn, ast.Attribute) and fn.attr in LOGGER_METHODS:
        return True
    if isinstance(fn, ast.Name) and fn.id in {"print"}:
        return True
    return False


def _is_attest_call(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    fn = node.func
    if isinstance(fn, ast.Attribute) and fn.attr in ATTEST_FUNCS:
        return True
    if isinstance(fn, ast.Name) and fn.id in ATTEST_FUNCS:
        return True
    return False


def _walk_handler_body(handler: ast.ExceptHandler) -> Iterable[ast.AST]:
    for stmt in handler.body:
        for node in ast.walk(stmt):
            yield node


def _check_except_block(
    handler: ast.ExceptHandler,
    source_lines: list[str],
    path: str,
) -> list[Finding]:
    """Check A: broad except needs both log and attest, or a justifying marker."""
    exc_type = handler.type
    is_broad = (
        exc_type is None
        or (isinstance(exc_type, ast.Name) and exc_type.id in {"Exception", "BaseException"})
    )
    if not is_broad:
        return []

    line_text = _line_text(source_lines, handler.lineno).strip()
    if any(marker in line_text for marker in EXCEPT_ALLOW_MARKERS):
        return []

    has_log = False
    has_attest = False
    has_reraise = False
    for node in _walk_handler_body(handler):
        if isinstance(node, ast.Raise):
            has_reraise = True
        if _is_logger_call(node):
            has_log = True
        if _is_attest_call(node):
            has_attest = True

    # A bare re-raise is fine — the exception will be caught higher up.
    if has_reraise and not has_attest:
        return []

    missing = []
    if not has_log:
        missing.append("log")
    if not has_attest:
        missing.append("attest_state/attest_data_batch")
    if not missing:
        return []

    return [
        Finding(
            path=path,
            lineno=handler.lineno,
            code="SILENT-EXCEPT",
            message=(
                f"broad except missing {' + '.join(missing)} (or `# rethrown` / "
                f"`# expected` / `# silenced-by-design` marker)"
            ),
        )
    ]


def _is_list_arg(node: ast.AST) -> bool:
    """Heuristic: arg is a list comprehension or a literal empty list.

    A name argument can't reliably be typed without flow analysis. We focus on
    the two unambiguous foot-guns: list comprehension result and empty list
    literal.
    """
    if isinstance(node, ast.ListComp):
        return True
    if isinstance(node, ast.List) and not node.elts:
        return True
    return False


def _check_attest_arg(
    call: ast.Call,
    source_lines: list[str],
    path: str,
) -> list[Finding]:
    """Check B: list-comprehension/empty-list arg to attest_* without justification.

    Check C: string-literal domain longer than DOMAIN_MAX_LEN.
    """
    findings: list[Finding] = []
    if not _is_attest_call(call):
        return findings

    # Domain is the first positional argument (or kwarg name=).
    domain_arg = None
    if call.args:
        domain_arg = call.args[0]
    else:
        for kw in call.keywords:
            if kw.arg in {"domain", "scope"}:
                domain_arg = kw.value
                break

    # Records / state are subsequent positional args (or kwarg records=/state=).
    records_arg = None
    if len(call.args) >= 2:
        records_arg = call.args[1]
    else:
        for kw in call.keywords:
            if kw.arg in {"records", "state", "data"}:
                records_arg = kw.value
                break

    # --- Check C: domain length ---
    if isinstance(domain_arg, ast.Constant) and isinstance(domain_arg.value, str):
        if len(domain_arg.value) > DOMAIN_MAX_LEN:
            findings.append(
                Finding(
                    path=path,
                    lineno=call.lineno,
                    code="ATTEST-DOMAIN-LEN",
                    message=(
                        f"domain literal {domain_arg.value!r} is "
                        f"{len(domain_arg.value)} chars > {DOMAIN_MAX_LEN} — "
                        f"migration 107 widened state_attestations.domain to "
                        f"TEXT, but adjacent VARCHAR(N) columns may still "
                        f"truncate. Shorten or audit downstream storage."
                    ),
                )
            )

    # --- Check B: list-typed records arg ---
    if records_arg is not None and _is_list_arg(records_arg):
        line_text = _line_text(source_lines, call.lineno)
        # Multi-line calls may put the arg on a later line — also scan the
        # raw argument's lineno if available.
        arg_line_text = _line_text(source_lines, records_arg.lineno) if hasattr(records_arg, "lineno") else ""
        joined = line_text + " " + arg_line_text
        if ATTEST_NONEMPTY_MARKER not in joined:
            findings.append(
                Finding(
                    path=path,
                    lineno=call.lineno,
                    code="ATTEST-EMPTY-LIST",
                    message=(
                        "attest_state/attest_data_batch called with list-comp "
                        "or empty-list arg without `# guaranteed non-empty` "
                        "marker — attest_state([]) silently early-returns "
                        "(lesson 3)"
                    ),
                )
            )

    return findings


def audit_file(path: Path) -> list[Finding]:
    source = path.read_text(encoding="utf-8", errors="replace")
    source_lines = source.splitlines()
    try:
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as e:
        return [
            Finding(
                path=str(path),
                lineno=e.lineno or 0,
                code="PARSE-ERROR",
                message=f"could not parse: {e.msg}",
            )
        ]
    findings: list[Finding] = []
    rel = str(path)
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler):
            findings.extend(_check_except_block(node, source_lines, rel))
        elif isinstance(node, ast.Call):
            findings.extend(_check_attest_arg(node, source_lines, rel))
    return findings


def iter_python_files() -> Iterable[Path]:
    repo_root = Path(__file__).resolve().parent.parent
    for entry in SCAN_ROOTS:
        target = repo_root / entry
        if target.is_file() and target.suffix == ".py":
            yield target
            continue
        if not target.is_dir():
            continue
        for p in target.rglob("*.py"):
            if any(part in SKIP_DIRS for part in p.parts):
                continue
            yield p


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quiet", action="store_true", help="CI mode — print only counts")
    parser.add_argument("--json", action="store_true", help="emit JSON to stdout")
    parser.add_argument(
        "--max-findings",
        type=int,
        default=None,
        help="exit 0 even with findings if count <= max-findings (soft-launch)",
    )
    args = parser.parse_args()

    all_findings: list[Finding] = []
    for path in iter_python_files():
        all_findings.extend(audit_file(path))

    if args.json:
        print(json.dumps([f.as_dict() for f in all_findings], indent=2))
    else:
        by_code: dict[str, int] = {}
        for f in all_findings:
            by_code[f.code] = by_code.get(f.code, 0) + 1
            if not args.quiet:
                print(f)
        if all_findings:
            summary = ", ".join(f"{k}={v}" for k, v in sorted(by_code.items()))
            print(f"\nsilent-failure audit: {len(all_findings)} finding(s) [{summary}]", file=sys.stderr)
        else:
            print("silent-failure audit: clean", file=sys.stderr)

    if not all_findings:
        return 0
    if args.max_findings is not None and len(all_findings) <= args.max_findings:
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
