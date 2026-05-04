#!/usr/bin/env python3
"""
Audit: sync DB calls inside async functions.

Finds any async function that calls a sync DB primitive directly without
wrapping it in asyncio.to_thread or using the _async variant.

Sync primitives (DANGEROUS inside async def):
  fetch_all, fetch_one, execute, get_cursor

Safe patterns (NOT flagged):
  fetch_all_async, fetch_one_async, execute_async
  await asyncio.to_thread(fetch_all, ...)
  Calls inside a sync def (even if that def is in the same file)

Exit code 1 if violations found, 0 if clean.
"""

import ast
import os
import sys

SYNC_DB_NAMES = {"fetch_all", "fetch_one", "execute", "get_cursor"}
ASYNC_DB_NAMES = {"fetch_all_async", "fetch_one_async", "execute_async"}

SKIP_DIRS = {"__pycache__", ".git", "node_modules", "frontend", "dbt", "keeper", ".venv", "venv"}


def _get_call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _is_to_thread_wrapped(node: ast.Call) -> bool:
    """Check if this call is asyncio.to_thread(sync_fn, ...) or loop.run_in_executor(None, sync_fn, ...)."""
    name = _get_call_name(node)
    if name == "to_thread":
        return True
    if name == "run_in_executor":
        return True
    return False


def _check_async_body(body: list[ast.stmt], func_name: str, filepath: str, violations: list):
    """Walk the body of an async function looking for bare sync DB calls."""
    for node in ast.walk(ast.Module(body=body, type_ignores=[])):
        # Skip sync inner function definitions — their bodies are fine
        if isinstance(node, ast.FunctionDef):
            continue

        if not isinstance(node, ast.Call):
            continue

        call_name = _get_call_name(node)
        if call_name not in SYNC_DB_NAMES:
            continue

        # Check: is this call an argument to asyncio.to_thread or run_in_executor?
        # We need to walk parents for this. Instead, check if the Call is a direct
        # arg to another Call that's to_thread/run_in_executor.
        # ast.walk doesn't give parents, so we do a second pass.
        pass  # handled below

    # More precise: walk with parent tracking
    _walk_with_parent(body, func_name, filepath, violations)


def _walk_with_parent(body: list[ast.stmt], func_name: str, filepath: str, violations: list):
    """Walk AST body tracking parent to detect if sync call is wrapped in to_thread."""

    class _Visitor(ast.NodeVisitor):
        def __init__(self):
            self.in_sync_func = False

        def visit_FunctionDef(self, node):
            # Sync inner function — don't flag calls inside it
            old = self.in_sync_func
            self.in_sync_func = True
            self.generic_visit(node)
            self.in_sync_func = old

        def visit_AsyncFunctionDef(self, node):
            # Nested async — recurse but don't flag (it'll be handled as its own top-level)
            pass

        def visit_Call(self, node):
            if self.in_sync_func:
                self.generic_visit(node)
                return

            call_name = _get_call_name(node)

            # Check if this is to_thread(sync_fn, ...) — the sync_fn arg is safe
            if call_name in ("to_thread", "run_in_executor"):
                # Don't recurse into args — they're intentionally sync
                return

            if call_name in SYNC_DB_NAMES:
                violations.append({
                    "file": filepath,
                    "line": node.lineno,
                    "func": func_name,
                    "call": call_name,
                })

            self.generic_visit(node)

        def visit_With(self, node):
            if self.in_sync_func:
                self.generic_visit(node)
                return

            for item in node.items:
                if isinstance(item.context_expr, ast.Call):
                    cname = _get_call_name(item.context_expr)
                    if cname == "get_cursor":
                        violations.append({
                            "file": filepath,
                            "line": node.lineno,
                            "func": func_name,
                            "call": "get_cursor (with block)",
                        })

            self.generic_visit(node)

    visitor = _Visitor()
    for stmt in body:
        visitor.visit(stmt)


def audit_file(filepath: str) -> list[dict]:
    violations = []
    try:
        with open(filepath) as f:
            source = f.read()
        tree = ast.parse(source, filename=filepath)
    except (SyntaxError, UnicodeDecodeError):
        return []

    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef):
            _check_async_body(node.body, node.name, filepath, violations)
    return violations


def main():
    app_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app")
    if not os.path.isdir(app_dir):
        app_dir = "app"

    all_violations = []
    files_with_violations = set()

    for root, dirs, files in os.walk(app_dir):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for fname in sorted(files):
            if not fname.endswith(".py"):
                continue
            filepath = os.path.join(root, fname)
            violations = audit_file(filepath)
            if violations:
                all_violations.extend(violations)
                files_with_violations.add(filepath)

    for v in all_violations:
        print(f"{v['file']}:{v['line']}: in async {v['func']}: {v['call']}")

    print()
    print(f"{len(all_violations)} violations across {len(files_with_violations)} files.")

    return 1 if all_violations else 0


if __name__ == "__main__":
    sys.exit(main())
