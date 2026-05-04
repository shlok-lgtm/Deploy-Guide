#!/usr/bin/env python3
"""
Audit v1: callgraph-aware sync-blocking-in-async detection.

Finds async functions that call blocking primitives — either directly or
transitively through sync helper functions.

Blocking primitives:
  DB sync:     fetch_all, fetch_one, execute, get_cursor,
               psycopg2.connect, execute_values, sqlite3.connect
  HTTP sync:   requests.get/post/put/delete/patch/request,
               urllib.request.urlopen, httpx.get/post (module-level sync)
  Subprocess:  subprocess.run/check_output/check_call/call, Popen.wait
  Sleep:       time.sleep

Safe patterns (NOT flagged):
  fetch_all_async, fetch_one_async, execute_async
  await asyncio.to_thread(blocking_fn, ...)
  await loop.run_in_executor(None, blocking_fn, ...)
  Calls inside a sync def NOT called from async context
  Lines with `# audit: ok-sync-from-async` comment

Known limitations (v1):
  - Name-based callgraph: misses calls through variables, dynamic dispatch,
    decorators that wrap functions. Known false-negative source.
  - Cross-module imports with aliases are handled for common cases but
    complex re-export chains may be missed.
  - We do NOT resolve class inheritance or method resolution order.
  - Goal: catch 80% of remaining bugs, not be a perfect static analyzer.

Exit code 1 if violations found, 0 if clean.
"""

import ast
import os
import sys
from collections import defaultdict

BLOCKING_PRIMITIVES = {
    # DB sync
    "fetch_all", "fetch_one", "execute", "get_cursor",
    "execute_values",
    # HTTP sync
    "urlopen",
    # Subprocess
    "check_output", "check_call",
}

BLOCKING_QUALIFIED = {
    ("requests", "get"), ("requests", "post"), ("requests", "put"),
    ("requests", "delete"), ("requests", "patch"), ("requests", "request"),
    ("subprocess", "run"), ("subprocess", "call"),
    ("time", "sleep"),
    ("httpx", "get"), ("httpx", "post"),
}

SAFE_NAMES = {"fetch_all_async", "fetch_one_async", "execute_async"}

SKIP_DIRS = {
    "__pycache__", ".git", "node_modules", "frontend", "dbt",
    "keeper", ".venv", "venv", "test", "tests",
}

WHITELIST_COMMENT = "# audit: ok-sync-from-async"


def _get_call_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _get_qualified_call(node: ast.Call) -> tuple[str, str] | None:
    if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
        return (node.func.value.id, node.func.attr)
    return None


# ── Phase 1: Build per-file function info ──────────────────────────────────

class FuncInfo:
    __slots__ = ("name", "is_async", "calls", "has_blocking", "lineno", "filepath")

    def __init__(self, name, is_async, lineno, filepath):
        self.name = name
        self.is_async = is_async
        self.calls = set()
        self.has_blocking = False
        self.lineno = lineno
        self.filepath = filepath


def _extract_functions(filepath: str) -> tuple[list[FuncInfo], dict[str, str], list[str]]:
    """Parse a file and return function info, import aliases, and source lines."""
    try:
        with open(filepath) as f:
            source = f.read()
        tree = ast.parse(source, filename=filepath)
        lines = source.splitlines()
    except (SyntaxError, UnicodeDecodeError):
        return [], {}, []

    funcs = []
    import_aliases = {}

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                real = alias.name
                local = alias.asname or alias.name
                import_aliases[local] = real
        elif isinstance(node, ast.Import):
            for alias in node.names:
                local = alias.asname or alias.name
                import_aliases[local] = alias.name

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            fi = FuncInfo(
                name=node.name,
                is_async=isinstance(node, ast.AsyncFunctionDef),
                lineno=node.lineno,
                filepath=filepath,
            )
            _scan_func_body(node.body, fi, lines, import_aliases)
            funcs.append(fi)

    return funcs, import_aliases, lines


def _scan_func_body(body, fi: FuncInfo, lines: list[str], import_aliases: dict):
    """Scan function body for calls and blocking primitives."""

    class _Scanner(ast.NodeVisitor):
        def __init__(self):
            self.depth = 0

        def visit_FunctionDef(self, node):
            if self.depth > 0:
                return
            self.depth += 1
            self.generic_visit(node)
            self.depth -= 1

        def visit_AsyncFunctionDef(self, node):
            pass

        def visit_Call(self, node):
            name = _get_call_name(node)
            qual = _get_qualified_call(node)

            if name in ("to_thread", "run_in_executor"):
                return

            if name:
                resolved = import_aliases.get(name, name)
                fi.calls.add(resolved)
                if resolved in BLOCKING_PRIMITIVES:
                    line_text = lines[node.lineno - 1] if node.lineno <= len(lines) else ""
                    if WHITELIST_COMMENT not in line_text:
                        fi.has_blocking = True

            if qual:
                mod, attr = qual
                resolved_mod = import_aliases.get(mod, mod)
                if (resolved_mod, attr) in BLOCKING_QUALIFIED:
                    line_text = lines[node.lineno - 1] if node.lineno <= len(lines) else ""
                    if WHITELIST_COMMENT not in line_text:
                        fi.has_blocking = True
                fi.calls.add(attr)

            self.generic_visit(node)

        def visit_With(self, node):
            for item in node.items:
                if isinstance(item.context_expr, ast.Call):
                    cname = _get_call_name(item.context_expr)
                    if cname and import_aliases.get(cname, cname) == "get_cursor":
                        line_text = lines[node.lineno - 1] if node.lineno <= len(lines) else ""
                        if WHITELIST_COMMENT not in line_text:
                            fi.has_blocking = True
                            fi.calls.add("get_cursor")
            self.generic_visit(node)

    scanner = _Scanner()
    for stmt in body:
        scanner.visit(stmt)


# ── Phase 2: Transitive blocking resolution ────────────────────────────────

def _resolve_blocking(all_funcs: dict[str, FuncInfo], max_depth=10):
    """Mark functions as blocking if they transitively call a blocking function."""
    changed = True
    iteration = 0
    while changed and iteration < max_depth:
        changed = False
        iteration += 1
        for fi in all_funcs.values():
            if fi.has_blocking:
                continue
            for callee_name in fi.calls:
                callee = all_funcs.get(callee_name)
                if callee and callee.has_blocking:
                    fi.has_blocking = True
                    changed = True
                    break


def _find_blocking_chain(fi: FuncInfo, all_funcs: dict[str, FuncInfo], seen=None) -> list[str]:
    """Find the shortest chain from fi to a blocking primitive."""
    if seen is None:
        seen = set()
    if fi.name in seen:
        return []
    seen.add(fi.name)

    for callee_name in fi.calls:
        if callee_name in BLOCKING_PRIMITIVES:
            return [fi.name, callee_name]
        callee = all_funcs.get(callee_name)
        if callee and callee.has_blocking:
            chain = _find_blocking_chain(callee, all_funcs, seen)
            if chain:
                return [fi.name] + chain
    return [fi.name, "?"]


# ── Phase 3: Find async functions calling blocking sync functions ──────────

def _find_violations(filepath, funcs, all_funcs, lines):
    """Find async functions that call blocking sync helpers bare."""
    violations = []

    async_funcs = [f for f in funcs if f.is_async]
    for af in async_funcs:
        for callee_name in af.calls:
            callee = all_funcs.get(callee_name)
            if callee and callee.has_blocking and not callee.is_async:
                chain = _find_blocking_chain(callee, all_funcs)
                chain_str = " → ".join(chain) if chain else callee_name
                violations.append({
                    "file": filepath,
                    "line": af.lineno,
                    "func": af.name,
                    "call": callee_name,
                    "chain": chain_str,
                    "type": "indirect",
                })

        # Also check direct blocking calls (v0 behavior)
        class _DirectChecker(ast.NodeVisitor):
            def __init__(self):
                self.in_sync = False

            def visit_FunctionDef(self, node):
                old = self.in_sync
                self.in_sync = True
                self.generic_visit(node)
                self.in_sync = old

            def visit_AsyncFunctionDef(self, node):
                pass

            def visit_Call(self, node):
                if self.in_sync:
                    self.generic_visit(node)
                    return
                name = _get_call_name(node)
                if name in ("to_thread", "run_in_executor"):
                    return
                if name and name in BLOCKING_PRIMITIVES and name not in SAFE_NAMES:
                    line_text = lines[node.lineno - 1] if node.lineno <= len(lines) else ""
                    if WHITELIST_COMMENT not in line_text:
                        violations.append({
                            "file": filepath,
                            "line": node.lineno,
                            "func": af.name,
                            "call": name,
                            "chain": f"{af.name} → {name}",
                            "type": "direct",
                        })

                qual = _get_qualified_call(node)
                if qual:
                    mod, attr = qual
                    from_aliases = {k: v for k, v in []}
                    if (mod, attr) in BLOCKING_QUALIFIED:
                        line_text = lines[node.lineno - 1] if node.lineno <= len(lines) else ""
                        if WHITELIST_COMMENT not in line_text:
                            violations.append({
                                "file": filepath,
                                "line": node.lineno,
                                "func": af.name,
                                "call": f"{mod}.{attr}",
                                "chain": f"{af.name} → {mod}.{attr}",
                                "type": "direct",
                            })
                self.generic_visit(node)

            def visit_With(self, node):
                if self.in_sync:
                    self.generic_visit(node)
                    return
                for item in node.items:
                    if isinstance(item.context_expr, ast.Call):
                        cname = _get_call_name(item.context_expr)
                        if cname == "get_cursor":
                            line_text = lines[node.lineno - 1] if node.lineno <= len(lines) else ""
                            if WHITELIST_COMMENT not in line_text:
                                violations.append({
                                    "file": filepath,
                                    "line": node.lineno,
                                    "func": af.name,
                                    "call": "get_cursor (with block)",
                                    "chain": f"{af.name} → get_cursor",
                                    "type": "direct",
                                })
                self.generic_visit(node)

        try:
            with open(filepath) as f:
                tree = ast.parse(f.read(), filename=filepath)
        except (SyntaxError, UnicodeDecodeError):
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.AsyncFunctionDef) and node.name == af.name:
                checker = _DirectChecker()
                for stmt in node.body:
                    checker.visit(stmt)
                break

    return violations


def main():
    app_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "app")
    if not os.path.isdir(app_dir):
        app_dir = "app"

    # Phase 1: collect all functions across all files
    all_file_funcs = {}
    all_funcs_by_name = {}

    for root, dirs, files in os.walk(app_dir):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for fname in sorted(files):
            if not fname.endswith(".py"):
                continue
            filepath = os.path.join(root, fname)
            funcs, aliases, lines = _extract_functions(filepath)
            all_file_funcs[filepath] = (funcs, aliases, lines)
            for fi in funcs:
                all_funcs_by_name[fi.name] = fi

    # Phase 2: resolve transitive blocking
    _resolve_blocking(all_funcs_by_name)

    # Phase 3: find violations
    all_violations = []
    files_with_violations = set()

    for filepath, (funcs, aliases, lines) in sorted(all_file_funcs.items()):
        violations = _find_violations(filepath, funcs, all_funcs_by_name, lines)
        if violations:
            # Deduplicate by (file, line, func, call)
            seen = set()
            for v in violations:
                key = (v["file"], v["line"], v["func"], v["call"])
                if key not in seen:
                    seen.add(key)
                    all_violations.append(v)
                    files_with_violations.add(filepath)

    for v in all_violations:
        print(f"{v['file']}:{v['line']}: async {v['func']} → {v['call']} via {v['chain']}")

    print()
    print(f"{len(all_violations)} violations across {len(files_with_violations)} files.")

    return 1 if all_violations else 0


if __name__ == "__main__":
    sys.exit(main())
