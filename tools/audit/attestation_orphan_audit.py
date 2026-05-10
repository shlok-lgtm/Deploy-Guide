#!/usr/bin/env python3
"""
Attestation orphan audit.

Cross-references three data sources:

1. The set of distinct ``domain`` values that have ever been written to the
   ``state_attestations`` table.
2. Every static call to ``attest_state(...)`` found by AST-walking ``app/``.
3. A static call-graph reachability check from the two active orchestrator
   roots (``run_slow_cycle_parallel`` in ``app/worker.py`` and
   ``run_enrichment_pipeline`` in ``app/enrichment_worker.py``).

A domain is **ok** if exactly one reachable call site writes it.
A domain is **publication_ready** if it is in ``PUBLICATION_READY_DOMAINS``,
no reachable call site writes it, and the four V9.13 §N invariants are
satisfied (deterministic compute, stable serialization, attestation at
compute time, documented spec). The CSV ``publication_ready_invariants``
column lists which invariants pass; ``--strict`` exits non-zero if any
publication_ready domain has missing invariants.
A domain is **orphaned** if no reachable call site writes it AND it is
not in ``PUBLICATION_READY_DOMAINS`` (the chain is dead — the table
column will go stale forever).
A domain is a **duplicate** if more than one reachable call site writes it
(double-attest, will inflate ``state_attestations`` and produce inconsistent
``batch_hash`` values per cycle).

Status precedence (highest to lowest):
  ok > duplicate > publication_ready > orphaned > unknown

So a domain that IS reachable and ALSO in the publication list still
shows ``ok`` — ``publication_ready`` is a fallback status, not an
override. This matters because if someone wires a publication-ready
domain into an orchestrator (buyer requires on-chain publishing), the
audit should report the domain as ordinary ``ok``, not silently keep it
in the publication-ready bucket.

Background
----------
The April 12 cluster outage taught us that orphaned attestation domains are
silent — the ``state_attestations`` row simply never gets a fresh hash, but
the rest of the pipeline keeps producing scores. The post-mortem identified
the ``actor_classification`` chain as orphaned (a Wave A relocation removed
the only caller). This audit catches that class of bug statically before
the next cycle quietly drifts.

Usage
-----
    python tools/audit/attestation_orphan_audit.py                # CSV to stdout
    python tools/audit/attestation_orphan_audit.py --summary      # summary only
    python tools/audit/attestation_orphan_audit.py --fail-on-orphans   # CI gate
    python tools/audit/attestation_orphan_audit.py --strict       # V9.13 §N gate

Exit codes:
    0  — all domains ok (or audit ran without --fail-on-orphans / --strict)
    1  — at least one domain is orphaned or duplicated and the gate is on,
         OR a publication_ready domain has missing invariants under --strict
    2  — usage / IO error

DB fallback:
    If ``DATABASE_URL`` is unset or the connection fails (non-auth), the
    script still produces a CSV — but the ``domain`` cell is blank for
    AST-derived rows that don't appear in the (empty) DB set, and the
    status is reported as ``unknown`` because it cannot be determined
    without the DB row inventory.

    If the DB connection fails with an *auth* error, the script aborts
    with exit code 2 — credentials may have been rotated.
"""

import argparse
import ast
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Every async entry point that is started by app.worker's main loop or
# scheduled via asyncio.create_task at startup must appear here. When you
# add a new background loop or scheduled task, add its module+function
# name to this list. Otherwise the audit will report its downstream
# state_attestations writes as orphans (false positive).
ACTIVE_ORCHESTRATORS = (
    # Cycle entrypoints (run from worker.py main loop)
    ("app.worker", "run_fast_cycle"),
    ("app.worker", "run_slow_cycle_parallel"),
    ("app.enrichment_worker", "run_enrichment_pipeline"),
    # Background loops (asyncio.create_task'd at startup)
    ("app.worker", "_diagnostic_loop"),
    ("app.lib.watchdog", "cancellation_watchdog"),
    ("app.data_layer.oracle_cadence_collector", "run_oracle_cadence_loop"),
    ("app.data_layer.holder_ingestion_collector", "holder_ingestion_background_loop"),
    ("app.data_layer.multichain_holder_collector", "multichain_holder_background_loop"),
    ("app.data_layer.wallet_presence_scanner", "wallet_presence_background_loop"),
    ("app.indexer.edges", "edge_builder_background_loop"),
    ("app.data_layer.transfer_edge_builder", "transfer_edge_builder_background_loop"),
    ("app.data_layer.trace_collector", "trace_collector_background_loop"),
    ("app.data_layer.approval_collector", "approval_collector_background_loop"),
    ("app.data_layer.mempool_watcher", "start_mempool_tasks"),
    ("app.utils.rpc_provider", "probe_rpc_capabilities"),
)

# Function name we're hunting. Verified against ``app/state_attestation.py``
# (per CLAUDE.md). If you rename the function, update this constant.
ATTEST_FN_NAME = "attest_state"

# V9.13 §N — Publication-Ready Composition Outputs.
#
# The domains below are intentionally not orchestrator-reachable. They
# are derived from attested SII and PSI state and computed only when
# requested via the FastAPI handlers. Without this exemption, the audit
# would (correctly) flag them as orphans every cycle.
#
# Each domain in this list MUST satisfy four invariants (see V9.13 §N
# of docs/drafts/basis_protocol_v9_13_constitution_amendment.md):
#
#   1. deterministic     — two calls with identical inputs produce
#                          structurally and byte-identically equal output
#                          (tests/test_composition_determinism.py)
#   2. serialized        — outputs serialize to byte-identical bytes for
#                          structurally equal inputs
#                          (tests/test_composition_serialization.py)
#   3. attested          — at least one attest_state(...) call site exists
#                          in the writer module (this audit's existing
#                          signal)
#   4. spec_documented   — docs/composition_spec.md exists and contains a
#                          section heading matching the domain name
#
# When a buyer requirement triggers on-chain publication, a domain
# graduates out of this list and into a normal orchestrator path. When
# a domain is added to this list, the four invariants must be satisfied
# at the same commit. When a domain is removed, the assertion is
# downgraded — record the reason in the V9.13 amendment changelog.
PUBLICATION_READY_DOMAINS = (
    "cqi_compositions",
    "rqs_composition",
    "rqs_compositions",
)

# Status precedence (highest first). See module docstring for rationale.
STATUSES = ("ok", "duplicate", "publication_ready", "orphaned", "unknown")


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------

def _parent_map(tree: ast.AST) -> dict[int, ast.AST]:
    parents: dict[int, ast.AST] = {}
    for parent in ast.walk(tree):
        for child in ast.iter_child_nodes(parent):
            parents[id(child)] = parent
    return parents


def _enclosing_function(node: ast.AST, parents: dict[int, ast.AST]) -> Optional[ast.AST]:
    cur: Optional[ast.AST] = parents.get(id(node))
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return cur
        cur = parents.get(id(cur))
    return None


def _module_name_for(path: Path, root: Path) -> str:
    """Return the dotted module name for a Python file under ``root``.
    Example: app/collectors/smart_contract.py -> app.collectors.smart_contract
    """
    rel = path.relative_to(root.parent if root.name == "app" else root)
    parts = list(rel.with_suffix("").parts)
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _attr_chain_name(node: ast.AST) -> Optional[str]:
    """Return the rightmost attribute or Name id, or None."""
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Name):
        return node.id
    return None


def _is_attest_state_call(call: ast.Call) -> bool:
    """True iff this Call node looks like ``attest_state(...)``.

    Matches:
      - ``attest_state("foo", ...)``
      - ``state_attestation.attest_state("foo", ...)``
      - ``functools.partial(attest_state, "foo", ...)``
      - ``loop.run_in_executor(None, attest_state, "foo", ...)``
      - ``asyncio.to_thread(attest_state, "foo", ...)``
    """
    f = call.func
    name = _attr_chain_name(f)
    if name == ATTEST_FN_NAME:
        return True
    return False


def _domain_from_call(call: ast.Call) -> Optional[str]:
    """Return the literal first positional argument (the domain string), if
    it is an ast.Constant str. Otherwise None."""
    if not call.args:
        return None
    arg0 = call.args[0]
    if isinstance(arg0, ast.Constant) and isinstance(arg0.value, str):
        return arg0.value
    return None


def _domain_from_indirect_call(call: ast.Call) -> Optional[str]:
    """Handle ``run_in_executor(None, attest_state, "domain", ...)`` and
    ``asyncio.to_thread(attest_state, "domain", ...)`` and
    ``partial(attest_state, "domain", ...)``: scan the args for a Name node
    referencing ``attest_state`` and pull the next positional string.
    """
    args = call.args
    for i, a in enumerate(args):
        if isinstance(a, ast.Name) and a.id == ATTEST_FN_NAME:
            # the next positional, if any, should be the domain
            for j in range(i + 1, len(args)):
                aj = args[j]
                # skip the literal None placeholder used by run_in_executor
                if isinstance(aj, ast.Constant) and isinstance(aj.value, str):
                    return aj.value
                # for partial, the very next arg is the domain
                if isinstance(aj, ast.Constant) and aj.value is None:
                    continue
                return None  # non-string first positional → unknown
    return None


def _is_indirect_attest_caller(call: ast.Call) -> bool:
    """True iff this Call is a wrapper that forwards to ``attest_state``
    (asyncio.to_thread, run_in_executor, functools.partial)."""
    f = call.func
    name = _attr_chain_name(f)
    if name not in {"to_thread", "run_in_executor", "partial"}:
        return False
    return any(
        isinstance(a, ast.Name) and a.id == ATTEST_FN_NAME
        for a in call.args
    )


# ---------------------------------------------------------------------------
# File walk: find attest_state call sites and build per-module call-graph
# ---------------------------------------------------------------------------

class CallSite:
    __slots__ = ("path", "line", "module", "function", "domain")

    def __init__(self, path: str, line: int, module: str, function: str, domain: Optional[str]):
        self.path = path
        self.line = line
        self.module = module
        self.function = function
        self.domain = domain

    def fq_name(self) -> str:
        return f"{self.module}.{self.function}"


# Wrappers whose first/second positional arg is the *real* callee. The
# tuple value is the index of the arg that holds the callable Name.
_FORWARDING_WRAPPERS: dict[str, tuple[int, ...]] = {
    "to_thread": (0,),         # asyncio.to_thread(fn, ...)
    "run_in_executor": (1,),   # loop.run_in_executor(None, fn, ...)
    "partial": (0,),           # functools.partial(fn, ...)
    "create_task": (0,),       # asyncio.create_task(fn())   — usually a Call, not Name
    "ensure_future": (0,),
    "gather": (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),  # asyncio.gather(c1, c2, ...)
    "wait_for": (0,),
}


def _name_args_as_callees(call: ast.Call) -> set[str]:
    """If this Call is a forwarding wrapper, return the set of Name ids
    found at the wrapper's "callable" arg slots. Otherwise empty set.
    """
    f_name = _attr_chain_name(call.func)
    if f_name not in _FORWARDING_WRAPPERS:
        return set()
    out: set[str] = set()
    slots = _FORWARDING_WRAPPERS[f_name]
    for idx in slots:
        if idx < len(call.args):
            a = call.args[idx]
            if isinstance(a, ast.Name):
                out.add(a.id)
            elif isinstance(a, ast.Call):
                # asyncio.create_task(fn()) — the inner call is the callee
                inner = _attr_chain_name(a.func)
                if inner:
                    out.add(inner)
    return out


def _kwarg_callable_callees(call: ast.Call) -> set[str]:
    """Return the set of Name ids passed as keyword args named ``func``,
    ``target``, ``coro``, or ``coro_fn``. Catches the EnrichmentTask
    construction pattern (``EnrichmentTask(func=_run_lsti, ...)``) and
    the ``_supervised_loop(name="x", coro_fn=fn, ...)`` pattern in
    worker.py.
    """
    out: set[str] = set()
    for kw in call.keywords:
        if kw.arg in {"func", "target", "coro", "coro_fn", "callback"}:
            v = kw.value
            if isinstance(v, ast.Name):
                out.add(v.id)
            elif isinstance(v, ast.Call):
                inner = _attr_chain_name(v.func)
                if inner:
                    out.add(inner)
    return out


def scan_file(path: Path, app_root: Path) -> tuple[list[CallSite], dict[str, set[str]]]:
    """Return (call_sites, callgraph_edges).

    callgraph_edges: { caller_fq_name -> set(callee_simple_name) }
    where caller_fq_name = "module.function" and callee_simple_name is the
    rightmost identifier of any Call.func, plus indirectly-passed callables
    (forwarding-wrapper positional args, EnrichmentTask(func=...) kwargs).
    """
    try:
        src = path.read_text(encoding="utf-8")
        tree = ast.parse(src, filename=str(path))
    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"# audit: skip {path}: {e}", file=sys.stderr)
        return [], {}

    parents = _parent_map(tree)
    module = _module_name_for(path, app_root)
    sites: list[CallSite] = []
    edges: dict[str, set[str]] = defaultdict(set)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = _enclosing_function(node, parents)
        fn_name = fn.name if fn is not None else "<module>"
        fq = f"{module}.{fn_name}"

        # Track call-graph: every Call gets its callee name added.
        callee = _attr_chain_name(node.func)
        if callee:
            edges[fq].add(callee)

        # Forwarding wrappers (asyncio.to_thread, run_in_executor, partial,
        # create_task, gather, ensure_future) — pull the wrapped callable's
        # name into edges too.
        edges[fq].update(_name_args_as_callees(node))

        # Keyword-arg callables: EnrichmentTask(func=_run_lsti) etc.
        edges[fq].update(_kwarg_callable_callees(node))

        # Find attest_state call sites (direct).
        if _is_attest_state_call(node):
            domain = _domain_from_call(node)
            sites.append(CallSite(
                path=str(path),
                line=node.lineno,
                module=module,
                function=fn_name,
                domain=domain,
            ))
            continue

        # Find indirect wrappers that forward to attest_state.
        if _is_indirect_attest_caller(node):
            domain = _domain_from_indirect_call(node)
            sites.append(CallSite(
                path=str(path),
                line=node.lineno,
                module=module,
                function=fn_name,
                domain=domain,
            ))

    # Also consider that nested functions defined inside another function
    # are typically *invoked* by the enclosing function (via `await fn()`
    # or stored in a structure passed to a registry). Add an edge from
    # the parent to every nested FunctionDef name so static reachability
    # treats nested closures as reachable when their parent is reachable.
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        parent_fn = _enclosing_function(node, parents)
        if parent_fn is None:
            continue
        parent_fq = f"{module}.{parent_fn.name}"
        edges[parent_fq].add(node.name)

    return sites, edges


def walk_app(app_root: Path) -> tuple[list[CallSite], dict[str, set[str]]]:
    all_sites: list[CallSite] = []
    all_edges: dict[str, set[str]] = defaultdict(set)
    for path in sorted(app_root.rglob("*.py")):
        if any(part.startswith(".") or part == "__pycache__" for part in path.parts):
            continue
        sites, edges = scan_file(path, app_root)
        all_sites.extend(sites)
        for k, v in edges.items():
            all_edges[k].update(v)
    return all_sites, all_edges


# ---------------------------------------------------------------------------
# Reachability: which (module.function) nodes are reachable from the
# orchestrator entry points by static call-name matching?
# ---------------------------------------------------------------------------

def build_function_index(edges: dict[str, set[str]]) -> dict[str, set[str]]:
    """Map simple function name -> set of fully-qualified declarations.

    A simple-name match is intentionally optimistic: any function in the
    codebase that bears the called name is treated as a candidate edge
    target. This over-approximates reachability, which is the safe side
    for an "orphaned" finding — false negatives (calling something
    orphaned) are far worse than false positives (calling something
    live).
    """
    by_name: dict[str, set[str]] = defaultdict(set)
    for fq in edges.keys():
        # fq is "module.path.fn_name"
        simple = fq.rsplit(".", 1)[-1]
        by_name[simple].add(fq)
    return by_name


def reachable_set(
    edges: dict[str, set[str]],
    roots: tuple[tuple[str, str], ...],
) -> set[str]:
    """BFS from each root over the call-graph edges.

    A node ``module.fn`` is in the reachable set iff there's a path from
    one of the roots to it via callee-name matching.
    """
    by_name = build_function_index(edges)

    seen: set[str] = set()
    stack: list[str] = []
    for mod, fn in roots:
        fq = f"{mod}.{fn}"
        if fq in edges or fq in by_name.get(fn, set()):
            stack.append(fq)
            seen.add(fq)

    while stack:
        cur = stack.pop()
        callees = edges.get(cur, set())
        for callee_simple in callees:
            for target_fq in by_name.get(callee_simple, set()):
                if target_fq not in seen:
                    seen.add(target_fq)
                    stack.append(target_fq)
    return seen


# ---------------------------------------------------------------------------
# DB query
# ---------------------------------------------------------------------------

def fetch_db_domains() -> tuple[Optional[set[str]], str]:
    """Return (set_of_domains, status_string).

    status_string is one of:
        "ok"                — fetch succeeded, set is populated
        "no_db_url"         — DATABASE_URL unset
        "auth_error"        — authentication failed (script should abort)
        "conn_error"        — connection error (other)
        "query_error"       — connect ok but query failed
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None, "no_db_url"

    try:
        import psycopg2  # type: ignore
    except ImportError:
        print("audit: psycopg2 not installed; skipping DB query", file=sys.stderr)
        return None, "conn_error"

    try:
        conn = psycopg2.connect(url, connect_timeout=10)
    except Exception as e:  # noqa: BLE001
        msg = str(e).lower()
        if any(tok in msg for tok in ("password", "authentication", "auth", "role", "permission denied")):
            return None, "auth_error"
        return None, "conn_error"

    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT domain FROM state_attestations;")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {r[0] for r in rows if r[0] is not None}, "ok"
    except Exception as e:  # noqa: BLE001
        try:
            conn.close()
        except Exception:
            pass
        print(f"audit: state_attestations query failed: {e}", file=sys.stderr)
        return None, "query_error"


# ---------------------------------------------------------------------------
# Publication-ready invariant checks (V9.13 §N)
# ---------------------------------------------------------------------------
#
# Each check is a presence-of-artifact check, not a deep verification. The
# tests in tests/test_composition_determinism.py and
# tests/test_composition_serialization.py do the deep verification — this
# audit just confirms those artifacts exist, so a missing test surfaces in
# the audit CSV (and under --strict, fails the build).

# All four invariants we check.
PUBLICATION_INVARIANTS = ("deterministic", "serialized", "attested", "spec_documented")


def _repo_root_from(audit_root: Path) -> Path:
    """The audit's ``root`` arg points at ``app/``. The artifacts we
    check live alongside it (``tests/`` and ``docs/``), at the repo
    root. Return that repo root."""
    audit_root = audit_root.resolve()
    if audit_root.name == "app":
        return audit_root.parent
    return audit_root


def check_publication_invariants(
    domain: str,
    audit_root: Path,
    sites: list[CallSite],
) -> list[str]:
    """Return the list of invariants that pass for ``domain``. Order
    matches PUBLICATION_INVARIANTS so the CSV output is deterministic.

    presence-of-artifact:
      - deterministic: tests/test_composition_determinism.py exists
      - serialized:    tests/test_composition_serialization.py exists
      - attested:      at least one attest_state(...) call site whose
                       domain literal matches (reuses the existing AST
                       sweep — no extra walk)
      - spec_documented: docs/composition_spec.md exists AND contains a
                       section heading whose text contains the domain
                       name (case-insensitive substring match)
    """
    repo = _repo_root_from(audit_root)
    present: list[str] = []

    if (repo / "tests" / "test_composition_determinism.py").exists():
        present.append("deterministic")
    if (repo / "tests" / "test_composition_serialization.py").exists():
        present.append("serialized")
    # Reuse the AST sweep — domain has at least one call site.
    if any(s.domain == domain for s in sites):
        present.append("attested")
    spec_path = repo / "docs" / "composition_spec.md"
    if spec_path.exists():
        try:
            spec_text = spec_path.read_text(encoding="utf-8")
            # Case-insensitive substring match on lines starting with '#'.
            for line in spec_text.splitlines():
                if line.lstrip().startswith("#") and domain.lower() in line.lower():
                    present.append("spec_documented")
                    break
        except (OSError, UnicodeDecodeError):
            pass

    return present


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def build_rows(
    sites: list[CallSite],
    reachable: set[str],
    db_domains: Optional[set[str]],
    audit_root: Optional[Path] = None,
) -> list[dict]:
    """Group call sites by domain and emit one row per domain.

    A site's "reachable" flag is True iff its enclosing function is in the
    reachable set. Status is computed from the *reachable* call site count
    only.
    """
    # Group by domain. Sites with literal=None go in a special "<unknown>"
    # bucket so they're still visible.
    by_domain: dict[Optional[str], list[CallSite]] = defaultdict(list)
    for s in sites:
        by_domain[s.domain].append(s)

    rows: list[dict] = []

    universe: set = set()
    for d in by_domain.keys():
        if d is not None:
            universe.add(d)
    if db_domains is not None:
        universe |= db_domains

    for domain in sorted(universe):
        these = by_domain.get(domain, [])
        reachable_sites = [
            s for s in these
            if s.fq_name() in reachable
        ]
        # Distinct reachable enclosing functions — multiple call sites
        # inside the same function (e.g. success+empty branches) count
        # once. The "duplicate" status is reserved for the case where
        # *separate* orchestrator-reachable functions both write the
        # same domain, which would inflate state_attestations.
        distinct_reachable_fns = {s.fq_name() for s in reachable_sites}
        all_locations = "; ".join(
            f"{s.path}:{s.line} ({s.fq_name()}, "
            f"reachable={'1' if s.fq_name() in reachable else '0'})"
            for s in these
        )
        n_distinct = len(distinct_reachable_fns)
        # Status precedence (highest first):
        #   ok > duplicate > publication_ready > orphaned > unknown
        # publication_ready is a fallback ONLY when n_distinct == 0 and
        # the domain is in the publication list. Reachable domains are
        # always ok/duplicate even if they happen to be in the list (a
        # domain that gets wired into an orchestrator should report as
        # ordinary ok, not silently keep its publication_ready status).
        #
        # Status assignment is driven by AST + reachability alone. The DB
        # inventory is used only to surface domains that exist in
        # state_attestations but have no AST call site (covered by the
        # `unknown` fallback in the AST-only-rows path below).
        publication_invariants: list[str] = []
        if n_distinct == 1:
            status = "ok"
        elif n_distinct >= 2:
            status = "duplicate"
        elif domain in PUBLICATION_READY_DOMAINS:
            status = "publication_ready"
            if audit_root is not None:
                publication_invariants = check_publication_invariants(
                    domain, audit_root, sites
                )
        elif these:
            # Has AST call sites but none reachable.
            status = "orphaned"
        else:
            # In DB inventory only, no AST evidence.
            status = "unknown"
        rows.append({
            "domain": domain,
            "call_sites": all_locations,
            "reachable_from_active_orchestrator": str(n_distinct),
            "status": status,
            "publication_ready_invariants": ",".join(publication_invariants),
        })

    # Also include AST-only sites whose domain is not a literal (None bucket).
    if None in by_domain:
        these = by_domain[None]
        all_locations = "; ".join(
            f"{s.path}:{s.line} ({s.fq_name()}, "
            f"reachable={'1' if s.fq_name() in reachable else '0'})"
            for s in these
        )
        distinct_reachable_fns = {s.fq_name() for s in these if s.fq_name() in reachable}
        rows.append({
            "domain": "<non-literal>",
            "call_sites": all_locations,
            "reachable_from_active_orchestrator": str(len(distinct_reachable_fns)),
            "status": "unknown",
            "publication_ready_invariants": "",
        })

    return rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

CSV_FIELDS = (
    "domain",
    "call_sites",
    "reachable_from_active_orchestrator",
    "status",
    "publication_ready_invariants",
)


def write_csv(rows: list[dict], out) -> None:
    writer = csv.DictWriter(out, fieldnames=list(CSV_FIELDS))
    writer.writeheader()
    for r in rows:
        writer.writerow(r)


def summary_counts(rows: list[dict]) -> dict[str, int]:
    counts = {s: 0 for s in STATUSES}
    for r in rows:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    counts["TOTAL"] = len(rows)
    # publication_ready_with_gaps: any publication_ready row missing one
    # or more invariants. This is the metric --strict gates on.
    gaps = 0
    for r in rows:
        if r["status"] != "publication_ready":
            continue
        present = set((r.get("publication_ready_invariants") or "").split(",")) - {""}
        if len(present) < len(PUBLICATION_INVARIANTS):
            gaps += 1
    counts["publication_ready_with_gaps"] = gaps
    return counts


def print_summary(counts: dict[str, int], db_status: str, out=sys.stdout) -> None:
    print("# attestation orphan audit summary", file=out)
    print(f"#   db_status: {db_status}", file=out)
    print(f"#   total domains: {counts['TOTAL']}", file=out)
    for s in STATUSES:
        print(f"#   {s:<20} {counts.get(s, 0)}", file=out)
    # V9.13 §N — publication-ready domains with missing invariants.
    print(
        f"#   {'publication_ready_with_gaps':<20} "
        f"{counts.get('publication_ready_with_gaps', 0)}",
        file=out,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(
        description=(
            "Attestation orphan audit (state_attestations cross-check). "
            "V9.13 §N publication_ready domains are exempted from the "
            "orphan gate — see PUBLICATION_READY_DOMAINS for the list."
        ),
    )
    p.add_argument("root", nargs="?", default="app",
                   help="Root directory to AST-scan (default: app)")
    p.add_argument("--summary", action="store_true",
                   help="Print summary counts only (no CSV body)")
    p.add_argument(
        "--fail-on-orphans",
        action="store_true",
        help=(
            "CI gate: exit 1 if any domain is orphaned/duplicated/unknown. "
            "Domains in PUBLICATION_READY_DOMAINS no longer count as "
            "orphans (V9.13 §N)."
        ),
    )
    p.add_argument(
        "--strict",
        action="store_true",
        help=(
            "V9.13 §N CI gate: exit 1 if any publication_ready domain "
            "is missing one or more of the four invariants "
            "(deterministic, serialized, attested, spec_documented)."
        ),
    )
    args = p.parse_args()

    app_root = Path(args.root)
    if not app_root.exists() or not app_root.is_dir():
        print(f"audit: root '{app_root}' not found or not a directory", file=sys.stderr)
        return 2

    # 1. AST scan ----------------------------------------------------------
    sites, edges = walk_app(app_root)

    # 2. Reachability -------------------------------------------------------
    reachable = reachable_set(edges, ACTIVE_ORCHESTRATORS)

    # 3. DB inventory --------------------------------------------------------
    db_domains, db_status = fetch_db_domains()
    if db_status == "auth_error":
        print(
            "audit: DATABASE_URL connect failed with auth error. "
            "Credentials may have been rotated. Aborting.",
            file=sys.stderr,
        )
        return 2
    if db_status == "no_db_url":
        print(
            "audit: DATABASE_URL not set or unreachable -- "
            "skipping DB query, AST-only output.",
            file=sys.stderr,
        )
    elif db_status != "ok":
        print(
            f"audit: DB query unavailable ({db_status}) -- "
            "skipping DB query, AST-only output.",
            file=sys.stderr,
        )

    # 4. Aggregate -----------------------------------------------------------
    rows = build_rows(sites, reachable, db_domains, audit_root=app_root)
    counts = summary_counts(rows)

    # 5. Emit ---------------------------------------------------------------
    if args.summary:
        print_summary(counts, db_status)
    else:
        write_csv(rows, sys.stdout)
        print_summary(counts, db_status, out=sys.stderr)

    # 6. CI gates -----------------------------------------------------------
    # --fail-on-orphans: domains in PUBLICATION_READY_DOMAINS are exempt
    # (their orphan-by-design status is the V9.13 §N policy).
    if args.fail_on_orphans:
        non_ok = [
            r for r in rows
            if r["status"] not in ("ok", "publication_ready")
        ]
        if non_ok:
            print(
                f"audit: {len(non_ok)} non-ok domains "
                f"(orphaned/duplicate/unknown) — gate failed",
                file=sys.stderr,
            )
            return 1

    # --strict: every publication_ready domain must satisfy all four
    # invariants. Fail loudly otherwise — the assertion is real, not
    # cosmetic.
    if args.strict:
        gaps = [
            r for r in rows
            if r["status"] == "publication_ready"
            and len(set((r.get("publication_ready_invariants") or "").split(","))
                    - {""}) < len(PUBLICATION_INVARIANTS)
        ]
        if gaps:
            print(
                f"audit: --strict failed — {len(gaps)} publication_ready "
                f"domain(s) missing invariants: "
                + "; ".join(
                    f"{r['domain']}=[{r['publication_ready_invariants']}]"
                    for r in gaps
                ),
                file=sys.stderr,
            )
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
