# Constitution Amendment v9.13 — Coupled-Write Modules Authorized

**Date:** 2026-05-11
**Status:** Proposed (resolves Blocker 1 of #185's P0 sweep blockers doc)
**Supersedes:** Extends v9.12; does not replace.

## Forcing function

v9.12 codified module-canonical as the direction for resolving live-path
drift. The dex_pool_ohlcv pilot (#179) verified the pattern works for
modules whose write surface matches worker.py inline 1:1. The
2026-05-11 P0 sweep continuation (#185 findings doc) surfaced
peg_snapshots_5m as a domain where the module's write surface is a
strict SUPERSET of worker.py inline — the module path additionally
writes volatility_surfaces from a days=90 fetch that the worker.py
inline path skips entirely.

Substrate confirms volatility_surfaces is a live data-layer table
with consumers:
  - server.py:7916 — API endpoint reads from it
  - catalog.py, index_simulator.py, component_replay.py — treat as
    first-class
  - storage_evaluation.py, state_growth.py — count it in capacity
    planning
  - 261 rows total, last write 2026-05-08 (3+ days stale at amendment
    time, exhibiting the same v9.11 dead-module pattern)

Without an explicit rule, the v9.12 sweep would either crystallize
the worker.py drift (mirror current behavior, leave vs broken
forever) or scope-creep into ad-hoc feature decisions per domain.

## Decision

Coupled-write modules are AUTHORIZED under v9.12 module-canonical when
both of the following hold:

1. **Shared upstream fetch.** The module's writes derive from a
   single (or unavoidable-coupled) upstream API call. Splitting into
   two modules would force duplicate fetches against the same
   external endpoint.

2. **Module owns ALL attestation domains it writes to.** A
   coupled-write module that writes to N tables must attest to N
   domains, one per table written, with status fields reflecting the
   work done.

When both hold, the module is the canonical live path for ALL of its
write tables, even if worker.py inline previously wrote only a subset.

## Rule for contributors

When refactoring an inline implementation to module-canonical:

1. Compare the module's write surface to worker.py inline's. If they
   match 1:1, proceed per v9.12 directly (the #179 dex_pool_ohlcv
   pattern).

2. If the module writes to MORE tables than the inline, check whether
   those additional tables have live consumers (read paths, API
   endpoints, downstream replay/scoring/provenance code). If yes, the
   module's broader surface is the truth and the inline is the drift.
   Restore the broader surface in the refactor.

3. If the module writes to MORE tables but those tables have NO
   consumers, the module's broader surface is dead feature surface.
   Either: trim the module to match the inline (preferred), OR
   propose a follow-up "do we want this feature back" PR.

4. Coupled-write modules attest to every domain they write. No
   "primary attest + side-effect write" pattern.

## Implication for peg_monitor (Blocker 1)

The peg_snapshots_5m + market_chart_history + volatility_surfaces
group meets the coupled-write criteria:
  - Shared fetch: /coins/{cg}/market_chart drives all three writes
    (days=1 for peg+mchart, days=90 for vs)
  - Live consumers: server.py:7916 serves vs; catalog and replay
    tooling treat it as first-class
  - Paid CoinGecko tier (500/min, 500k credits/month, currently 12.8%
    used) has ample headroom for the 2x fetch pattern

Refactor: peg_monitor.py becomes the canonical writer for all three
domains. worker.py inline at lines 1310-1373 is deleted. The
days=90 fetch is restored in the same call sequence.

## Open questions (deferred)

- exchange_snapshots: schema/list reconciliation is mechanical, not
  constitutional. No amendment needed; just decide whether to adopt
  the richer module schema and reconcile TOP_EXCHANGES vs worker.py's
  hardcoded list.
- psi_discoveries: semantic equivalence between three paths is a
  diagnostic question, not a constitutional one. Run a 24h
  side-by-side comparison; pick the one with correct semantics;
  retire the other two.

Both will be handled by separate Wave-N work after this amendment
merges.

## References

- v9.12 amendment (module-canonical decision)
- PR #179 (dex_pool_ohlcv pilot — 1:1 match case)
- PR #185 (P0 sweep blockers doc — surfacing the coupled-write
  question)
- substrate cite (2026-05-11): volatility_surfaces 261 rows, last
  write May 8, server.py:7916 consumer confirmed by code grep
