# Constitution Amendment v9.11 — Worker-Authoritative Live Path

**Date:** 2026-05-11
**Status:** Proposed (post-mortem of the 2026-05-11 Wave 1→3 staleness triage)
**Supersedes:** (none — codifies an architectural rule that was implicit before)

## Forcing function

Between Wave 1 (#137–#146) and Wave 3 (#153–#157) on 2026-05-11, the
same diagnostic recurred on 7 domains: a fix patched the canonical
implementation in app/data_layer/ or app/collectors/, the worker
deployed the new code, and state_attestations never advanced. The
fix was dead code in production.

Exemplar: psi_discoveries took THREE PRs (#137 enrichment task, #150
main.py legacy path, #157 worker.py:2468 inline). Only the last was
the live path. The first two never run in steady state because
worker.py keeps protocol_collateral_exposure fresh, which closes the
db_gates on both prior implementations.

Same shape on data_layer:peg_snapshots_5m / exchange_snapshots /
dex_pool_ohlcv (#153 — three inline INSERTs in worker.py never called
attest_data_batch), mempool_capture_status (#154 — watcher
self-disabled, heartbeat moved to emit_24h_summary), wallets (#155 —
4 early-return paths bypass inner attest), web_research (#156 —
MAX-aggregated db_gate closes when ANY index_id is fresh).

## Decision

`app/worker.py` is the canonical live path for hot operations — the
operations that fire every fast cycle and keep core tables fresh. For
these operations, worker.py's inline implementation is what runs in
production. Modules in app/data_layer/ and app/collectors/ that share
entry-point names with worker.py inline blocks are
**enrichment-pipeline utilities**, not the live path.

This describes how Basis works today, codified so future contributors
don't rediscover it in a Wave-N PR.

## Implication for db_gates

Enrichment tasks routinely gate on table freshness (MAX(updated_at) >
NOW() - INTERVAL '...'). In steady state these gates are closed
because worker.py keeps the tables fresh. This is intentional —
the enrichment path is for catching up after outages, not
steady-state operation.

Consequence: an attest call inside a gated enrichment task will not
fire in steady state. Attestations belong on the live path
(worker.py) or must be reachable independently of any freshness gate.

## Rule for contributors

When adding or moving an attest_state(...) call:

1. Start at app/worker.py and trace the call chain for the cadence
   you care about (every cycle, hourly, daily summary).
2. Find where the operation executes in steady state. An inline
   block in worker.py is the live path.
3. Place the attest call on the live path.
4. The canonical module may have a duplicate attest call but likely
   runs only during enrichment catch-up. Patching only the module is
   a no-op for steady-state freshness.

If unsure: query state_attestations for the target domain. If your
attest call has fired recently, you're on the live path. If not, look
for the worker.py inline block.

## Open question (deferred)

Whether to refactor away the duplication is left to a future
amendment. Two viable directions: module-canonical (worker.py invokes
modules, modules become truth) or worker-canonical (delete the
modules, worker.py inline is authoritative). Either reduces surface
area. The status quo is acceptable if the rule above is followed.

## References

- Wave 1: PRs #137–#146
- Wave 2: PRs #147–#151
- Wave 3: PRs #153–#157
- Lesson #4 of docs/basis_punchlist_2026_05_11.md (two-path attest sites)
