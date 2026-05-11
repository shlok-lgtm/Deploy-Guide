# v9.12 module-canonical migration plan — 2026-05-11

**Purpose:** Phase 2.1 of the all-day orchestrator. Enumerates every
`attest_state(...)` and `attest_data_batch(...)` call site in the
basis-hub Python tree, classifies each domain as `SINGLE_WRITER`
(canonical) or `DUAL_WRITER` (worker.py inline + module), and lays out
the priority order for the v9.12 refactor sweep.

Sources
-------
- `app/worker.py` attest sites (the v9.11 "live path")
- `main.py` attest sites (legacy live paths surfaced by Wave 1/3)
- `app/data_layer/**` and `app/collectors/**` modules

Method: `grep -rn "attest_state\|attest_data_batch" app/ main.py` at
2026-05-11T16:30Z; cross-referenced with today's Waves 1-7 PR list and
`docs/audits/2026-05-11-data-layer-cadence-audit.md`.

## Domains classified

### DUAL_WRITER (needs consolidation)

Both `app/worker.py` (or `main.py`) AND the canonical module write
to the same domain. The worker.py inline is the live path (v9.11);
the module write is dead-in-steady-state. v9.12 says modules are
canonical → delete the worker.py inline, keep the module call,
schedule the module from worker.py.

| domain | worker/main site | module site | priority¹ | notes |
|---|---|---|---|---|
| `psi_discoveries` | `worker.py:2548`, `main.py:262` | (none — enrichment task wraps `app/discovery.py`) | **P0** | 3-PR history (#137, #150, #157). Highest-friction case. |
| `data_layer:peg_snapshots_5m` | `worker.py:1371`, also Wave 5b heartbeat at 2787 | `app/data_layer/peg_monitor.py:382` | **P0** | Wave 5a + 5b both touched it. |
| `data_layer:exchange_snapshots` | `worker.py:1261` | `app/data_layer/exchange_collector.py:274` | **P0** | Wave 5a hoist. |
| ~~`data_layer:dex_pool_ohlcv`~~ ✅ | ~~worker.py inline~~ removed | `app/data_layer/ohlcv_collector.py::run_ohlcv_collection_scheduled` | **P0 → DONE** | Pilot landed this session; module-canonical via `run_ohlcv_collection_scheduled()`. Dispatcher heartbeat at worker.py:2778 left in place until Phase 2.3. |
| `wallets` | `worker.py:2082`, also `worker.py:2787` (heartbeat) | — (driven by `app/indexer/pipeline.py`) | **P1** | Wave 1 + Wave 3 + Wave 5b. |
| `web_research` | `worker.py:1960`, also 2787 | `app/collectors/web_research.py:563` | **P1** | Wave 1 + Wave 3 + Wave 5b. |
| `psi_components` | `worker.py:1100`, `main.py:224` | (enrichment task) | **P1** | Two live paths post-Wave-1. |
| `cda_extractions` | `main.py:167` | (enrichment task wraps `app/services/cda_collector.py`) | **P2** | Single legacy main.py site; module exists. |
| `wallet_profiles` | `main.py:301` | (`app/wallet_profile.py`) | **P2** | |
| `actors` | `main.py:317` | (`app/agent/`) | **P2** | |
| `edges` | `main.py:456` | (`app/indexer/edges.py`) | **P2** | |

¹ Priority key:
- **P0** — most-fraught, multiple PRs in flight on 2026-05-11; should
  be the first refactor wave to validate the v9.12 pattern.
- **P1** — recurred in today's Waves but stabilized; refactor once
  P0 pattern is proven.
- **P2** — main.py legacy entries, no worker.py duplication; refactor
  last (lowest risk, lowest signal).

### SINGLE_WRITER (already canonical — no refactor needed)

These already have exactly one writer. v9.12 leaves them alone
EXCEPT to (a) confirm the writer's location in the manifest, (b)
ensure no future PR re-introduces a worker.py inline.

| domain | sole writer | notes |
|---|---|---|
| `sii_components` | `worker.py:665` (sole site; modules don't write) | Worker-canonical OK per v9.12 — this is the orchestrator-level scoring loop, not a data domain. Marked SINGLE_WRITER. |
| `data_layer:liquidity_depth` | `app/data_layer/liquidity_collector.py:381` | Already module-canonical. ✅ |
| `data_layer:token_approvals` | `app/data_layer/approval_collector.py:236` | ✅ |
| `data_layer:yield_snapshots` | `app/data_layer/yield_collector.py:273` | ✅ |
| `data_layer:mint_burn_events` | `app/data_layer/mint_burn_collector.py:322` | ✅ (data dry, op-followup #1 — not a v9.12 issue) |
| `data_layer:governance_proposals` | `app/data_layer/governance_collector.py:473` | ✅ |
| `data_layer:bridge_flows` | `app/data_layer/bridge_flow_collector.py:236` | ✅ (deferred per v9.3) |
| `data_layer:oracle_cadence` | `app/data_layer/oracle_cadence_collector.py:176` | ✅ |
| `data_layer:wallet_holdings` | `app/data_layer/holder_discovery.py:266` | ✅ |
| `data_layer:market_chart_history` | `app/data_layer/markets_collector.py:298` + `market_chart_backfill.py:359` | Two module sites but BOTH module-canonical — different invocation paths (cron-style backfill vs live update). Not a v9.12 issue. |
| `data_layer:protocol_traces` | `app/data_layer/trace_collector.py:270` | ✅ |
| `data_layer:contract_surveillance` | `app/data_layer/contract_surveillance.py:396` | ✅ |
| `data_layer:wallet_holder_discovery` | `app/data_layer/holder_ingestion_collector.py:402` | ✅ (weekly cadence, Wave 8 diagnosis) |
| `data_layer:entity_snapshots_hourly` | `app/data_layer/entity_snapshots.py:326` | ✅ (Migration 107 fix landed) |
| `data_layer:wallet_chain_presence` | `app/data_layer/multichain_holder_collector.py:251` + `wallet_presence_scanner.py:160` | Two module sites, different scanners; module-canonical. |
| `mempool_observations` | `app/data_layer/mempool_watcher.py:800` | ✅ |
| `oracle_readings` | `app/collectors/oracle_behavior.py:629` | ✅ |
| `oracle_stress_events` | `app/collectors/oracle_behavior.py:471,679` | ✅ |
| `sanctions_screening` | `app/collectors/sanctions_screening.py:198` | ✅ |
| `clustered_concentration` | `app/collectors/clustered_concentration.py:339` | ✅ |
| `governance_events` | `app/collectors/governance_events.py:443,448` | ✅ |
| `enforcement_records` | `app/collectors/enforcement_history.py:273` | ✅ |
| `exchange_health` | `app/collectors/exchange_health.py:303` | ✅ |
| `parent_company_financials` | `app/collectors/parent_company_financials.py:285` | ✅ |
| `flows` | `app/collectors/flows.py:433` | ✅ |
| `tti_components` | `app/collectors/tti_collector.py:698,703` | ✅ |
| `contract_dependencies(_snapshot)` | `app/collectors/contract_dependencies.py:375,460` | ✅ |
| `lsti_components` | `app/collectors/lst_collector.py:671,676` | ✅ |
| `validator_performance` | `app/collectors/rated_validators.py:227` | ✅ |
| `dex_pool_data` | `app/collectors/dex_pools.py:426,431` | ✅ |
| `rpi_components` | (`_emit_slow_cycle_heartbeats` in worker.py) | Wave 7 heartbeat lives in worker.py — this is a v9.12 candidate but lower priority because the heartbeat shape is intentionally orchestrator-level. Keep as worker-canonical for now and revisit. |

## Refactor sweep order

Per orchestrator §2.2: priority order P0 → P1 → P2. Each PR follows
the template:

1. Branch `refactor/v9-12-<domain-slug>`.
2. Move write logic from worker.py inline → module function.
   Delete the inline implementation.
3. worker.py main loop calls the module function on the same cadence.
4. The attest call lives inside the module function, not in worker.py.
5. PR body includes substrate gate per `.github/PULL_REQUEST_TEMPLATE.md`
   (Pre-deploy + Post-deploy substrate quoted after cycle elapse).
6. After merge + deploy + cycle elapse: query state_attestations for
   freshness; confirm COUNT(*) incremented.

Halt conditions:
- More than 3 consecutive PRs fail their substrate verification.
- A refactor PR causes any previously-fresh domain to regress.
- A refactor PR's diff exceeds 500 lines (split it).

### Domain queue (P0 first)

1. `data_layer:dex_pool_ohlcv` — clearest case (3 worker.py sites,
   1 module). Pilot the v9.12 pattern here.
2. `data_layer:peg_snapshots_5m` — module is `peg_monitor.py`; the
   worker.py inline reads from `peg_snapshots_5m` after a separate
   write block. Coordinate with #2 above.
3. `data_layer:exchange_snapshots` — module is `exchange_collector.py`;
   similar shape.
4. `psi_discoveries` — three live paths (worker.py:2548, main.py:262,
   and the enrichment task at app/discovery.py). Highest historical
   friction. Coordinate carefully — verify all three retire.
5. `wallets` — driven by pipeline; the worker.py attest is the
   wave-3 + wave-5b heartbeat. Move into the pipeline's own status
   surface.
6. `web_research` — worker.py:1960 + Wave 5b heartbeat; module
   exists at `collectors/web_research.py:563`. Coordinate.
7. `psi_components` — two live paths (worker.py:1100, main.py:224);
   enrichment task in PSI scorer.
8. `cda_extractions`, `wallet_profiles`, `actors`, `edges` —
   main.py legacy entries. Refactor as a single PR if diff < 500
   lines (likely fine since each is one attest call).

### Dispatcher collapse (Phase 2.3 — last)

After all P0 + P1 domains migrate, `run_slow_cycle` and
`run_slow_cycle_parallel` should have no domain-specific work left.
The `_emit_slow_cycle_heartbeats` helper (Wave 5b/7) is the last
worker.py-resident attest logic; once the underlying domains are
module-canonical with their own heartbeats, the helper can be
deleted and the dispatcher collapsed.

Per orchestrator: do NOT collapse until every P0 + P1 + (most) P2
domain in §Refactor sweep order shows fresh attestations within
expected cadence. The legacy fallback may still be serving them.

## Session scope note

This audit lists the entire v9.12 migration surface but the
orchestrator session's 15-PR ceiling limits how many refactors can
land today. Likely landing this session:

- 2.1 audit (this PR)
- 1-2 P0 pilots (likely `dex_pool_ohlcv` + `peg_snapshots_5m`)
- Closeout punchlist + amendment proposal

Remaining P0 + P1 + P2 refactors carry to follow-up sessions, queued
via this audit doc. The dispatcher collapse (2.3) **cannot** land
this session because it requires the full P0 + P1 sweep to be
verified — substrate verification of each refactor is gated by the
cycle elapse (30 min – 2h per domain).

## Cross-references

- v9.11 amendment (worker-authoritative live path)
- v9.12 amendment (module-canonical live path)
- `docs/basis_punchlist_2026_05_11.md` Waves 1-7 + lessons 1-10
- `docs/audits/2026-05-11-data-layer-cadence-audit.md`
- `docs/audits/2026-05-11-enrichment-task-budget-audit.md`
