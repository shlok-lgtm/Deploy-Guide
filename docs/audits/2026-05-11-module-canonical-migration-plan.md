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
| ~~`data_layer:peg_snapshots_5m` (+ `market_chart_history` + `volatility_surfaces`)~~ ✅ | ~~worker.py:1310-1373 inline~~ removed | `app/data_layer/peg_monitor.py::run_peg_monitoring_scheduled` | **P0 → DONE** | First v9.13 coupled-write refactor. #188 made the module canonical writer for all 3 domains; this PR adds the scheduled wrapper (freshness gate + 3-domain skip/error attest) and routes worker, enrichment_worker, and main.py through it. Dispatcher heartbeat at worker.py:2787 left in place until Phase 2.3. |
| ~~`data_layer:exchange_snapshots`~~ ✅ | ~~worker.py:1186-1246 inline~~ removed | `app/data_layer/exchange_collector.py::run_exchange_collection_scheduled` | **P0 → DONE** | Q1 answered by #197 (migration 109 — schema replay). Q2 answered: 15 exchanges (worker._EX list), `_EX_FIX` legacy-slug remap ported into module. 35 additional exchanges deferred to Q2-extension (see below). enrichment_worker.py task entry removed (was kept closed by inline; lesson 6 family). main.py daily lambda switched. Dispatcher heartbeat at worker.py:2787 left in place until Phase 2.3. |
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

---

## P0 sweep blockers — lesson 10 findings (2026-05-11 21:20Z)

After PR #179 (`dex_pool_ohlcv`) verified clean, an attempted
continuation of the P0 sweep (peg_snapshots_5m / exchange_snapshots /
psi_discoveries) was halted before any refactor code was written.
Reading each module first (lesson 10) surfaced per-domain
architectural decisions that the v9.12 pattern from #179 does NOT
generalize over. Each blocker is an operator decision, not a coding
question.

### Blocker 1 — `data_layer:peg_snapshots_5m` (also implicates `data_layer:market_chart_history`)

**Live path** (worker.py:1310-1373, ~60 lines): one `await
client.get(/coins/{cg}/market_chart, days=1)` per stablecoin → writes
to BOTH `peg_snapshots_5m` AND `market_chart_history` in the same
loop via `_bulk_insert_peg_and_mchart()`. Wave 5a attest hoisted out
of the outer try (#162). No volatility surfaces.

**Module path** (`app/data_layer/peg_monitor.py::run_peg_monitoring`):
fetches `/market_chart` days=1 AND days=90 per stablecoin → writes
`peg_snapshots_5m` and `volatility_surfaces` (1d + 90d). Does **not**
write `market_chart_history`. Attests `peg_snapshots_5m`.

Symmetric difference (what each path uniquely does):
- worker.py only: writes `market_chart_history` (~13k rows/cycle)
- module only: fetches days=90, writes `volatility_surfaces`

Decision needed: should the v9.12 module own BOTH `peg_snapshots_5m`
and `market_chart_history` (coupled-write via one fetch, what the
worker.py inline already does), or should `market_chart_history` get
a separate module/scheduler entry that does its own fetch (clean
separation, double the CG /market_chart calls)?

Risk: peg is the 30% weight component of SII. A botched refactor
regresses peg freshness and silently corrupts the score. **Highest
blast radius of any v9.12 candidate.**

### Blocker 2 — `data_layer:exchange_snapshots`

**Live path** (worker.py:1203-1263, ~60 lines): hardcoded 15-exchange
list with `_EX_FIX` corrections (coinbase→gdax, htx→huobi, okx→okex,
mexc→mxc). Stores 8 columns: `exchange_id, name, trust_score,
trust_score_rank, trade_volume_24h_btc, year_established, country,
trading_pairs (count)`.

**Module path** (`app/data_layer/exchange_collector.py::run_exchange_collection`):
iterates a `TOP_EXCHANGES` constant (not verified to match the
worker.py list). Stores richer rows including
`trade_volume_24h_usd` (estimated from BTC × 65000), `stablecoin_pairs`
(top 50 pairs as JSON), `raw_data` jsonb with public_notice /
alert_notice / status_updates, plus a 30d volume history call per
exchange.

Decision needed: (a) is the `exchange_snapshots` table schema
prepared for `raw_data` jsonb and `stablecoin_pairs`?
(b) does the operator want the richer data shape on prod? (c) does
`TOP_EXCHANGES` match the hardcoded worker.py list (same coverage)
or differ (silent coverage change)?

Risk: silent data-shape change at the storage layer; some
downstream consumers may rely on the 8-column shape.

### Blocker 3 — `psi_discoveries`

Three live paths (per #137 / #150 / #157 history):
- `app/discovery.py` enrichment task
- `main.py:262` legacy block
- `app/worker.py:2548` inline (the actual live path per Wave 3)

Each was patched in a separate wave. Their semantic equivalence has
never been verified — each may produce subtly different `payload`
shapes or different sets of discoveries.

Decision needed: which of the three is the source of truth for
"what counts as a psi_discovery"? The refactor cannot be a
mechanical merge without resolving this.

Risk: silent semantic drift in the discovery feed. Lower blast
radius than peg (psi_discoveries doesn't gate SII), but high
correctness sensitivity (discovery is a publication signal).

### Recommendation

These three are NOT v9.12 mechanical refactors; they are design
decisions that change what data lands in prod. Do not proceed in
the same shape as #179.

Suggested per-domain path:
- **peg/mchart:** propose a v9.13 micro-amendment that codifies
  "coupled-write modules are allowed when they share a single
  upstream fetch; the module owns both attestation domains." Then
  refactor peg_monitor to also write market_chart_history.
- **exchange_snapshots:** start by reconciling `TOP_EXCHANGES` ↔
  worker.py's hardcoded list and confirming schema acceptance of
  the module's richer rows. Then refactor.
- **psi_discoveries:** diagnostic pass first — compare the three
  paths' actual outputs over the next 24h of substrate. Then pick
  one as canonical and retire the other two.

The remaining P1/P2 domains (`wallets`, `web_research`,
`psi_components`, `cda_extractions`, `wallet_profiles`, `actors`,
`edges`) are likely each their own variant of the same problem.
Each needs the lesson-10 reading before any refactor commit lands.

Phase 2.3 dispatcher collapse remains the last item in the queue;
it is blocked on every P0+P1 domain reaching SINGLE_WRITER state.
The shape of "what does SINGLE_WRITER mean for peg+mchart coupled-write?"
is the unresolved design question gating the whole sweep.

---

## Blocker 2 / Q2-extension — additional 35 exchanges (deferred)

The v9.12 exchange_snapshots refactor (#197 + the second-PR refactor)
adopts the **15-exchange list** that matches worker.py's pre-v9.12
`_EX` (binance, coinbase-exchange, okx, bybit_spot, kraken, kucoin,
gate, bitget, htx, crypto_com, mexc, bitfinex, bitstamp, gemini,
lbank). The module's original `TOP_EXCHANGES` had **50 ids** — the
35 not in worker._EX are deferred from the live writer until an
operator decision on coverage expansion.

Deferred ids (35), grouped by region/tier as they appeared in the
pre-refactor `TOP_EXCHANGES`:

```
# Tier-2 global volume (5):
upbit, bithumb, whitebit, bitrue, poloniex

# Specialty / derivatives (5):
hashkey-exchange, bitmart, phemex, deribit, bitflyer

# Regional KR/JP (5):
indodax, korbit, exmo, btcturk, tidex

# Regional KR/JP (5):
coinone, probit-exchange, bitbank, zaif, coincheck

# Tier-3 / legacy (5):
okcoin, gopax, liquid, btcbox, bkex

# Tier-3 / smaller (5):
latoken, hotbit, coinex, bigone, digifinex

# Newer / aggregators (5):
xt, deepcoin, toobit, bingx, bitvenus
```

Re-enabling: add the ids back to `TOP_EXCHANGES` in
`app/data_layer/exchange_collector.py`. No schema change needed —
the schema (post-migration 109) already supports the richer
column shape. If any of the deferred ids has a CG-legacy slug,
add it to `_EX_FIX` at the same time.

Cost: +35 `/exchanges/{id}` + `/exchanges/{id}/volume_chart/30`
calls per fast cycle (~80 calls / cycle / hour). CG pro tier
(500/min, 500k credits/month) has ample headroom. Worth a brief
budget check against the daily usage tracker before flipping.
