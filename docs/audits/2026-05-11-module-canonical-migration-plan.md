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

## Blocker 2 — `exchange_snapshots`: investigation findings (2026-05-12)

Diagnostic pass per the P0 sweep continuation. **No refactor**; this
section reframes the design questions surfaced in #185 based on actual
substrate. The original Blocker 2 hypothesis ("module writes richer
rows than worker") is half-correct — it's also writing rows to a
schema that no longer has the matching columns.

### 1. Live paths — column-by-column write comparison

**worker.py:1186-1222** (`run_fast_cycle`, lines 1186-1212 is the
write block):

  - Hardcoded list `_EX` (15 ids), `_EX_FIX` remap for the four CG
    legacy slugs (`coinbase-exchange→gdax`, `okx→okex`, `htx→huobi`,
    `mexc→mxc`).
  - INSERT at worker.py:1206-1212 writes **8 data columns**:
    `(exchange_id, name, trust_score, trust_score_rank,
    trade_volume_24h_btc, year_established, country, trading_pairs,
    snapshot_at NOW())`.
  - No `ON CONFLICT` clause; relies on `snapshot_at NOW()` for
    unique-constraint avoidance.
  - Attest fires from worker.py:1244 unconditionally outside the
    outer try (Wave 5a hoist).

**app/data_layer/exchange_collector.py** (`run_exchange_collection`,
lines 182-298):

  - `TOP_EXCHANGES` constant at line 30 — **50 ids**, first 15 are
    a strict superset of worker's `_EX` (identical order), then 35
    additions (`upbit, bithumb, whitebit, … bitvenus`).
  - **No `_EX_FIX` remap** — the module calls `/exchanges/{cg_id}`
    directly. For `coinbase-exchange`, `okx`, `htx`, `mexc` the CG
    legacy-slug endpoints would 404 (the `_fetch_exchange_data`
    helper handles that as empty-dict, continues silently — see
    line 70-82 + 200-201).
  - INSERT at exchange_collector.py:153-171 writes **12 data
    columns**: worker's 8 PLUS `trade_volume_24h_usd`,
    `has_trading_incentive`, `stablecoin_pairs` (jsonb),
    `raw_data` (jsonb).
  - `ON CONFLICT (exchange_id, snapshot_at) DO UPDATE`.
  - Attest fires from exchange_collector.py:274.

### 2. Schema — substrate cite

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name='exchange_snapshots' AND table_schema='public'
ORDER BY ordinal_position;
```

Result (11 columns total):

  id, exchange_id, name, trust_score, trust_score_rank,
  trade_volume_24h_btc, year_established, country, trading_pairs,
  snapshot_at, provenance_proof_id

**Migration 058** (`058_universal_data_layer.sql:208-224`) declares
14 columns — the actual schema is **missing 4**:

  - `trade_volume_24h_usd` NUMERIC          ← module writes this
  - `has_trading_incentive` BOOLEAN         ← module writes this
  - `stablecoin_pairs` JSONB                ← module writes this
  - `raw_data` JSONB                        ← module writes this

`migrations` table records 058 as applied on 2026-04-13. This is
the **same pg_dump silent-drift pattern as Wave 2.5 / Wave 9c**:
migration recorded as applied, table created, but partial DDL
content lost across the dump/restore.

`provenance_proof_id` (from migration 059) is present in schema
but written by neither writer path — it's back-filled by
`link_batch_to_proof` from the attestation pipeline.

### 3. Substrate — what's actually in the table (lesson 8: COUNT before MAX)

```sql
SELECT COUNT(*),
       COUNT(DISTINCT exchange_id) AS distinct_ex,
       COUNT(*) FILTER (WHERE provenance_proof_id IS NOT NULL) AS prov_populated,
       COUNT(*) FILTER (WHERE trade_volume_24h_btc IS NOT NULL) AS btc_vol_populated,
       COUNT(*) FILTER (WHERE trading_pairs IS NOT NULL) AS trading_pairs_populated,
       COUNT(*) FILTER (WHERE name IS NOT NULL) AS name_populated,
       MIN(snapshot_at) AS earliest,
       MAX(snapshot_at) AS latest,
       NOW() - MAX(snapshot_at) AS staleness
FROM exchange_snapshots;
```

Result (2026-05-12 09:15 UTC):

```
total_rows:                   2099
distinct_ex:                    15      ← worker._EX, not module.TOP_EXCHANGES
btc_vol_populated:            2099 / 2099
trading_pairs_populated:      2099 / 2099
name_populated:               2099 / 2099
prov_populated:               1545 / 2099   (73.6%)
earliest:        2026-04-21 09:17 UTC
latest:          2026-05-12 08:53 UTC
staleness:       ~25 min
```

`distinct_ex = 15` is the definitive evidence: **the module path
has never successfully inserted a row.** Worker.py inline is the
only writer.

### 4. Consumers — `FROM exchange_snapshots` grep

| file:line | reads | works on current schema? |
|---|---|---|
| `app/server.py:7876` | `SELECT *` (detail endpoint) | ✅ returns 11 columns |
| `app/server.py:7886` | `SELECT … trade_volume_24h_usd … ORDER BY trade_volume_24h_usd` | ❌ **raises `column does not exist`** |
| `app/data_layer/coherence_guards.py:334` | `trust_score` | ✅ |
| `app/data_layer/state_growth.py:167` | `COUNT(*)` | ✅ |
| `app/data_layer/index_simulator.py:348` | `DISTINCT exchange_id` | ✅ |
| `app/enrichment_worker.py:880` | `MAX(snapshot_at)` (gate check) | ✅ |
| `app/worker.py:1218` | `COUNT(*)` (logging) | ✅ |

**Verified by running the actual query against prod Neon:**
`SELECT … trade_volume_24h_usd … FROM exchange_snapshots …`
returned `NeonDbError: column "trade_volume_24h_usd" does not
exist`. The `/api/data/exchanges` endpoint in list mode (when
called without an `exchange_id` query param) is broken and has
been since the Neon migration's silent column loss.

### 5. Why the module path's failures aren't in cycle_errors

```sql
SELECT COUNT(*) FROM cycle_errors
WHERE occurred_at > NOW() - INTERVAL '24 hours'
  AND (cycle_phase ILIKE '%exchange%' OR error_message ILIKE '%exchange_snapshots%');
```

Result: **0 rows.**

Three reasons the broken module path doesn't surface as cycle errors:

  - `main.py:335` calls `run_exchange_collection()` every fast cycle
    inside a `try/except Exception as e: logger.error(…); results[name]
    = {"error": str(e)}` block at main.py:342-344 — failure is logged
    to stdout but **not recorded via `_record_cycle_error`**.
  - `enrichment_worker.py:872` registers the module call gated on
    `SELECT MAX(snapshot_at) AS latest FROM exchange_snapshots` with
    `min_hours=1`. Worker.py keeps the table fresh hourly, so this
    gate **always skips** in steady state. Same lesson-6 pattern as
    psi_discoveries / peg pre-Wave-5.
  - The module's `_store_exchange_snapshots` at exchange_collector.py:174-176
    catches `Exception` per-row and logs the first 3 — those go to
    stdout only.

So in steady state every fast cycle has both a successful worker.py
inline write of 15 rows AND a silent failure of the module path's
12-column INSERT against the 8-column schema, but only the success
is visible in substrate.

### 6. Exchange list reconciliation

  worker._EX  (15):   binance, coinbase-exchange, okx, bybit_spot,
                      kraken, kucoin, gate, bitget, htx, crypto_com,
                      mexc, bitfinex, bitstamp, gemini, lbank

  module.TOP_EXCHANGES (50): worker._EX + 35 more
                      (upbit, bithumb, whitebit, bitrue, poloniex,
                      hashkey-exchange, bitmart, phemex, deribit,
                      bitflyer, indodax, korbit, exmo, btcturk,
                      tidex, coinone, probit-exchange, bitbank,
                      zaif, coincheck, okcoin, gopax, liquid,
                      btcbox, bkex, latoken, hotbit, coinex, bigone,
                      digifinex, xt, deepcoin, toobit, bingx,
                      bitvenus)

Module is a strict superset; first 15 entries match worker's order
exactly. **But** module lacks `_EX_FIX` so the four CG-legacy ids
(`coinbase-exchange, okx, htx, mexc`) would 404 if the module ever
got past its broken INSERT.

### 7. Halt-condition assessment vs the briefing

| condition | met? |
|---|---|
| Schema has >11 columns (drift since #185) | No — schema has exactly 11, but migration 058 declared 14. Drift is in the OTHER direction (schema is shorter than the migrations table claims). |
| Consumer grep surfaces heavy reader on `raw_data` | Not on `raw_data`. But `server.py:7886` is a heavy reader on `trade_volume_24h_usd` — different column, same family of missing data. |
| Paths writing to DIFFERENT TABLES (v9.11-style drift) | No — both target `exchange_snapshots`. The drift is column-shape, not table-shape. |

### 8. Reframed design questions (operator decides)

The pre-investigation questions assumed the schema was prepared for
the module's richer rows and the design question was about
"acceptance." Substrate says otherwise:

**Q1 — schema replay or schema trim?**
   Migration 058 was supposed to give us 14 columns. We have 11.
   Two paths forward:
   - (a) **Replay missing DDL** for the 4 missing columns (same
     pattern as Wave 9c's migration 108 for the 3 pg-dump-drift
     tables). Then the module's INSERT and `server.py:7886` both
     start working. This restores the originally-intended schema.
   - (b) **Trim the module** to match the current (8-col) schema.
     Cheaper but loses `stablecoin_pairs` + `raw_data` data
     forever (and quietly fixes `server.py:7886` by removing the
     broken column reference).

   Recommendation hint: (a) — three downstream consumers in
   migration 058's design (the original schema declared the four
   columns for a reason, and `server.py:7886` is one consumer).
   But the operator owns this call.

**Q2 — 15 exchanges or 50?**
   Module's `TOP_EXCHANGES` is the worker's `_EX` + 35 more. If we
   adopt the module path:
   - The 4 CG-legacy ids (`coinbase-exchange, okx, htx, mexc`)
     need `_EX_FIX` ported over or the slugs corrected in
     `TOP_EXCHANGES` (so 11 of the original 15 keep working).
   - 35 additional /exchanges/{id} + /exchanges/{id}/volume_chart/30
     calls per cycle. CG pro paid tier (500/min, 500k credits/month)
     handles this comfortably.

**Q3 — provenance_proof_id schema-only column.**
   Already documented: populated by `link_batch_to_proof` after
   the attest batch is written. Not a writer-path question. No
   action needed.

**Q4 — silently-broken `server.py:7886` endpoint.**
   Independent of the refactor decision. Either (a) restoring the
   missing column or (b) editing the query to use
   `trade_volume_24h_btc` fixes it. Best handled in whichever
   PR implements Q1.

### 9. What this means for the v9.12 sweep order

`data_layer:exchange_snapshots` is **not** "module-canonical
refactor blocked on a design question." It's "module path silently
broken on a schema drift; ALSO refactor needs design call."

The schema decision (Q1) has to land before the v9.12 module-
canonical refactor here. Recommendation: split into two PRs:

  1. **Schema replay PR** (migration 109, mirrors 108) — adds the
     4 missing columns idempotently. Verifies `server.py:7886`
     unbroken. No refactor.
  2. **Module-canonical refactor PR** — once schema is restored,
     follows the #193 / v9.13 pilot pattern: scheduler wrapper +
     `_EX_FIX` ported + worker.py inline removed.

PR #1 is safe to land before answering Q2 because it only restores
columns to migration-058 state; nothing's writing those columns
yet so the additions are harmless.

PR #2 stays blocked on Q2 (list reconciliation).

