# Punchlist Entry — 2026-05-11

**Scope:** Full-day triage following the 2026-05-10 Neon migration. Three distinct waves shipped: deploy-config fix, staleness-tail closure, and schema-drift cleanup. Cycle errors dropped from ~838/24h to 0/h for the affected phases.

---

## Background — the May-10 migration's lingering exhaust

The 2026-05-10 cutover from Replit-managed Neon to owned Neon (`small-scene-57890564`) shipped clean for the database connection (per `docs/punchlist_2026-05-10_neon_incident.md`), but left three classes of secondary fallout that surfaced over the next 24h:

1. **Per-service deploy config** — `railway.json` healthcheck applied repo-wide; non-web services failed → Wave 1.
2. **Attest-only-on-happy-path** — many `state_attestations` call sites gated on truthy results; in steady state they went silent → Wave 1.5 + Wave 2.
3. **Schema drift** — Replit-Neon's pg_dump preserved the `migrations` tracking table but skipped several table DDLs → Wave 2.5 (parallel investigation).
4. **Wave-1 fixes patched dead code** — verification ~2h after merge revealed 7 domains where the PR landed but `state_attestations` had NOT advanced. The canonical modules were enrichment-pipeline utilities; the live path lives inline in `app/worker.py` → Wave 3.
5. **Structural pipeline hang** — `run_pipeline_batch` was hanging past its 900s budget; 848k wallets unindexed since May 1; the freshness fix from Wave 3 surfaced this latent work-side bug → Wave 4.

Each wave landed as a separate PR series so reviewer context stayed local.

---

## Wave 1 — staleness tail (9 PRs, all merged)

Closed 9 domains that had been silent for 2.5–29 days. Family was uniformly "attest fires only when X > 0" or "task isn't registered as a worker."

| PR | Domain | Days stale | Family |
|---|---|---|---|
| #137 | psi_discoveries | 29 | gate-on-truthy (also got cleanup in #150 — see below) |
| #138 | exchange_trust_ratio | (not stale; raw=0 misread) | field-absent ≠ field-is-zero |
| #139 | rqs_composition | 19 | orphaned worker task |
| #140 | mempool_capture_status | 14 | only-on-transition (added heartbeat) |
| #141 | dex_pool_ohlcv | 10 | gate-on-truthy |
| #142 | wallets | 9 | gate-on-truthy (both inner + caller sites) |
| #143 | web_research | 8 | gate-on-truthy |
| #144 | mempool_observations | 3 | silent per-tx failures → summary backstop |
| #145 | cqi_compositions | 3 | orphaned worker task |
| #146 | peg_snapshots_5m + exchange_snapshots | 2.5 each | gate-on-truthy (one PR, two sites) |

Plus the deploy-config thrash that surfaced #135/#136 immediately before this:
- **#135** — bump `healthcheckTimeout` 10s → 300s (api-server takes 34s to boot).
- **#136** — move healthcheck from repo `railway.json` to per-service config; only api-server has `/healthz`. The other 12 services were marked FAILED and rolled back to pre-Wave-1 code until this landed.

---

## Wave 2 — staleness tail follow-on (4 domains)

Verification of Wave-1 freshness revealed 4 more silent domains. Triaged in priority order:

| PR | Domain | Days stale | Family |
|---|---|---|---|
| #147 | oracle_stress_events | 1d 11h | event-driven attest → added cycle-summary heartbeat |
| #148 | rpi_components | 22h | gate-on-truthy |
| #150 | psi_discoveries main.py duplicate | 29 (#137 was no-op on live path) | gate on the legacy code path |
| #151 | governance_events | 18h | `attest_state([])` silently drops empty list |

`dohi_components` was listed at 18h stale but is **not a bug** — it's on its 24h cadence by design (gate_check on `governance_events` table freshness; DOHI score doesn't change without governance activity). Inner attestation already handles both branches. No code change; documenting here.

---

## Wave 2.5 — schema drift (1 PR + 6 direct DDLs, parallel investigation)

**838+ cycle failures/24h** all traced to the May-10 pg_dump/pg_restore preserving `migrations` records but skipping table DDL for tables created outside Replit's UI. Confirmed by querying prod Neon directly:

| migration | status table | tables existed in prod |
|---|---|---|
| `055_phase3_disclosure_tables` | applied 2026-04-12 | ❌ tti_disclosure_extractions |
| `066_enforcement_history` | applied 2026-04-14 | ❌ enforcement_records |
| `071_protocol_parameter_history` | applied 2026-04-14 | ❌ protocol_parameters, _changes, _snapshots |
| `103_regulatory_registry_checks` | applied 2026-05-01 | ❌ regulatory_registry_checks |

**Actions taken:**

- Re-applied DDL for all 6 tables via `run_sql` (idempotent `CREATE TABLE IF NOT EXISTS`). `migrations` table left as-is so the runner doesn't try to re-execute migrations with `CREATE INDEX CONCURRENTLY` inside a transaction.
- **PR #149** shipped the 4 code-level fixes that surfaced alongside the tables:
  - `app/coherence.py:227` — `protocol_collateral_exposure.stablecoin_id` doesn't exist; rewritten to JOIN `stablecoins` on `LOWER(symbol)`.
  - `app/ops/routes.py:1883` — `stablecoin_symbol` doesn't exist either; aliased `token_symbol AS stablecoin_symbol`.
  - `app/rpi/forum_scraper.py:271` — `mentioned_vendors` is `jsonb` but code passed `list[str]` (adapter sent text[]); wrapped with `psycopg2.extras.Json`.
  - `app/collectors/parameter_history.py:75` — Etherscan v2 fallback was missing `chainid`; API returned valid JSON with `result="Missing/Invalid Chain Id, see https://api.etherscan.io/v2/chainlist ..."` which downstream `_decode_uint256` then tried to parse as hex. Added chainid=1 + hex-format guard.

**Plus #150**, which closed two follow-up paper-cuts that surfaced during Wave 3:

- `app/worker.py:341` — `component_readings` INSERT was silently dropping `is_stale` and `error_message` columns even though PR #138's exchange_trust_ratio fix relied on them. Schema had the columns (migration 001:37-38). Fix in the INSERT/ON CONFLICT list.
- `main.py:244` — `psi_discoveries` attest had `if discovered or promoted` gate on the **live** code path. #137 dropped the same gate inside the enrichment task, but that task never fires in prod because the daily worker-loop calls `collect_collateral_exposure()` first and keeps the enrichment task's db_gate closed. Dropped the live-path gate too.

---

## Wave 3 — deeper-bug followup (5 PRs, all merged)

Verification ~2h after Wave 1+2 merge revealed 7 domains where the PR
had landed but state_attestations had NOT advanced. The operational
follow-up #3 warning in the morning section was correct.

Diagnostic: every Wave-1 fix patched the canonical module in
app/data_layer/ or app/collectors/. In steady state, the live path is
inline in app/worker.py. The canonical modules sit behind db_gates that
worker.py keeps closed by maintaining the underlying tables. Patching
canonical without patching worker.py = no-op.

Exemplar: psi_discoveries took THREE PRs to fix (#137 enrichment, #150
main.py legacy, #157 worker.py:2468 inline). Only the last was the
live path.

| PR | Domain(s) | Diagnostic |
|---|---|---|
| #153 | peg_snapshots_5m, exchange_snapshots, dex_pool_ohlcv | worker.py inline INSERTs at 977/1222/2094 never call attest_data_batch. #141/#146 modules dead code. |
| #154 | mempool_capture_status | watcher self-disabled by 20× Alchemy 429s; _subscribe_and_consume never runs. Heartbeat moved to emit_24h_summary. |
| #155 | wallets | run_pipeline_batch structurally hanging (848k unindexed since May 1). Worker-side attest fires regardless. |
| #156 | web_research | 24h gate reads MAX(computed_at) across all web_research_* index_ids; bridge at 13h fresh suppresses protocol/exchange (7-8d stale). Worker-side attest decouples freshness from gate. |
| #157 | psi_discoveries | Third live path at worker.py:2468 closes db_gates on #137 + #150 paths. Worker-side attest in actual live block. |

**Architectural lesson:** worker.py inline implementations are the
canonical live path; modules in app/data_layer/ and app/collectors/
are enrichment utilities, dead code in steady state. Codified in
constitution amendment v9.11
(docs/basis_protocol_v9_11_constitution_amendment.md).

**Verification (T+2h after Wave-3 merge):** mempool_capture_status at
2m stale (was 14d). Remaining 6 domains advancing within respective
cadences.

---

## Wave 4 — pipeline hang root-cause (1 PR, merged)

Wave 3's worker-side attest for `wallets` (PR #155) surfaced the
underlying work-side bug: `run_pipeline_batch` was hanging past its
900s budget. 848k wallets had not been indexed since 2026-05-01.

Root cause: `BlockscoutFetcher.fetch_all_balances` declared a
`Semaphore` at init (BLOCKSCOUT_CONCURRENCY=10) but used a sequential
`for addr in wallet_addresses` loop that awaited each `_fetch_single`
one at a time. The semaphore was meaningless — only one coroutine
was ever in flight. 500 wallets × (1.5s/call + 0.22s rate-limit
delay) ≈ 860s per batch, right at the 900s budget with no margin for
429 retries.

| PR | Fix |
|---|---|
| #160 | Replace serial loop with `asyncio.gather`; the existing Semaphore now actually caps in-flight calls. Per-wallet failures absorbed in a `_scan_one` wrapper; `abort_event` short-circuits when 50+ consecutive failures cascade. |

Expected impact: 500-wallet batch ~860s → ~100-200s. Backlog drain at
Blockscout free-tier 5 req/sec ≈ 47h. `wallet_graph.wallets.MAX
(last_indexed_at)` should advance past 2026-05-01 within hours of
deploy.

---

## Wave 5 — Wave 3's outer conditionals (deferred verification revealed)

Wave 3's verification claim "remaining 6 domains advancing within
respective cadences" was wrong. ~40 minutes after the Wave-4 deploy
(PR #160, dbf0722, the most recent Scoring-Worker rebuild),
`state_attestations` showed 6 of the 7 Wave-3 domains had NOT
advanced past their pre-fix timestamps. Only #154
(`mempool_capture_status` → `emit_24h_summary`, unconditional) had
landed.

Why Wave 3 looked like it worked but didn't:

- **#153 (peg + exchange + ohlcv)** — placed the attest_data_batch
  call INSIDE the outer `try/except _e2: logger.error("=== X
  FAILED")` of each sub-block. Any inner failure (most likely the
  `fetch_one_async COUNT(*)` read at the end, or a transient
  httpx error) was caught and the attest was skipped. Sibling
  sub-blocks (`data_layer:liquidity_depth`, `data_layer:mint_burn_events`)
  attested correctly because they didn't have the same buried failure
  path.
- **#155 / #156 / #157 (wallets, web_research, psi_discoveries) +
  the ohlcv attest from #153** — added attests inside
  `run_slow_cycle`, which is **dead code**. `run_scoring_cycle` calls
  `run_slow_cycle_parallel`, which dispatches via
  `run_enrichment_pipeline` and only falls through to
  `run_slow_cycle` if the pipeline raises. Same v9.11 pattern as
  Wave 3 itself, one level higher.

| PR | Domain(s) | Fix |
|---|---|---|
| #162 | data_layer:peg_snapshots_5m, data_layer:exchange_snapshots | Hoist attest OUT of outer `try/except` via local error var; status payload carries `block_failed` when the outer try catches. |
| #163 | data_layer:dex_pool_ohlcv, wallets, web_research, psi_discoveries | New `_emit_slow_cycle_heartbeats()` helper called from `run_slow_cycle_parallel`. Heartbeats fire on both happy path and pipeline-failure fallback. |

**Architectural pattern again:** the v9.11 lesson recurses. Where
Wave 3 found that `worker.py` inline ≠ canonical module is the live
path, Wave 5 finds that `run_slow_cycle_parallel` ≠ `run_slow_cycle`
is the same shape at the dispatcher level. The Wave-3 attests are
still in `run_slow_cycle` but harmless — they'll fire if someone
ever calls it directly.

---

## Verification

**cycle_errors — last hour:**

```sql
SELECT cycle_phase, error_type, COUNT(*) AS n
FROM cycle_errors
WHERE cycle_phase IN ('parameter_history', 'tti_collector', 'enforcement_history',
                       'regulatory_scraper', 'rpi_forum_scraper')
  AND occurred_at > NOW() - INTERVAL '1 hour'
GROUP BY cycle_phase, error_type
ORDER BY n DESC;
```
→ **0 rows** (was 838+/24h pre-fix).

**state_attestations — only mempool_observations has advanced** (~7min stale) at punchlist-write time. The other 13 affected domains will advance over the next 1–24h as their respective slow-cycle / 24h-gated tasks fire. Re-verify the morning after.

---

## Operational follow-ups (NOT addressed in code)

1. **Alchemy plan exhausted** — DEFERRED. 88 "Monthly capacity exceeded"
   429s/24h + dwellir reverting on same calls. Parked until stability
   work completes. Mempool capture is heartbeat-only in the interim;
   parameter_history collector continues failing ~88/day. Accepted
   degraded state.
2. **state_attestations.domain VARCHAR(30) truncation** — 3 collectors
   generate names exceeding 30 chars (entity_snapshots_hourly=34,
   market_chart_history=31, wallet_chain_presence=32). 2 silent
   `data_layer_attest_data_batch_failure` "value too long" errors in 24h.
   Schema migration to TEXT (or VARCHAR(128)) queued.
3. **run_pipeline_batch structural hang** — FIXED in PR #160 (Wave 4).
   See above for diagnostic. Watch the Wave-4 verification queries over
   the next 24h: enrichment_wallet_reindex timeouts should stop, MAX
   (last_indexed_at) should advance, and fresh_24h count should trend up.
4. **web_research per-index targeting** — MAX-aggregated gate semantics
   wrong; Wave 3 #156 decoupled freshness signal but the gate logic
   still suppresses collection for stale `protocol`/`exchange` indices
   when `bridge` is fresh. Future PR — round-robin across index_ids
   rather than MAX-aggregating.
5. **Migration-tracking integrity** — the `migrations` table claims
   migrations 055, 066, 071, 103 are applied, but they weren't (the
   tables didn't exist). Records are now technically correct (because
   the DDL ran), but a future fresh-DB setup restored from a similar
   partial dump would have the same blind spot. Self-healing `CREATE
   TABLE IF NOT EXISTS` on every deploy still TODO.
6. **Re-verify Wave-3 freshness in ~1h** — all 7 domains should be
   inside their cadence interval. mempool_capture_status already
   verified at 2m stale.
7. **Codespaces verification** — still owed from Track F of the May-10
   punchlist.
8. **Replit decommission** — flip the canon status on 2026-05-17.

---

## PRs landed today (24 code PRs + 6 manual DDL re-applies)

| # | Title |
|---|---|
| 135 | hotfix: healthcheckTimeout 10s → 300s |
| 136 | hotfix: per-service healthcheck (api-server only) |
| 137 | fix(psi): always attest psi_discoveries — gate kept it silent 29 days |
| 138 | fix(coingecko): exchange_trust_ratio — distinguish "field absent" from zero |
| 139 | fix(enrichment): register rqs_composition as a slow-cycle task |
| 140 | fix(mempool): emit hourly heartbeat attestation in capture loop |
| 141 | fix(ohlcv): always attest dex_pool_ohlcv, even with zero records |
| 142 | fix(wallets): always attest batch reindex, even with processed=0 |
| 143 | fix(web-research): always attest, even when no scores produced |
| 144 | fix(mempool): summary-based backstop attestation for mempool_observations |
| 145 | fix(enrichment): register cqi_composition as a slow-cycle task |
| 146 | fix(data_layer): always attest peg_snapshots_5m + exchange_snapshots |
| 147 | fix(oracle): cycle-summary heartbeat for oracle_stress_events |
| 148 | fix(rpi): always attest rpi_components, even when scoring returns empty |
| 149 | fix(schema-drift): column + parser fixes for the May 10 Neon migration fallout |
| 150 | fix(followups): persist is_stale/error_message + close psi_discoveries gate |
| 151 | fix(governance): attest with status when protocols_processed is empty |
| 153 | fix(data_layer): worker.py inline attests for peg_snapshots_5m, exchange_snapshots, dex_pool_ohlcv |
| 154 | fix(mempool): move heartbeat to emit_24h_summary (WS-independent) |
| 155 | fix(wallets): worker-side attest decoupled from hanging pipeline |
| 156 | fix(web_research): worker-side attest decoupled from MAX-aggregated gate |
| 157 | fix(psi_discoveries): patch the actual live path at worker.py:2468 |
| 158 | docs: renumber pgbouncer amendment v9.9 → v9.10 |
| 159 | docs: v9.11 amendment — worker-authoritative live path |
| 160 | fix(wallet-scanner): use asyncio.gather so BlockscoutFetcher actually concurrent |
| 162 | fix(wave5a): hoist peg + exchange attests outside outer try/except |
| 163 | fix(wave5b): slow-cycle heartbeats for ohlcv/wallets/web_research/psi_discoveries |

Plus 6 manual DDL re-applies on prod Neon for the 6 dropped tables.

---

## Lessons

1. **pg_dump can preserve metadata while skipping content.** The `migrations` table looked perfect on the new Neon; only direct `information_schema.tables` queries revealed the missing DDL. Future migrations: assume the dump-restore is lossy, validate against `information_schema` not against the migrations log.

2. **Attestation gates on change are a freshness anti-pattern.** `if discovered or promoted`, `if results`, `if total_snapshots > 0` all silently kill freshness signal in steady state. Default to "always attest; record what we found"; let coherence's threshold logic decide whether the state is interesting.

3. **`attest_state([])` is a foot-gun.** The helper silently early-returns on empty list. Any list-comprehension caller can hit this without realizing. Either change `attest_state` to always insert a row (with `record_count=0`) or audit every call site for empty-list handling. The latter is mechanical and was done piecemeal across #137/#139/#141/#143/#144/#145/#146/#148/#151.

4. **Two-path attest sites need both paths fixed.** psi_discoveries (#137 → #150) and wallets (#142, both sites) showed the pattern: a "canonical" implementation in a service module + a "legacy" duplicate in main.py. Fixing only one is a no-op if the other is what's actually running. Always trace which path the worker is invoking before declaring victory.

5. **Per-service Railway config beats repo-wide railway.json** for any project where service shapes differ (HTTP server vs forever-loop vs one-shot). Lesson now codified in basis-hub canon docs as constitution amendment v9.10.

6. **The "canonical module" is not always the live path.** Wave 1
   patched modules that turned out to be dead code in steady state.
   Wave 3 re-patched the inline implementations in worker.py. Pattern
   codified in v9.11. When adding an attest call, start at
   app/worker.py and trace outward; do not start at the collector and
   trace inward.

7. **Verification claims must cite substrate, not expectation.**
   Wave 3's punchlist claimed "remaining 6 domains advancing within
   respective cadences" based on cadence math. The substrate had
   not advanced. Wave 5 had to add the missing diagnostic.
   Future rule: any "verification" line in a punchlist or PR
   description must quote the actual query result at write-time.
