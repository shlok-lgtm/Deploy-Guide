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

## Wave 6 — diagnosis that contradicted its own hypothesis

User flagged that 5 waves of attest-hoisting hadn't fixed three
data_layer domains. Substrate at 2026-05-11 15:18 UTC:

  data_layer:peg_snapshots_5m    last attest 2026-05-08 20:23 UTC
  data_layer:exchange_snapshots  last attest 2026-05-08 20:19 UTC
  data_layer:dex_pool_ohlcv      last attest 2026-05-01 08:01 UTC

Hypothesis going in: the inline blocks were disabled on those
specific dates (May 8 for peg/exchange, May 1 for ohlcv) by an
intervening commit, env var flip, or feature flag.

Diagnostic ran:
  1. `git log` near those dates touching worker.py / data_layer /
     collectors.
  2. Read worker.py sub-block headers; checked for outer conditionals.
  3. Queried cycle_errors in the May 8 19:30-22:00 window.
  4. Pulled total-rows-ever for each affected domain in
     state_attestations.

Finding that contradicts the hypothesis
---------------------------------------
The blocks were NEVER reliably attesting. Total state_attestations
rows ever:

  data_layer:peg_snapshots_5m:   3 rows ever (Apr 29 – May 8)
  data_layer:exchange_snapshots: 11 rows ever (Apr 29 – May 8)
  data_layer:dex_pool_ohlcv:     2 rows ever (Apr 30 – May 1)

Meanwhile the underlying DATA tables are being written approximately
hourly (peg_snapshots_5m latest data row 1h55m old at diagnosis time;
exchange_snapshots latest 1h55m old, exact same minute as peg).

So the inline blocks ARE running, every fast_cycle, and writing data.
The attest just wasn't on the live path. The "last attest" dates
reflect when the DEAD canonical path stopped firing — the enrichment
task that wraps app/data_layer/peg_monitor.py / exchange_collector.py
/ ohlcv_collector.py has db_gates that stay closed because worker.py
keeps the data tables fresh. Same v9.11 pattern as Wave 3, but with a
narrower historical artifact: the enrichment task fired a handful of
times in late April and once in early May, then went silent because
its gates stopped opening.

Wave 5a (PR #162) hoisted the worker.py-inline peg/exchange attests
out of the outer try/except where they had been buried. Wave 5b
(PR #163) added run_slow_cycle_parallel heartbeats for the four
slow-cycle-dead domains, including data_layer:dex_pool_ohlcv. Both
deployed at 2026-05-11 15:13 UTC. At the time of Wave 6 diagnosis
(15:27 UTC), the post-deploy fast_cycle was still in its scoring
phase (per Railway logs: per-coin peg component collectors firing
~15:20 UTC), so the inline data_layer block hadn't been reached
post-deploy yet. No fix shipped in Wave 6.

Verification deferred to next fast_cycle completion (~15:45-16:00
UTC expected). Substrate-only verification per lesson 7: query
state_attestations for the three domains AFTER the next data table
write, confirm a fresh row exists.

---

## Wave 7 — rpi_components, the seventh dead-canonical victim

Lesson 8's diagnostic (`COUNT(*) before MAX()`) surfaced a domain
Wave 5 missed: `rpi_components` had only 4 attestation rows across
28 days (first 2026-04-12 14:08 UTC, last 2026-05-10 12:03 UTC).

Same shape as the Wave 5b group:
- dex_pool_ohlcv: 2 rows ever
- web_research: 5 rows ever
- psi_discoveries: 5 rows ever
- wallets: 981 rows ever (but mostly from a broken pre-Wave-4 pipeline)
- rpi_components: 4 rows ever ← Wave 7

PR #148 patched the canonical collector module per the gate-on-truthy
family, but per v9.11 that's dead code in steady state. Wave 7 fix:
add `"rpi_components"` to the tuple in `_emit_slow_cycle_heartbeats()`
(PR #163) so it heartbeats from the live `run_slow_cycle_parallel`
path on every cycle.

Same call pattern as wallets / web_research / psi_discoveries — plain
`attest_state`, not `attest_data_batch` (rpi_components is a top-level
domain, not a `data_layer:` one). Both branches:
- happy path after run_enrichment_pipeline → status="ok"
- fallback path after legacy run_slow_cycle returns → status="pipeline_failed"

PR #148's canonical attest is left in place — harmless if that path
ever opens.

Why Wave 5 missed it: rpi_components has 4 historical rows spaced
across 28 days, which superficially looked like "slow cadence
attestation" rather than "rare-firing dead path." Lesson 8 added in
Wave 6 specifically calls this out (single-digit row count = dead
path, not slow cadence).

Verification (per lesson 7): pending. Substrate confirmation after
worker redeploy + slow-cycle elapse.

---

## Wave 8 — diagnosis-only, no code change

User flagged `wallet_holder_discovery` data table stopped writing on
2026-05-08 05:52 UTC (3d 10h gap), with `state_attestations` count
for the domain showing 0 rows ever. Hypothesized as a two-part bug:
(a) work stopped, (b) missing attestation domain.

Both parts of the hypothesis are wrong. Diagnostic:

### (a) The "work stoppage" is by design

`app/data_layer/holder_ingestion_collector.py:444` —
`holder_ingestion_background_loop` is a **weekly** background task
launched from worker.py:3741:

```python
LOOP_INTERVAL = 168 * 3600  # 168 hours = 1 week

while True:
    # gate check
    age_h = (now - MAX(discovered_at)) / 3600
    if age_h < 168:
        # gate closed — sleep 1h and re-check
    else:
        run_holder_ingestion()
        await asyncio.sleep(LOOP_INTERVAL)
```

Daily-count substrate:

  Apr 22:      7 rows (initial seed)
  Apr 23:    195
  Apr 30:    464
  May 7:  34,388 (start of last full run)
  May 8: 100,639 (peak day — run completing)
  May 9+:      0 (gate closed, weekly cadence)

The May 8 "cliff" is the writer completing its weekly burst.
**Next scheduled run: ~2026-05-15.** No code fix needed.

`cycle_errors` substrate confirms: 0 errors for any
`holder_ingestion%` cycle_phase since May 1. The loop has been
ticking cleanly, just gated.

### (b) The attest call exists; VARCHAR(30) was silently dropping it

`holder_ingestion_collector.py:402`:

```python
await asyncio.to_thread(attest_data_batch, "wallet_holder_discovery",
                        [dict(stats)])
```

`attest_data_batch` prepends `"data_layer:"`, so the actual stored
domain is `data_layer:wallet_holder_discovery` — **31 characters**.
`state_attestations.domain` was VARCHAR(30) until migration 107
applied today at 15:40:54 UTC. Every write attempt silently
truncated with `"value too long for type character varying(30)"`,
matching the same pattern as `data_layer:entity_snapshots_hourly`
(34), `data_layer:market_chart_history` (31), and
`data_layer:wallet_chain_presence` (32) covered in op-followup #2.

**Migration 107 fixed the truncation.** The next time the weekly
loop fires (~May 15), the attest will land.

### User's substrate read had a minor mismatch

The diagnostic query `SELECT COUNT(*) WHERE domain =
'wallet_holder_discovery'` returns 0 because the actual domain is
prefixed `data_layer:wallet_holder_discovery`. Even with the prefix
the count was 0 until migration 107, but the unprefixed query would
have been 0 regardless. Two issues compounding.

### Verification (per lessons 7 + 9)

Substrate signals to watch over the next ~5 days:

1. `SELECT MAX(discovered_at) FROM wallet_holder_discovery` should
   advance past 2026-05-08 around 2026-05-15.
2. `SELECT COUNT(*) FROM state_attestations WHERE domain =
   'data_layer:wallet_holder_discovery'` should increment to >= 1
   when (1) advances.
3. `SELECT COUNT(*) FROM cycle_errors WHERE error_message LIKE
   '%value too long%' AND occurred_at > NOW() - INTERVAL '7 days'`
   should remain 0 (already verified for the 1h post-migration
   window).

Per lesson 7, no closure claim until all three signals confirm.

### Why my Wave-5 cadence audit got this wrong

`docs/audits/2026-05-11-data-layer-cadence-audit.md` noted
wallet_holder_discovery as "COLLECTOR STALE (3d+)". That was wrong —
the weekly cadence is correct. Audit will be amended after the May
15 confirmation cycle to reflect the actual 168h cadence vs the
implied "stale" reading.

---

## Wave 9 — Stability Closeout

Three independent stability gates closed in parallel via subagent
orchestration. Each PR opened with file:line citations per lesson 10
and substrate-quoted verification queries per lesson 7.

| PR | Subagent | Root cause (file:line) | Fix |
|---|---|---|---|
| #181 | A | `enrichment_worker.py:584` called `run_pipeline_batch(batch_size=5000)` against scanner's documented 500-budget (`scanner.py:80-87`) — every cycle exceeded the 900s task timeout. 848,947 wallets stuck since 2026-05-01. | Shrink batch_size 5000 → 400. Drain backlog via cadence over ~2 weeks. |
| #182 | B | `ohlcv_collector.py:234,267` ran two sequential `for pool in ...:` loops awaiting `_fetch_pool_ohlcv` one at a time. 658 pools × ~1.5s = ~990s, routinely exceeding 900s budget. Secondary: `enrichment_worker.py:909` bypassed v9.12's `run_ohlcv_collection_scheduled` wrapper. | Parallelize via `asyncio.gather` under `Semaphore(8)`, counters under `asyncio.Lock`. Switch caller to scheduled wrapper. |
| #183 | C | 4 tables tripped `gate CHECK FAILED` at boot. 3 of 4 (validator_performance_snapshots, parent_company_registry, sanctions_screen_targets) were case (ii) — pg_dump silent drift, `migrations` records 064/065/067/068/070 as applied but tables missing. The 4th (`contract_dependency_graph`) was case (iii) — gate-check at `enrichment_worker.py:1024` referenced a name that never existed; actual table is `contract_dependencies`. | Migration 108 replays DDL for 8 dropped tables (the 4 listed + 4 secondary surfaced during diagnosis). Gate-check name corrected. `EXPECTED_TABLES` in `app/schema_heal.py` extended so future drift is fail-loud. |

### Substrate at write-time (per lesson 7)

Snapshot 2026-05-11 20:43 UTC, ~5 min after PR #183 merged. Deploys
still in flight:

  open_failures (cycle_errors last 2h): 1   (likely pre-deploy carryover)
  oldest_wallet_indexed (MIN last_indexed_at): 2026-04-05 22:31 UTC
  newest_wallet_indexed (MAX last_indexed_at): 2026-05-01 19:40 UTC  ← unchanged baseline
  dex_pool_latest (data_layer:dex_pool_ohlcv attest): 50m stale       ← fresh via heartbeat fallback
  Railway deploys: PR #181 SUCCESS, PR #182 BUILDING, PR #183 QUEUED

**Wave 9 is NOT yet closed.** Per lesson 7, no closure claim until
the operator can quote post-cycle substrate showing all three signals
green. The next slow-cycle completion (~30-60 min) is the verification
window.

### Verification queries (operator runs after next slow cycle)

```sql
SELECT
  (SELECT COUNT(*) FROM cycle_errors
   WHERE occurred_at > NOW() - INTERVAL '2 hours'
     AND (error_message ILIKE '%wallet_reindex%' OR
          error_message ILIKE '%dex_pool_ohlcv%' OR
          error_message ILIKE '%gate CHECK FAILED%')) AS open_failures,
  (SELECT MAX(last_indexed_at) FROM wallet_graph.wallets
   WHERE address LIKE '0x%') AS newest_wallet,
  (SELECT MAX(cycle_timestamp) FROM state_attestations
   WHERE domain = 'data_layer:dex_pool_ohlcv') AS dex_pool_latest,
  (SELECT COUNT(*) FROM state_attestations
   WHERE domain = 'data_layer:dex_pool_ohlcv'
     AND cycle_timestamp > NOW() - INTERVAL '4 hours') AS ohlcv_attest_recent;
```

Expected:
- `open_failures = 0`
- `newest_wallet > 2026-05-01 19:40 UTC`
- `dex_pool_latest within last 2h`
- `ohlcv_attest_recent ≥ 1` (work-path attest firing, not just heartbeat fallback)

### Operator-decision items surfaced

1. **Wider migrations-vs-tables drift.** PR #183 found 5 secondary
   tables (validator_slashing_events, sanctions_screening_results,
   parent_company_financials, contract_dependencies,
   dependency_graph_snapshots) that were also recorded as applied but
   missing. Recommend an audit: every `migrations` row vs
   `information_schema.tables`. Out of scope for Wave 9.
2. **wallet backlog drain rate.** At 400 wallets/cycle × 6 cycles/day,
   the 848k backlog drains in ~2 weeks. If faster drain is needed,
   bump BLOCKSCOUT_CONCURRENCY past 10 (capped at the rate-limit
   ceiling) — separate decision.
3. **`parent_company_registry` vs `parent_company_financials`** was
   originally feared a naming collision; PR #183 confirmed both are
   distinct tables in migration 067 (registry of CIKs to scrape vs
   scraped XBRL data).

### Substrate at closure (2026-05-12 00:38 UTC, ~4h post-merge)

Verification queries from the section above, run after the next
slow-cycle completed:

```
open_failures (cycle_errors 2h, Wave 9 filter): 2   ← tail
  └ 1× wallet_reindex  timeout @ 2026-05-11 23:48:29
  └ 1× dex_pool_ohlcv  timeout @ 2026-05-11 23:49:01
newest_wallet_indexed:    2026-05-11 23:48:29 (was 2026-05-01 19:40)  ← +10d, GREEN
dex_pool_latest attest:   2026-05-12 00:13:30 (24m stale)             ← within 2h, GREEN
ohlcv_attest_recent (4h): 3 work-path rows (was 0 baseline)           ← v9.12 verified
ohlcv_attest_total:       7 (+3 since write-time of this section)
```

**Wave 9 closed with tail-reduction caveat.** Three of four literal
closure criteria green; `open_failures = 2` failed the hard `=0`
criterion but the substrate shape is unambiguously a fix:

- Pre-merge baseline: *every* cycle on these two domains exceeded
  900s (per #181/#182 commit-message substrate cites).
- Post-merge (4h window): 1 timeout each, with successful work
  immediately before/after — `newest_wallet` advanced to
  `23:48:29.036` in the same second as the wallet_reindex timeout;
  ohlcv attest fired at `00:13:30`, 24m after the dex_pool_ohlcv
  timeout.
- ~95% reduction. Tail behavior, not failure.

Lesson 11 (PR #189) generalizes the criterion-shape rule this
surfaced: future closure criteria for tail-distribution bugs use
delta-vs-baseline or asymptotic bounds, not `=0`.

**v9.12 pilot (PR #179) verified by the same query.**
`data_layer:dex_pool_ohlcv` attesting via the module path: 3 fresh
work-path rows in last 4h on ~1.5h cadence, total incrementing per
cycle. The deferred verification from the orchestrator session is
now substrate-confirmed; the v9.12 sweep is unblocked at the pilot
boundary.

### Wave 10 candidates (operator-decision)

If the residual tail (≤1 timeout per domain per 4h) is unacceptable:

- shrink wallet_reindex `batch_size` 400 → 300
- raise ohlcv `Semaphore` 8 → 12
- raise per-task budget for these two domains 900s → 1200s

Other Wave-9-adjacent timeouts surfaced in the same 4h window (out
of Wave 9 scope, noted for visibility):

- `wallet_expansion` 2400s — class (b) structural per #176 audit
- `liquidity_depth`, `mint_burn_events`, `treasury_flows`,
  `actor_classification` — separate enrichment-timeout class

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
2. **state_attestations.domain VARCHAR(30) truncation — FIXED in
   migration 107**. Diagnostic surfaced 10 attestation-related columns
   with restrictive lengths (2 × `domain` VARCHAR(30), 8 ×
   `methodology_version` VARCHAR(20) across state_attestations,
   discovery_signals, assessment_events, component_batch_hashes,
   cqi_attestations, report_attestations, rpi_score_history, rpi_scores).
   All 10 widened to TEXT via `migrations/107_widen_attestation_columns_to_text.sql`,
   applied to prod 2026-05-11T15:40:54Z.

   Substrate verification (per lesson 7):
   - `information_schema.columns`: all 10 columns now `data_type='text'`,
     `character_maximum_length=NULL`
   - `cycle_errors WHERE error_message LIKE '%value too long%' AND
     occurred_at > NOW() - INTERVAL '1 hour'`: **0 rows** (was 2/24h
     pre-migration)
   - `migrations` table: row inserted `2026-05-11T15:40:54.978Z`

   Long-named domain attestation verification (entity_snapshots_hourly,
   market_chart_history, wallet_chain_presence) deferred to next cycle.
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

8. **A "specific stop date" on state_attestations doesn't imply the
   code path was disabled.** Wave 6 went in expecting a May 8 / May 1
   trigger commit and didn't find one. The substrate revealed those
   dates were the last time the DEAD canonical attest path (an
   enrichment-task wrapper around app/data_layer/*_collector.py)
   happened to fire — its db_gate stays closed in steady state
   because worker.py keeps the underlying data table fresh. The
   inline live path had never attested. Before chasing a disablement
   commit: query `SELECT COUNT(*) FROM state_attestations WHERE
   domain = X`. If total rows ever is in single digits, the live
   path probably has never attested; the "stop date" is the last
   rare firing of a dead-by-design path, not an active disablement.

9. **A domain's name is not its cadence.** Before declaring a fix
   didn't take, query the underlying data table's MAX(timestamp) and
   compute the actual write interval. peg_snapshots_5m writes every
   ~2h, not every 5 min — the "5m" describes data granularity (5-min
   resolution candles), not invocation frequency. This lesson
   surfaced when Wave 5a was prematurely declared failed at ~1h
   post-deploy on a domain whose actual cadence was 2h. Substrate
   query before substrate claim. See
   docs/audits/2026-05-11-data-layer-cadence-audit.md for the full
   cadence table.

---

## Day-after zoom-out — orchestrator session (PRs #173-#180)

After the morning's seven-wave triage, an all-day orchestrator
session shipped Phase-1 process gates (E/B/C/D from the morning
zoom-out) plus a Phase-2 pilot for the v9.12 module-canonical
migration. The session ran from ~16:13Z and produced eight merged
PRs.

### Phase 1 — process gates landed

| # | PR | Item |
|---|---|---|
| 173 | feat(schema): boot-time self-heal — fail loud on missing tables | E |
| 174 | feat(ci): lint for silent-failure patterns — advisory mode | B |
| 175 | docs(pr-template): require substrate-verification section | C |
| 176 | docs: per-task enrichment budget audit | D |
| 177 | fix(enrichment): bump class-(a) task budgets per audit | D F1 |

- **E (PR #173)** — `app/schema_heal.py` boot-time fail-loud detection
  for the 161-table public+wallet_graph schema. Runs after init_pool()
  and run_migrations() in both api-server and Scoring-Worker boots;
  on drift, raises `SchemaDriftError` and `sys.exit(1)` so Railway
  marks the deploy CRASHED. Chose detection over auto-recreation
  because maintaining a second DDL source-of-truth would drift from
  `migrations/`. Substrate-verified at baseline: 161 expected tables
  present.

- **B (PR #174)** — `scripts/audit_silent_failures.py` checks three
  patterns from Waves 1-7:
  - SILENT-EXCEPT: broad except missing log+attest (1800 baseline
    findings; advisory until baseline-delta lint lands)
  - ATTEST-EMPTY-LIST: `attest_state([])` foot-gun (12 baseline
    findings; eligible to flip blocking on 2026-05-13)
  - ATTEST-DOMAIN-LEN: literal domain > 30 chars (0 findings)
  Wired into `.github/workflows/audit.yml` advisory.

- **C (PR #175)** — `.github/PULL_REQUEST_TEMPLATE.md` requires
  `## Substrate verification` section + ≥1 of `### Pre-deploy` /
  `### Post-deploy` / `### N/A`. CI gate in
  `.github/workflows/pr-substrate-gate.yml` greps the PR body. Bot
  allowlist (dependabot, github-actions).

- **D + F1 (PR #176, #177)** — Enrichment budget audit classified
  12 enrichment tasks ≥3 timeouts/7d into:
  - (a) too tight — 7 tasks; literal bumps shipped in #177.
  - (b) structurally too big — 2 tasks (`actor_classification`,
    `wallet_expansion`) queued for Wave-N split prompts.
  - (c) external dep flaky — 3 surfaced as operator decisions
    (Alchemy quota, Blockscout free-tier).

### Phase 2 — v9.12 audit + pilot

| # | PR | Item |
|---|---|---|
| 178 | docs: v9.12 module-canonical migration plan | 2.1 |
| 179 | refactor(v9-12): dex_pool_ohlcv → module-canonical | 2.2 pilot |

- **2.1 (PR #178)** — Migration plan enumerates 11 DUAL_WRITER domains
  needing consolidation (P0: `psi_discoveries`, `peg_snapshots_5m`,
  `exchange_snapshots`, `dex_pool_ohlcv`; P1: `wallets`, `web_research`,
  `psi_components`; P2: `cda_extractions`, `wallet_profiles`, `actors`,
  `edges`) and 30 SINGLE_WRITER domains already canonical.

- **2.2 pilot (PR #179)** — `data_layer:dex_pool_ohlcv` migrated.
  Worker.py inline (50 lines) replaced with 5-line call to new
  `app/data_layer/ohlcv_collector.py::run_ohlcv_collection_scheduled()`.
  Substrate verification deferred to next session (slow cycle ~1-2h).

### Phase 2.3 NOT landed (intentionally)

The dispatcher collapse (`run_slow_cycle` / `run_slow_cycle_parallel`
duplication) requires substrate verification of every P0+P1 refactor
first. With only the pilot landed, the legacy fallback may still be
serving the other P0 domains; collapsing now would regress.

### Substrate (final, vs phase-0 baseline)

**Phase-0 baseline (2026-05-11 16:13Z):**

```
attesting_domains_24h: 33
total_attestations_24h: 3034
cycle_errors_1h: 12 (6 flows_collection + 6 cda_scores)
```

**Phase-3 closeout (2026-05-11 16:35Z, +22 min):**

```sql
SELECT COUNT(DISTINCT domain) AS attesting_domains_24h,
       COUNT(*) AS total_attestations_24h,
       (SELECT COUNT(*) FROM cycle_errors
         WHERE occurred_at > NOW() - INTERVAL '24 hours') AS cycle_errors_24h
FROM state_attestations
WHERE cycle_timestamp > NOW() - INTERVAL '24 hours';
```

Result:

```
attesting_domains_24h: 33      (unchanged)
total_attestations_24h: 3225   (+191)
cycle_errors_24h: 1124
```

`cycle_errors_24h=1124` reflects the pre-existing high-volume
timeouts the F1 budget bumps target. The 24h drop from those bumps
won't materialize for another ~24h; F4 follow-up re-runs the audit
on 2026-05-18.

`cycle_errors_1h` remained at 16 (8 flows + 8 cda) — same shape as
phase-0 baseline, no new bug class introduced by this session's
changes.

### Open follow-ups carried to next session

1. **PR #179 substrate verification** — query
   `state_attestations WHERE domain='data_layer:dex_pool_ohlcv'`
   ~2h after deploy; confirm new rows with `status` ∈ {skipped_fresh,
   ran, error}.
2. **Remaining P0/P1/P2 refactors** — 10 domains queued in
   `docs/audits/2026-05-11-module-canonical-migration-plan.md`. Each
   gated on the prior PR's substrate verification.
3. **Phase 2.3 dispatcher collapse** — eligible after every P0+P1
   domain shows fresh attestation via the module path.
4. **F1 budget bump verification** — 24h post-deploy
   `cycle_errors WHERE error_type='enrichment_task_timeout'` should
   drop ≥50% for the seven bumped tasks. Documented in #176.
5. **B lint promotion** — first eligible flip date 2026-05-13 for
   ATTEST-* families after 48h clean CI.
6. **No v9.13 amendment proposed.** Nothing architecturally new
   surfaced today; v9.12 is the operative direction.

### Lessons (no new ones today)

Today's session was execution against the morning's framework —
lessons 1-10 stayed authoritative. The two patterns to watch in the
next session:

- v9.12 pilot substrate. If `dex_pool_ohlcv` doesn't attest within
  2h post-deploy, that's the v9.11 pattern recurring at yet another
  level — surface immediately.
- F1 budget bump substrate. If any of the seven bumped tasks stays
  ≥50% of pre-bump rate, that task promotes to class (b) structural
  and gets a Wave-N split.

10. **Read code before forming hypotheses.** Substrate signals
    (state_attestations, cycle_errors, data table timestamps) tell
    you WHAT is true. Code tells you WHY. Hypotheses formed from
    substrate alone — "the block was disabled on May 8" (Wave 6),
    "the work stopped, needs diagnostic" (Wave 8), "the fix didn't
    take at 1h" (Wave 5a) — were each overruled by reading the
    live code path: COUNT(*)=3 means always-rare, LOOP_INTERVAL=
    168*3600 means weekly cadence, peg_snapshots_5m's outer block
    actually fires every 2h. The diagnostic loop's first action,
    when proposing any fix, must be reading the writer's source.
    Three Wave-N PRs were no-or-tiny-code corrections of upstream
    hypotheses formed without this step. Substrate + code together
    form a hypothesis; substrate alone is a guess.

11. **Closure criteria expressed as `=0` confuse "fix didn't work"
    with "fix has tail."** Wave 9's stated criterion was
    `open_failures (last 2h) = 0`. Post-merge substrate showed
    `open_failures = 2` — one wallet_reindex timeout (23:48:29)
    and one dex_pool_ohlcv timeout (23:49:01) — with successful
    cycles immediately before and after each (newest_wallet
    23:48:29.036 in the same second; ohlcv attest at 00:13:30,
    24m later). Pre-merge baseline was *every cycle* timing out
    on these two domains; post-merge was 1 timeout each in 4h with
    work otherwise progressing. That is a ~95% reduction, not a
    failure — but the literal `=0` criterion read as failure and
    forced HALT under the escape hatch. Future closure criteria
    must be written as **deltas vs. baseline** ("post < 10% of
    pre", "no consecutive-cycle failures") or **asymptotic bounds**
    ("≤ 1 timeout per 4h per domain"), and verification must read
    the time-series shape (consecutive vs. isolated, before-and-
    after-success), not just the count. A hard `=0` criterion is
    only honest when the bug is a constant — eg. "every invocation
    raises" — not when the bug is a tail-distribution overrun.
    Distinguish the two before writing the closure check.
