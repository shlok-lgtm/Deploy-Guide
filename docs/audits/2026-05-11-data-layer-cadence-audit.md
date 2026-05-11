# data_layer:* cadence audit — 2026-05-11

**Purpose:** Calibrate verification windows to substrate, not to domain
names. Wave 5a was prematurely declared failed at ~1h post-deploy on
`data_layer:peg_snapshots_5m` because the domain name implied a 5-min
cadence. The actual block invocation cadence is ~2h. This audit
enumerates every `data_layer:*` attest call site, its underlying data
table, the actual write cadence over the last 7 days, the code-level
gate, the name-implied cadence (where the name suggests one), and
flags any dead-canonical-path suspects per lessons 4 / 6 / 8.

## Comparison table

| domain | data table | timestamp col | actual block cadence¹ | data write avg | code gate² | name-implied | rows attested 7d | rows in table 7d | match? |
|---|---|---|---|---|---|---|---|---|---|
| data_layer:peg_snapshots_5m | peg_snapshots_5m | `timestamp` | ~2h | 8.3s | worker.py inline, no gate (every fast cycle) | **5min (misleading — resolution, not cadence)** | 1 | 71,767 | ❌ DEAD-CANONICAL (Wave 5a fix landed; not yet exercised) |
| data_layer:exchange_snapshots | exchange_snapshots | `snapshot_at` | ~2h | 7m | worker.py inline, no gate (every fast cycle); enrichment fallback `min_hours=1` | -- | 3 | 1,365 | ❌ DEAD-CANONICAL (Wave 5a fix landed; not yet exercised) |
| data_layer:dex_pool_ohlcv | dex_pool_ohlcv | `timestamp` | ~4–6h | 22.6s | worker.py inline `if ohlcv_age >= 6h`; enrichment task `min_hours=3` | -- | 0 | 25,972 | ❌ DEAD-CANONICAL (Wave 5b heartbeat fix landed; not yet exercised) |
| data_layer:entity_snapshots_hourly | entity_snapshots_hourly | `snapshot_at` | ~2h | 2m7s | worker.py inline, no gate | hourly (misleading — name = data resolution, but written every ~2m) | 0 | 4,560 | ❌ VARCHAR(30) TRUNCATION (34 chars; fixed in migration 107, not yet exercised) |
| data_layer:liquidity_depth | liquidity_depth | `snapshot_at` | ~2h | 6.4s | worker.py inline, no gate; enrichment task `min_hours=N/A` (always) | -- | 64 | 90,725 | ✅ MATCH |
| data_layer:token_approvals | token_approval_snapshots | `snapshot_at` | ~1.5h | 7.3s | enrichment task `min_hours=20` | -- | 31 | 81,705 | ✅ MATCH |
| data_layer:mint_burn_events | mint_burn_events | `collected_at` | ~2h heartbeat | 0 data rows | enrichment task `min_hours=24` | -- | 70 | 0 | ⚠️ COLLECTOR RUNS, DATA DRY (CoinGecko 429s / Alchemy quota; op-followup #1) |
| data_layer:yield_snapshots | yield_snapshots | `snapshot_at` | ~2h | 32s | worker.py inline, no gate; enrichment task `min_hours=24` | -- | not queried | 18,200 | (presumed ✅ — large row count) |
| data_layer:market_chart_history | market_chart_history | `timestamp` | ~2h | 4.5s | worker.py inline (chained off peg loop); enrichment task `min_hours=20` | -- | not queried | 129,841 | ⚠️ VARCHAR(30) TRUNCATION (31 chars; fixed in migration 107) |
| data_layer:bridge_flows | bridge_flows | `snapshot_at` | (deferred) | 0 data rows | block DEFERRED per constitution v9.3 (DeFiLlama paywall) | -- | not queried | 0 | ⚠️ INTENTIONALLY DISABLED |
| data_layer:contract_surveillance | contract_surveillance | `scanned_at` | ~36m | 36m | enrichment task `min_hours=20` | -- | not queried | 273 | (likely ✅) |
| data_layer:wallet_chain_presence | wallet_chain_presence | `last_verified_at` | (collector last fired 1d ago) | 25s | enrichment task | -- | not queried | 18,009 | ⚠️ VARCHAR(30) TRUNCATION (32 chars; fixed in migration 107) + collector stale 1d |
| data_layer:wallet_holdings | wallet_graph.wallets | `created_at` | ~3.5s within bursts | 3.5s | seed via run_pipeline / enrichment | -- | not queried | 168,598 | (presumed ✅) |
| data_layer:wallet_holder_discovery | wallet_holder_discovery | `discovered_at` | (last 3d 9h ago) | 210ms | enrichment task | -- | not queried | 135,027 | ⚠️ COLLECTOR STALE (3d+) |
| data_layer:governance_proposals | governance_proposals | `collected_at` | ~2h | 10h | enrichment task `min_hours=20` | -- | not queried | 13 | low-volume, presumed ✅ |
| data_layer:oracle_cadence | oracle_price_readings | `recorded_at` | continuous (background task) | 17m | background `asyncio.create_task(run_oracle_cadence_loop())` — no gate | -- | 403 | 522 | ✅ MATCH (background pattern) |
| data_layer:protocol_traces | protocol_trace_observations | `captured_at` | continuous (background task) | 1h10m | background `asyncio.create_task(trace_collector_background_loop())` — 6h cadence comment | -- | 143 | 68 | ⚠️ ATTEST CADENCE > DATA CADENCE (attest 143 rows / data 68 rows in 7d; attest is firing without data writes — heartbeat semantics OK) |

¹ "Actual block cadence" = approximate interval between successive
block invocations, inferred from the data table's last-write
timestamp relative to NOW and the spacing of data rows.

² "Code gate" = the outer conditional that decides whether the block
runs this cycle. Where listed as `worker.py inline, no gate`, the
block runs on every fast_cycle pass (~1–2h based on the scoring
phase length). Where listed as an enrichment task `min_hours=N`, the
block runs when the canonical data table's last write is older than
N hours.

## Bugs vs name mismatches

### Name mismatches (no bug)

- **peg_snapshots_5m** — "5m" describes the data granularity
  (5-minute resolution candles), not the block invocation cadence.
  The block writes ~10k rows per ~2h invocation. Verification
  windows must use ~2h, not 5min.
- **entity_snapshots_hourly** — "hourly" describes intended data
  granularity. Actual write interval per row is ~2 minutes; block
  invocation is ~2h. Verification windows must use ~2h.

### Real bugs (already in flight)

- **Wave 5a (#162) — peg_snapshots_5m, exchange_snapshots:** attest
  was buried in outer try/except that swallowed inner failures.
  Hoist landed 15:13 UTC. Substrate confirmation pending next
  fast_cycle completion (~30-60 min post-deploy).
- **Wave 5b (#163) — dex_pool_ohlcv:** attest was in dead
  `run_slow_cycle`. Heartbeat added in `run_slow_cycle_parallel`.
  Confirmation pending next slow-cycle completion (~3h).
- **Migration 107 — entity_snapshots_hourly (34 chars),
  market_chart_history (31), wallet_chain_presence (32):**
  state_attestations.domain was VARCHAR(30); writes silently
  truncated. Migration applied 2026-05-11 15:40:54 UTC. Confirmation
  pending next collector invocation.

### Pre-existing operational issues (not fix-able in code)

- **mint_burn_events:** Block runs and attests (70 rows in 7d), but
  the data table has 0 rows in 7d. CoinGecko/Alchemy upstream is
  dry (op-followup #1 — Alchemy quota exhausted).
- **bridge_flows:** Block intentionally deferred per constitution
  v9.3 (DeFiLlama paywalled the endpoints). Expected behavior.
- **wallet_holder_discovery:** Collector last fired 3d 9h ago. Likely
  the same upstream issue (Etherscan rate limiting) that Wave 4 #160
  fixed for the wallet scanner. Separate diagnostic queued.

## Recommendation for future verification windows

Before declaring a domain's fix failed, run:

```sql
-- 1. how often does the underlying data table get written?
SELECT MAX(<timestamp>) AS last_write,
       NOW() - MAX(<timestamp>) AS time_since,
       (MAX(<timestamp>) - MIN(<timestamp>))::interval
         / NULLIF(COUNT(*) - 1, 0) AS avg_write_interval,
       COUNT(*) AS rows_7d
FROM <table>
WHERE <timestamp> > NOW() - INTERVAL '7 days';

-- 2. then wait at least one block invocation interval before
--    declaring the attest fix failed.
```

The block invocation cadence ≠ data row write cadence ≠ name-implied
cadence. All three are different numbers for `peg_snapshots_5m`
(2h vs 8s vs 5m respectively). Substrate before name, every time.

## Cross-references

- v9.11 amendment (worker-authoritative live path)
- Lesson 4 (two-path attest sites)
- Lesson 6 (canonical module ≠ live path)
- Lesson 7 (verification must cite substrate)
- Lesson 8 (specific stop dates may indicate dead-canonical not disablement)
- Lesson 9 (this audit's contribution — TBD pending append to punchlist)
