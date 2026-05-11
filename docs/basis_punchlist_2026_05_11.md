# Punchlist Entry — 2026-05-11

**Scope:** Full-day triage following the 2026-05-10 Neon migration. Three distinct waves shipped: deploy-config fix, staleness-tail closure, and schema-drift cleanup. Cycle errors dropped from ~838/24h to 0/h for the affected phases.

---

## Background — the May-10 migration's lingering exhaust

The 2026-05-10 cutover from Replit-managed Neon to owned Neon (`small-scene-57890564`) shipped clean for the database connection (per `docs/punchlist_2026-05-10_neon_incident.md`), but left three classes of secondary fallout that surfaced over the next 24h:

1. **Per-service deploy config** — `railway.json` healthcheck applied repo-wide; non-web services failed → Wave 1.
2. **Attest-only-on-happy-path** — many `state_attestations` call sites gated on truthy results; in steady state they went silent → Wave 1.5 + Wave 2.
3. **Schema drift** — Replit-Neon's pg_dump preserved the `migrations` tracking table but skipped several table DDLs → Wave 3.

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

## Wave 3 — schema drift (1 PR + 6 direct DDLs)

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

1. **Alchemy plan exhausted** — 88 "Monthly capacity limit exceeded" 429s/24h on `parameter_history._eth_call_sync`. Operator decision:
   - Upgrade Alchemy plan, OR
   - Investigate why the dwellir fallback reverts on the same calls ("execution reverted" — different issue; the call shape itself may be wrong for dwellir's RPC semantics).
2. **Migration-tracking integrity** — the `migrations` table claims migrations 055, 066, 071, 103 are applied, but they weren't (the tables didn't exist). The records are now technically correct (because the DDL ran), but a future fresh-DB setup that's restored from a similar partial dump would have the same blind spot. Consider adding a self-healing migration that does `CREATE TABLE IF NOT EXISTS` for the canonical schema on every deploy, regardless of the migrations table state.
3. **Re-verify Wave-1/Wave-2 freshness tomorrow** — `state_attestations` for the 13 affected domains should all be < their gate cadence by ~24h after Wave-2-c lands (#151 was the last code change). If any domain is still pre-2026-05-11 at that point, that's a deeper bug.
4. **Codespaces verification** — still owed from Track F of the May-10 punchlist.
5. **Replit decommission** — flip the canon status on 2026-05-17.

---

## PRs landed today (12 total)

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

Plus 6 manual DDL re-applies on prod Neon for the 6 dropped tables.

---

## Lessons

1. **pg_dump can preserve metadata while skipping content.** The `migrations` table looked perfect on the new Neon; only direct `information_schema.tables` queries revealed the missing DDL. Future migrations: assume the dump-restore is lossy, validate against `information_schema` not against the migrations log.

2. **Attestation gates on change are a freshness anti-pattern.** `if discovered or promoted`, `if results`, `if total_snapshots > 0` all silently kill freshness signal in steady state. Default to "always attest; record what we found"; let coherence's threshold logic decide whether the state is interesting.

3. **`attest_state([])` is a foot-gun.** The helper silently early-returns on empty list. Any list-comprehension caller can hit this without realizing. Either change `attest_state` to always insert a row (with `record_count=0`) or audit every call site for empty-list handling. The latter is mechanical and was done piecemeal across #137/#139/#141/#143/#144/#145/#146/#148/#151.

4. **Two-path attest sites need both paths fixed.** psi_discoveries (#137 → #150) and wallets (#142, both sites) showed the pattern: a "canonical" implementation in a service module + a "legacy" duplicate in main.py. Fixing only one is a no-op if the other is what's actually running. Always trace which path the worker is invoking before declaring victory.

5. **Per-service Railway config beats repo-wide railway.json** for any project where service shapes differ (HTTP server vs forever-loop vs one-shot). Lesson now codified in basis-protocol/canon constitution v9.9.
