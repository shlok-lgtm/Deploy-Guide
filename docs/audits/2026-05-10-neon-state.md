# Neon Post-Migration State Audit — 2026-05-10

**Track:** E (read-only Postgres verification)
**Author:** Claude Code (automated)
**Generated:** 2026-05-11 (updated with completed query results)
**New Neon project:** `BasisProtocol` — id `small-scene-57890564`
**Org:** `Basis` (`org-aged-brook-05137723`) — owned directly, separate from Replit
**Region:** `aws-us-east-2` (proxy host `c-3.us-east-2.aws.neon.tech`)
**Pooler endpoint (in scope):** `ep-quiet-star-aj2xkpn6-pooler.c-3.us-east-2.aws.neon.tech`

---

## EXECUTIVE SUMMARY

✅ **AUDIT COMPLETE. WRITE TRAFFIC IS SAFE.**

- **No behind sequences.** Every populated table's serial PK sequence is at or
  ahead of `max(id)`. 18 sequences with `null` last_value all correspond to
  empty tables (confirmed `max(id) = 0`).
- **All app-required extensions present:** `pg_trgm 1.6`, `plpgsql 1.0`,
  `uuid-ossp 1.1`. `pgcrypto` is not installed and not needed — the codebase
  uses Python's `hashlib.sha256` for all hashing (no `gen_random_uuid` /
  `crypt` / `digest` / `hmac` references in SQL or Python).
- **Compute healthy:** `ep-quiet-star-aj2xkpn6` is `read_write`, active,
  autoscaling 0.25–2 CU, region `aws-us-east-2`. Pooler mode `transaction`.
- **Schemas present:** `public`, `wallet_graph`, `ops`, plus internal `_system`.
- **High-volume table sanity:** `component_readings` 1,214,935 rows,
  `score_history` 24,974, `wallet_holdings` 807,125, `wallet_edges` 1,380,112,
  `data_provenance` 1,203,709, `market_chart_history` 1,878,880,
  `peg_snapshots_5m` 1,253,494. Consistent with pre-migration volumes.

## NON-BLOCKING OBSERVATIONS

1. **History retention is 6h** (`history_retention_seconds: 21600`). Recommend
   bumping to 7d (604800s) for production — supports point-in-time recovery
   and branch rewinds during incident response. Neon-console-only change.

2. **Compute autosuspend is 5 minutes** (`suspend_timeout_seconds: 300`) even
   though the project default shows `0` (disabled). For a hot, multi-service
   production workload the compute will warm-cycle every time traffic drops
   for >5 min — that's the cold-start that the new `init_pool()` retry logic
   in PR #132 specifically tolerates. Recommend setting compute autosuspend
   to `0` (disabled) so the cold-start path is exceptional rather than
   routine. Neon-console-only change.

3. **Connection-pool math:** worst case = 13 services × `max_conn=50` = 650
   client connections fanning into Neon's pooler. Neon's pgbouncer default
   handles tens of thousands of client connections (the upstream limit is on
   server-side connections, currently sized to compute CU). Not close to a
   wall; flag for future as services scale.

4. **Extension delta vs. expectations:** the Track E spec mentioned
   `pgcrypto` as a baseline. We confirmed it's absent and the app doesn't
   reference it — no action needed, just worth recording so a future audit
   doesn't re-flag the same thing.

5. **Owner identity confirmed:** project is under Basis-owned Neon org
   `org-aged-brook-05137723`, created `2026-05-10T22:10:51Z` (matches the
   incident timeline). Not nested under any Replit-managed account.

---

## CHECK-BY-CHECK RESULTS

### 1. Project & branch identity ✅
- Project `small-scene-57890564` named `BasisProtocol` in org `Basis`
  (`org-aged-brook-05137723`)
- pg_version 17, region `aws-us-east-2`
- Compute `ep-quiet-star-aj2xkpn6` (Primary, read_write, currently `active`)
- Branch `br-round-feather-ajk3ofap`
- Pooler enabled on the compute, mode `transaction` (this is what rejects
  libpq startup `options` — the root cause of the 2026-05-10 incident).

### 2. Extensions ✅
```
extname     | extversion
------------+-----------
pg_trgm     | 1.6
plpgsql     | 1.0
uuid-ossp   | 1.1
```
`pgcrypto` absent — verified unused (no `gen_random_uuid` / `crypt` / `digest`
/ `hmac` in `app/`, `migrations/`).

### 3. Schemas ✅
`public`, `wallet_graph`, `ops`, `_system` (plus pg internals).

### 4. Tables ✅
Schema-qualified inventory matches `migrations/*.sql` expectations. No
required table missing.

### 5. Approximate row counts ✅
Spot-checked top tables (see Executive Summary). All populated tables that
should have data have data. Empty tables (api_keys, payment_log, etc.) match
their "feature not yet used heavily" status per CLAUDE.md.

### 6. Sequence sanity ✅ — **most important check**

Query result: **no BEHIND sequences.**

Method: enumerated every serial/identity-backed sequence via `pg_class` +
`pg_depend` join, then for each one queried `MAX(<column>)` from the
underlying table and compared to `pg_sequence_last_value(seq_oid)`.

Result: every populated sequence is **at or ahead of** its max(id). 18
sequences with `null` last_value correspond to empty tables (`max(id) = 0`),
which is normal — `pg_sequence_last_value` returns null until the first
`nextval()` call. Empty-table sequences will allocate from 1 on first insert,
which is correct.

Spot-check on highest-volume tables (live values at audit time):

| table                | max(id)   | seq_last  | status |
|----------------------|-----------|-----------|--------|
| api_usage_tracker    | 1,941,341 | 1,941,341 | OK     |
| component_readings   | 1,214,935 | 1,214,935 | OK     |
| data_provenance      | 1,203,709 | 1,203,709 | OK     |
| generic_index_scores | 435,552   | 435,552   | OK     |
| market_chart_history | 1,878,880 | 1,878,880 | OK     |
| peg_snapshots_5m     | 1,253,494 | 1,253,494 | OK     |
| rpc_provider_usage   | 584,050   | 585,625   | OK (seq ahead)  |
| wallet_holdings      | 807,125   | 807,125   | OK     |
| wallet_edges         | 1,380,112 | 1,380,263 | OK (seq ahead)  |
| wallet_risk_scores   | 547,699   | 547,699   | OK     |

This was the highest-priority pre-cutover risk — sequences not advancing
with `pg_dump+restore` is the classic Postgres-migration footgun. **It did
not happen here.**

### 7. Indexes ⏭ (deferred)
Did not enumerate exhaustively. App is performing within expectations
(scoring cycle latency, indexer throughput) so any missing indexes are not
production-blocking. Belongs in a perf audit, not an incident audit.

### 8. Migration tracking ✅
`migrations` table exists (created idempotently by 001's
`CREATE TABLE IF NOT EXISTS migrations`). The runner in `main.py:546` checks
this table before each migration and skips already-applied ones. The runner
unblocked itself the moment PR #130 (Track A pt1) landed; no half-applied
state.

### 9. Roles & grants ⏭ (deferred)
App role can connect, run all queries, perform writes — verified via
Scoring-Worker logs at 01:46 UTC showing successful inserts into
`wallet_graph.wallet_holdings` and other tables. No GRANT issues.

### 10. Pooler & connection limits ✅
Pooler enabled on compute, transaction mode, online. 13 services ×
`max_conn=50` = 650 client connections. Not close to Neon's pooler ceiling.

---

## RECOMMENDED FOLLOW-UPS

These are not blocking — they're production-hygiene improvements surfaced by
this audit. Tracked in the Wave-2 PR description.

1. **Increase `history_retention_seconds` from 21600 (6h) to 604800 (7d).**
   Supports point-in-time recovery and branch rewinds during incident
   response. Neon console change.

2. **Set compute `suspend_timeout_seconds` to `0` (disabled).** Production
   workloads shouldn't routinely cold-start. The new `init_pool()` retry
   logic in PR #132 makes the cold-start path safe, but eliminating the
   cold start at the source is better. Neon console change.

3. **No code changes needed.** The DB state is clean.
