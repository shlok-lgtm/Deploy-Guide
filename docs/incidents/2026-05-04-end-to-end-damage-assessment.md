# End-to-End Damage Assessment: April 26 + May 4 Production Freezes

**Prepared:** 2026-05-04
**Status:** QUERIES READY — awaiting production execution
**Queries:** `scripts/audit/damage_assessment_queries.sql`

## Incident Windows

| Window | Start (UTC) | End (UTC) | Duration | Root Cause |
|--------|------------|-----------|----------|------------|
| **A** | 2026-04-24 ~16:00 | 2026-04-27 ~13:00 | ~69h | `api_usage_tracker._flush_buffer()` called inline from `track_api_call()` in async finally blocks. Sequential sync DB INSERTs from event loop thread blocked all asyncio task scheduling. |
| **B** | 2026-05-03 ~00:00 | 2026-05-03 ~08:55 | ~9h | PR-A1 retry (90ae9c8) converted 370 call sites to `fetch_one_async` etc. but didn't add imports. Every DB-touching endpoint returned `NameError`. Fixed by hotfix PR #64. |
| **C** | 2026-05-03 ~09:10 | 2026-05-04 ~08:55 | ~23h | `holder_discovery.run_holder_discovery()` async function calling sync psycopg2 directly. Task-3754 entered CANCELLING state, couldn't propagate through C-level blocking. Event loop hostage. Fixed by PR #65. |

**Total downtime:** ~101 hours across 11 days (Apr 24 → May 4).

---

## Tier 1 — On-Chain Integrity (HIGHEST PRIORITY)

**Question:** Did the keeper publish stale scores on-chain during any freeze window?

### What to look for

On-chain writes are irreversible. If the keeper continued publishing while scoring was frozen, it published stale data. The severity depends on staleness:

| Staleness | Severity | Impact |
|-----------|----------|--------|
| < 1h | ACCEPTABLE | Normal operational latency |
| 1–6h | HIGH | Data noticeably stale but within same trading day |
| > 6h | CRITICAL | Published scores may not reflect current market conditions |

### Queries to run

Run queries **1a through 1d** from `scripts/audit/damage_assessment_queries.sql`.

### Results

```
[PASTE 1a OUTPUT HERE — keeper cycles during freeze windows]
```

```
[PASTE 1b OUTPUT HERE — state attestations during freeze windows]
```

```
[PASTE 1c OUTPUT HERE — staleness at each keeper cycle]
```

```
[PASTE 1d OUTPUT HERE — keeper cadence gaps]
```

### Interpretation guide

- If **1a returns zero rows**: keeper didn't fire during freezes — no on-chain damage. Best case.
- If **1a returns rows with sii_updates > 0**: keeper published. Check 1c for staleness.
- If **1c shows staleness_hours > 6**: CRITICAL. Those on-chain values were stale. Document every tx_hash for potential correction announcement.
- If **1d shows hours with 0 cycles**: keeper was correctly gated by the freeze. Expected.

---

## Tier 2 — Score Accuracy

**Question:** How large were the score gaps? Did any coin's score jump significantly when scoring resumed?

### Queries to run

Run queries **2a through 2d** from `scripts/audit/damage_assessment_queries.sql`.

### Results

```
[PASTE 2a OUTPUT HERE — score gaps per stablecoin]
```

```
[PASTE 2b OUTPUT HERE — score deltas across Window A]
```

```
[PASTE 2c OUTPUT HERE — score deltas across Windows B+C]
```

```
[PASTE 2d OUTPUT HERE — backdated score rows]
```

### Interpretation guide

- **gap_hours** in 2a tells you how long each coin went unscored. Should match the freeze duration (~69h for A, ~32h for B+C).
- **delta_pct > 5%** in 2b/2c: the score moved meaningfully while we were blind. Worth investigating whether the move was a real market event or an artifact of stale component data.
- **delta_pct > 30%**: RED FLAG. Either a genuine crisis happened during the freeze (check market events for that date) or a scoring pipeline bug.
- **Backdated rows** in 2d: if `lag_minutes > 60`, scores were created well after their `computed_at` timestamp. These may use post-freeze data labeled with pre-freeze timestamps — potentially misleading for historical analysis.

---

## Tier 3 — Indexer Coverage

**Question:** Did the wallet indexer, edge builder, and component collectors fall behind?

### Queries to run

Run queries **3a through 3c** from `scripts/audit/damage_assessment_queries.sql`.

### Results

```
[PASTE 3a OUTPUT HERE — wallet indexer activity]
```

```
[PASTE 3b OUTPUT HERE — edge builder activity]
```

```
[PASTE 3c OUTPUT HERE — component readings gap]
```

### Interpretation guide

- **3a**: Hours with 0 wallets indexed = indexer was frozen. These wallets' risk scores are stale by the gap duration.
- **3b**: Same for edge builder. Edges represent capital flow; stale edges mean the contagion model is working with old data.
- **3c**: Per-category component gap. Categories with gap_hours >> freeze duration may indicate collectors that didn't recover automatically after restart.

---

## Tier 4 — External Integration Impact

**Question:** How many users/integrators were affected?

### Queries to run

Run queries **4a through 4c** from `scripts/audit/damage_assessment_queries.sql`.

### Results

```
[PASTE 4a OUTPUT HERE — hourly 5xx error count]
```

```
[PASTE 4b OUTPUT HERE — unique affected IPs]
```

```
[PASTE 4c OUTPUT HERE — most-affected endpoints]
```

### Interpretation guide

- **Window A** (Apr 24-27): If the worker froze but the API server stayed up, users may have gotten 200s with stale data (worse than 5xx — they don't know it's stale). Check if the response `computed_at` timestamps advanced during the freeze.
- **Window B** (May 3 NameError): Every DB-touching endpoint returned 500. `unique_ips_affected` tells you how many external consumers were hit.
- **Window C** (May 3-4): Worker froze but API may have stayed up with stale cached data. Same stale-200 risk as Window A.
- **basisstate.xyz** specifically: it consumes `/api/scores`. If that endpoint returned 500 during Window B, the Classify page showed an error. If it returned stale 200s during A/C, the leaderboard showed old scores without warning.

---

## Tier 5 — Provenance Integrity

**Question:** Are provenance chains broken for data written during/around the freezes?

### Queries to run

Run queries **5a through 5c** from `scripts/audit/damage_assessment_queries.sql`.

### Results

```
[PASTE 5a OUTPUT HERE — null provenance rows]
```

```
[PASTE 5b OUTPUT HERE — orphan proofs]
```

```
[PASTE 5c OUTPUT HERE — proof/fact timestamp disagreement]
```

### Interpretation guide

- **5a**: Rows with NULL `provenance_proof_id` during freeze windows are expected — the provenance pipeline was also frozen. These rows are legitimate data but lack the audit trail. Decision needed: backfill provenance or annotate as "freeze-window, unattested."
- **5b**: Orphan proofs (proofs with no matching attestation) suggest the proof was computed but the attestation write failed mid-transaction. Low severity — the proof exists, the data exists, just the linkage is missing.
- **5c**: Timestamp disagreement > 30s means the proof and the fact were written in different cycles. Could indicate a retry/recovery that reused old proof hashes. Worth spot-checking a few rows.

---

## Remediation Decision Matrix

After populating the results above, classify overall severity:

| Tier | If clean | If damaged | Remediation |
|------|----------|------------|-------------|
| 1 (On-chain) | No action | Public disclosure + correction tx | Publish corrected scores on-chain with explanation |
| 2 (Scores) | No action | Backfill missing scores from raw components | Run temporal reconstruction for gap periods |
| 3 (Indexer) | No action | Reindex gap blocks | Trigger `run_pipeline_batch` with explicit block range |
| 4 (External) | No action | Incident page for affected consumers | Post to status page with timeline |
| 5 (Provenance) | No action | Backfill provenance links | Run `link_batch_to_proof` for unlinked rows |

---

## Next Steps

1. Run all queries in `scripts/audit/damage_assessment_queries.sql` against production Neon DB
2. Paste results into the placeholder sections above
3. Classify per-tier severity
4. If Tier 1 shows CRITICAL staleness: draft on-chain correction plan before any other remediation
5. File separate issues for each tier that needs remediation
