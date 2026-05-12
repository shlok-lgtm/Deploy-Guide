# `psi_discoveries` P0 design questions — 2026-05-12

**Status:** investigation-only. This doc enumerates the divergences
between the three live `psi_discoveries` write paths so the operator can
pick a canonical behavior for each. No refactor PR follows from this doc
without explicit per-question sign-off.

**Pattern source:** mirrors the lesson-10 investigation pattern from
#195 (Blocker 2 / `exchange_snapshots`). Cite substrate, surface
decisions, defer the code change.

**Important framing correction.** There is **no `psi_discoveries`
table.** `psi_discoveries` is a **`state_attestations.domain` value**.
All three writers call `attest_state("psi_discoveries", [payload_dict])`
— the dict is hashed (sha256 of canonical JSON) and stored as one row
in `state_attestations` (one row per cycle per writer). The "columns
written" question therefore reframes as "what fields does each writer
put in the payload dict that ends up driving `batch_hash`".

The §3-PR history in
`docs/audits/2026-05-11-module-canonical-migration-plan.md` lists the
module site as "(none — enrichment task wraps `app/discovery.py`)".
That is **incorrect**: `app/discovery.py` is the dbt discovery layer
(domain `discovery_signals`); the actual enrichment-pipeline writer
for `psi_discoveries` lives at
`app/enrichment_worker.py::_run_psi_expansion`. Corrected inventory
below.

---

## 1. 3-path inventory

| # | file:line | role | when it fires |
|---|---|---|---|
| A | `app/worker.py:2405-2417` | Live writer — heartbeats from `run_slow_cycle` after the in-block PSI expansion gate at lines 2351-2393. Added by #157 as the "third path" once #137/#150 were shown dead. | Every fast cycle's slow-cycle path (`run_slow_cycle`). Fires unconditionally with a `status` field (`ran`/`skipped_fresh`/`error`). |
| B | `main.py:260-268` | Legacy daily-loop writer — inside `run_worker_loop`'s 24h `last_expansion_at` block (`main.py:232-270`). Added by #150 dropping the `if discovered or promoted` gate. | Daily, only when the main.py worker-thread's own `last_expansion_at` timer crosses 24h. In production it almost never fires first — path A keeps `protocol_collateral_exposure` fresh, so path B's `run_worker_loop` rarely wins the gate. |
| C | `app/enrichment_worker.py:288-305` (inside `_run_psi_expansion`, registered at `app/enrichment_worker.py:774-781`) | Enrichment-task writer. Added by #137 dropping the `if discovered or promoted` gate. | Inside `run_enrichment_pipeline()` (called from `run_slow_cycle_parallel`), guarded by `_db_gate("MAX(snapshot_date) FROM protocol_collateral_exposure", min_hours=24)`. In steady state the gate is **closed** because path A wrote that table earlier in the same slow cycle. |

The slow-cycle heartbeat helper at `app/worker.py:2602-2652`
(`_emit_slow_cycle_heartbeats`) **also** writes a row to the
`psi_discoveries` domain when `run_slow_cycle_parallel` completes —
this is a fourth, post-pipeline heartbeat (intentional Wave-5b backfill
covering the case where every task errored). It is not one of the
three "PR-divergent" writers but is part of the live attestation
volume and must be reconciled by any consolidation.

## 2. Per-path detail

### Path A — `app/worker.py:2405-2417`

Payload dict (one record per cycle):

```python
{
  "status": _psi_status,                       # "ran" | "skipped_fresh" | "error"
  "hours_since_last_expansion": round(..., 2) | None,
  "synced":      _psi_synced,                  # int — 0 when skipped/error
  "discovered":  _psi_discovered,
  "enriched":    _psi_enriched,
  "promoted":    _psi_promoted,
}
```

- **Idempotency contract:** none at the attestation layer. `attest_state`
  unconditionally INSERTs. `batch_hash` collisions across cycles are
  expected when `status="skipped_fresh"` and counts are all zero —
  substrate shows this happening (`49e1...d544` and `f419...9526`
  appear twice each over the last 30 days).
- **NULL behavior:** `hours_since_last_expansion` can be `None`
  when `_psi_hours_since` was never set (e.g. exception before the
  freshness query).
- **`entity_id`:** always `NULL` (default arg).
- **`methodology_version`:** defaults to `FORMULA_VERSION` (= `v1.0.0`).
- **Error containment:** wrapped in its own try/except;
  `attest_state` failure logs a warning but does not propagate.

### Path B — `main.py:260-268`

Payload dict:

```python
{
  "synced":     synced,
  "discovered": discovered,
  "enriched":   enriched,
  "promoted":   promoted,
}
```

- **Idempotency contract:** none. No `status` field at all — caller
  reaches this block only when the outer `if hours_since_wallet_expansion >= 24`
  fired (path B at `main.py:232-270`).
- **NULL behavior:** the four counts can be 0 but never None.
- **`entity_id` / `methodology_version`:** same defaults as A.
- **Error containment:** wrapped (logger.debug on failure — quieter
  than path A's `logger.warning`).

### Path C — `app/enrichment_worker.py:288-305`

Payload dict:

```python
{
  "synced":     synced,
  "discovered": discovered,
  "enriched":   enriched,
  "promoted":   promoted,
}
```

- **Same shape as path B.** No status field.
- **Idempotency contract:** none, plus an external gate
  (`enrichment_worker.py:777` `_db_gate(...)`) prevents most invocations
  in steady state.
- **Error containment:** wrapped + `_record_cycle_error(...)` on
  failure (cycle_phase=`psi_expansion`) — strictly louder than A/B.

### Heartbeat (separate domain volume)

`app/worker.py:2644` writes a different payload entirely when
`run_slow_cycle_parallel` finishes:

```python
{"status": "ok"|"ran_no_result"|"pipeline_failed",
 "via": "run_slow_cycle_parallel",
 "succeeded": ..., "total_tasks": ...,
 "error": ...  # only on failure
}
```

This is shape-incompatible with A/B/C. Distinguishable in substrate
only via `batch_hash`.

## 3. Substrate snapshot

### 3.1 Schema of `state_attestations`

Raw output of `SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns WHERE table_name = 'state_attestations'
ORDER BY ordinal_position`:

```
column_name           | data_type                   | is_nullable | column_default
----------------------+-----------------------------+-------------+--------------------
id                    | uuid                        | NO          | gen_random_uuid()
domain                | text                        | NO          | (none)
entity_id             | character varying           | YES         | (none)
batch_hash            | character varying           | NO          | (none)
record_count          | integer                     | NO          | (none)
methodology_version   | text                        | YES         | (none)
cycle_timestamp       | timestamp with time zone    | NO          | now()
```

There is **no** `payload` column, no `created_at`, no `attested_at`.
Earlier sections of the briefing assumed a `psi_discoveries` table
with `created_at` — that table does not exist. Verified by

```
SELECT table_schema, table_name FROM information_schema.tables
 WHERE table_name ILIKE '%psi%' OR table_name ILIKE '%discover%';
```
yielding only `psi_governance_snapshots`, `psi_scores`,
`discovery_signals`, `public_discovery.disc_psi_signals`,
`public_discovery.int_psi_*` — **no `psi_discoveries` table.**

### 3.2 Volume — `psi_discoveries` attestation rows

`SELECT domain, COUNT(*), MIN(cycle_timestamp), MAX(cycle_timestamp)
FROM state_attestations WHERE domain ILIKE '%psi%' GROUP BY domain
ORDER BY 2 DESC`:

```
domain          | n   | first                      | last
----------------+-----+----------------------------+---------------------------
psi_components  | 270 | 2026-04-09T04:53:17.452Z   | 2026-05-12T09:54:07.582Z
psi_discoveries |  12 | 2026-04-09T04:54:34.754Z   | 2026-05-12T07:29:22.475Z
```

`psi_components` (the SII-cycle-end attest) fires every cycle (~270
rows / 33 days ≈ hourly). `psi_discoveries` shows **12 rows** for the
same window — the gap is the 29-day silence (#157 root cause):
between 2026-04-12 and 2026-05-11, **zero** rows.

The full row dump (most recent 12):

```
batch_hash[:16]   | record_count | methodology | cycle_timestamp
------------------+--------------+-------------+-------------------------
4167597418093d6f  |            1 | v1.0.0      | 2026-05-12T07:29:22.475Z
cc9ae5fcf8d43597  |            1 | v1.0.0      | 2026-05-12T05:20:06.459Z
49e1bed1fe40427e  |            1 | v1.0.0      | 2026-05-12T03:09:21.070Z
42e668324e8a77d9  |            1 | v1.0.0      | 2026-05-12T00:13:31.415Z
49e1bed1fe40427e  |            1 | v1.0.0      | 2026-05-11T21:59:56.103Z   # dup of 03:09
b4ed02612febe566  |            1 | v1.0.0      | 2026-05-11T19:53:05.366Z
63d4d62d4b439150  |            1 | v1.0.0      | 2026-05-11T17:39:55.671Z
0587ccee7c376297  |            1 | v1.0.0      | 2026-04-12T00:58:46.108Z   # last pre-gap
f4192ce9585f81f9  |            1 | v1.0.0      | 2026-04-11T14:37:00.958Z
f4192ce9585f81f9  |            1 | v1.0.0      | 2026-04-10T02:58:04.978Z   # dup of 04-11
4aebfa839e68cb1c  |            1 | v1.0.0      | 2026-04-09T19:59:34.509Z
bb206e82e0c14c78  |            1 | v1.0.0      | 2026-04-09T04:54:34.754Z
```

Every row has `entity_id IS NULL` and `record_count = 1`.

### 3.3 Hourly distribution since #157 landed

`SELECT date_trunc('hour', cycle_timestamp) AS hr, COUNT(*)
FROM state_attestations WHERE domain = 'psi_discoveries'
AND cycle_timestamp > NOW() - INTERVAL '7 days'
GROUP BY hr ORDER BY hr DESC`:

```
hr                          | count
----------------------------+-------
2026-05-12T07:00:00.000Z    |     1
2026-05-12T05:00:00.000Z    |     1
2026-05-12T03:00:00.000Z    |     1
2026-05-12T00:00:00.000Z    |     1
2026-05-11T21:00:00.000Z    |     1
2026-05-11T19:00:00.000Z    |     1
2026-05-11T17:00:00.000Z    |     1
```

Exactly **1 row per cycle, every ~2h** (slow cycle cadence) since
#157 merged on 2026-05-11T14:35Z. That cadence is path A — paths B
and C are not measurably contributing (their gates are closed every
time path A runs first in the same cycle).

### 3.4 Distinct payloads

`SELECT batch_hash, COUNT(*) FROM state_attestations
WHERE domain = 'psi_discoveries' GROUP BY batch_hash ORDER BY 2 DESC`
(top entries):

```
batch_hash                                                       | n
-----------------------------------------------------------------+----
49e1bed1fe40427e5f43ec7d8accb0404644385b3ad5c843c963191c7ed3d544 | 2
f4192ce9585f81f9a0cae890812e83d3d8b4b3374de930aac90c78ac8acd9526 | 2
... eight more singletons, all distinct.
```

10 distinct hashes / 12 rows = high churn. Inconsistent with the
"registry is steady, attest the same zero-counts every cycle" intent
behind #137. Likely cause: path A's payload includes
`hours_since_last_expansion` (rounded float) and `status`, both of
which mutate cycle-to-cycle even when the protocol registry is
stable. **This is observable evidence that the path A payload shape
is doing semantic work — it's not just a heartbeat.**

## 4. Divergences as numbered questions

### Q1. Which writer is canonical?

**Options:**
- **A.** Path A (`app/worker.py:2405-2417`) — the writer that *is*
  the live path per #157. Already runs every slow cycle. Carries the
  `status` + `hours_since_last_expansion` fields that paths B and C
  drop.
- **B.** Path C (`app/enrichment_worker.py::_run_psi_expansion`) —
  module-canonical per v9.12 doctrine. Requires moving the gate or
  removing it so the task actually fires.
- **C.** A new module (e.g. `app/data_layer/psi_discovery_monitor.py`)
  with a `run_psi_discovery_monitor_scheduled()` wrapper mirroring
  v9.13 (peg_monitor) pattern: freshness gate + 3-domain skip/error
  attest. worker.py, main.py, and enrichment_worker.py all route
  through the wrapper.

**Implications:**
- A keeps the v9.11 worker-authoritative pattern that #185 wants
  retired but is the path actually generating substrate today —
  zero behavior change risk.
- B aligns with v9.12 (module-canonical) but requires fixing the
  gate-closure problem first (the gate currently sees path A's
  writes and skips every cycle). If C also dropped its gate, paths
  A and C would both fire every cycle and double-write — operator
  must remove path A in the same PR.
- C is the v9.13 coupled-write / scheduled-wrapper pattern that
  landed for peg_monitor (#188 + #193) and exchange_snapshots
  (#197 + #198). Highest design effort, lowest divergence risk
  going forward.

**Recommended default: C** — the v9.13 pattern is the established
ratchet for this exact class of "three writers all gate against the
same upstream table" problem. The blast radius is low (psi_discoveries
does not gate SII), so this is the safest place to validate the
pattern outside the peg / exchange domains.

### Q2. Should the canonical payload include `status` and `hours_since_last_expansion`?

**Options:**
- **A.** Keep both (path A shape). Payload distinguishes
  `ran`/`skipped_fresh`/`error` and carries diagnostic freshness.
- **B.** Drop both (paths B/C shape). Payload is just the 4 counts
  `{synced, discovered, enriched, promoted}`. Heartbeat case
  becomes `{0, 0, 0, 0}` — distinguishable from "run that found
  nothing" only by external observation (cycle ran at all?).
- **C.** Keep `status` only, drop `hours_since_last_expansion`.
  Removes the cycle-to-cycle hash churn (3.4) but preserves the
  ran-vs-skipped distinction.

**Implications:**
- A is the only payload that lets a consumer disambiguate "registry
  is steady (skipped)" from "no work happened (error)" without
  cross-joining `cycle_errors`. But it churns batch_hash cycle-to-cycle
  → false signal that something changed.
- B is the original #137 design intent: same hash whenever the
  registry is steady. Easy to grep for "did anything new get
  discovered?" via `WHERE batch_hash != lag(batch_hash)`. Loses
  error-vs-skip distinction at the attestation layer.
- C is the compromise: `status` is bounded-cardinality (3 values)
  so duplicate hashes occur naturally when status repeats. Doesn't
  churn on freshness round-off.

**Recommended default: C** — preserves the diagnostic distinction
that path A added in #157 (which was the actual debugging value of
that PR) while restoring #137's intent that "steady state ⇒ stable
hash".

### Q3. What does the canonical writer do when the upstream gate is closed?

**Options:**
- **A.** Attest anyway with `status="skipped_fresh"` and zeros
  (path A behavior).
- **B.** Don't attest. Only the cycle in which `collect_collateral_exposure`
  actually runs (every 24h) writes a row. Relies on `cycle_errors`
  or coherence to flag silence.
- **C.** Attest only the gate-decision metadata, no count fields
  (e.g. `{"status": "skipped_fresh", "next_run_in_hours": 18}`).

**Implications:**
- A produces ~12-13 rows/day. Path A history shows the cadence
  works.
- B reverts to the pre-#157 pattern that produced the 29-day silence.
  The coherence sweep (`app/coherence.py`) does eventually detect
  this, but only after the 24h freshness threshold + the daily
  sweep cadence = 1-2 day detection lag.
- C is shape-different from "ran" rows, which complicates downstream
  consumers that assume a stable schema for `psi_discoveries` payloads.

**Recommended default: A** — explicit per-cycle heartbeat is the v9.11/
v9.12 freshness model. Don't regress.

### Q4. What happens to the `_emit_slow_cycle_heartbeats` `psi_discoveries` write at `app/worker.py:2644`?

**Options:**
- **A.** Keep — it covers the "every enrichment task errored"
  failure mode that the canonical writer (if it lives inside the
  pipeline) wouldn't get a chance to fire from.
- **B.** Remove once the canonical writer is established. The
  canonical writer (path A or the new module) is outside the
  enrichment-pipeline try/except in worker.py, so it fires whether
  or not the pipeline succeeded.
- **C.** Keep but rename to a distinct domain (e.g.
  `psi_discoveries_heartbeat`) so downstream consumers can
  distinguish the two payload shapes.

**Implications:**
- A is double-attestation. Tolerable but produces shape-incompatible
  payloads in the same domain (per §2 heartbeat section).
- B requires confirming the canonical writer's call site is
  outside `try: await run_enrichment_pipeline()`. If it lives at
  `worker.py:2405` (path A), it already is — `_run_psi_expansion`
  body is in `run_slow_cycle`, not `run_slow_cycle_parallel`.
  Verify before deleting.
- C avoids the shape ambiguity at the cost of one extra
  state_attestations domain.

**Recommended default: B** — once Q1's canonical writer is
unambiguously outside the pipeline failure path, the post-pipeline
heartbeat is redundant. If Q1 = option B (module-canonical inside
the pipeline), reconsider — the heartbeat then becomes load-bearing.

### Q5. What is the `entity_id` for `psi_discoveries`?

**Options:**
- **A.** Keep `NULL` (all current paths). One row per cycle, no
  per-protocol decomposition.
- **B.** Set to `entity_id = "psi_registry"` or similar fixed
  sentinel, so `get_latest_attestation("psi_discoveries", entity_id=...)`
  works (currently the function ignores `psi_discoveries` for
  entity-scoped lookups — see `app/state_attestation.py:71`).
- **C.** Decompose: one row per newly-promoted protocol when
  promoted > 0, plus a `status=skipped_fresh` summary row otherwise.

**Implications:**
- A is least change.
- B costs one extra TEXT column write per cycle, plus a one-line
  change in callers. Makes the domain consistent with how the rest
  of `state_attestations` is queried.
- C is a substantive payload shape change — promotes psi_discoveries
  from a domain-level heartbeat to a per-entity attestation surface.
  Requires consumer reconciliation.

**Recommended default: A** — `psi_discoveries` is a registry-state
heartbeat; per-entity attestation is `psi_components` (which already
exists and runs every cycle with 270 rows in 33 days).

### Q6. Methodology version pinning

**Options:**
- **A.** Keep the implicit default (`FORMULA_VERSION` = `v1.0.0`,
  the SII formula version). What all 12 substrate rows currently
  carry.
- **B.** Introduce a domain-specific version, e.g.
  `psi_discovery_v0.1.0`, passed explicitly to `attest_state(...,
  methodology_version=...)`. Decouples discovery cadence/payload
  from SII formula bumps.

**Implications:**
- A is a semantic lie: `v1.0.0` is the SII canonical formula
  version, which has nothing to do with PSI discovery. If/when
  the discovery payload shape changes, the version doesn't bump
  and consumers can't tell.
- B is more accurate but requires (1) deciding the seed version
  and (2) confirming no current consumer joins on
  `methodology_version` to filter for `v1.0.0`.

**Recommended default: B** — but only if Q2's payload shape is
finalized. Bumping a version makes sense alongside a structural
change, not as a standalone correctness fix.

## 5. Out of scope

This doc is **not** the refactor PR. It explicitly does **not**:

- Change any code in `app/worker.py`, `main.py`, or `app/enrichment_worker.py`.
- Pick the canonical path. Q1 has a recommended default but
  operator can override.
- Touch `app/state_attestation.py` or the `state_attestations`
  schema (e.g. for Q5's `entity_id` change).
- Address the path-A vs heartbeat shape mismatch (Q4) by adding
  a new domain — that is part of the refactor PR, not this doc.
- Modify the §3-PR-history row in
  `docs/audits/2026-05-11-module-canonical-migration-plan.md` —
  the "(none — enrichment task wraps `app/discovery.py`)" cell is
  factually wrong but its correction is its own PR. See "Important
  framing correction" at top.
- Touch the `psi_discoveries` audit row's priority classification
  (still P0). The refactor PR is gated on operator answers to
  Q1-Q6, not on this doc landing.
- Address P1/P2 domains (`wallets`, `web_research`, `psi_components`,
  `cda_extractions`, `wallet_profiles`, `actors`, `edges`). Each is
  its own lesson-10 reading per §"P0 sweep blockers" note in the
  migration plan.

## 6. Verification queries the refactor PR will need

For the substrate-gate section of the future PR:

```sql
-- Pre-deploy: snapshot baseline cadence
SELECT COUNT(*) AS rows_24h,
       MIN(cycle_timestamp) AS first_24h,
       MAX(cycle_timestamp) AS last_24h
FROM state_attestations
WHERE domain = 'psi_discoveries'
  AND cycle_timestamp > NOW() - INTERVAL '24 hours';

-- Post-deploy (after >= 1 slow cycle elapse): confirm cadence
-- continues, and that distinct batch_hash count matches the new
-- payload shape's expected churn rate.
SELECT batch_hash, COUNT(*)
FROM state_attestations
WHERE domain = 'psi_discoveries'
  AND cycle_timestamp > NOW() - INTERVAL '24 hours'
GROUP BY batch_hash;
```

The PR body should quote both before/after.

## 7. Cross-references

- #137 — "fix(psi): always attest psi_discoveries — gate kept it
  silent 29 days" — path C origin
- #150 — "fix(followups): persist is_stale/error_message + close
  psi_discoveries gate" — path B origin (and #137 declared dead
  in steady state)
- #157 — "fix(psi): third psi_discoveries path — worker.py:2468 PSI
  expansion" — path A origin (and #137 + #150 declared dead in
  steady state)
- #185 (closed) — v9.12 amendment introducing module-canonical
  doctrine
- #188 / #193 — peg_monitor v9.13 coupled-write pattern
- #197 / #198 — exchange_snapshots v9.13 schema-replay + refactor
- #195 — exchange_snapshots design-questions doc (this doc mirrors
  the format)
- `docs/audits/2026-05-11-module-canonical-migration-plan.md`
  §"Blocker 3 — psi_discoveries" — the 3-PR friction summary that
  motivated this doc
