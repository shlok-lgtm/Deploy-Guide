# Integrity Check — Work-Availability Heuristics

Version: `integrity_heuristics_v1`
Status: Active
Owner: Integrity / Coherence Sweep

## Background

`app/coherence.py` runs a daily freshness sweep over every entry in
`ALL_DOMAINS`. A domain is flagged "stale" when its most recent row in
`state_attestations` is older than `2 × DOMAIN_FREQUENCIES[domain]`. This
created a class of false alarms: domains that did not attest because **no
upstream work was available** (gate-blocked tasks, event-driven pipelines,
no new protocols discovered). Those situations are by design — they should
not generate alerts.

This doc specifies the work-availability heuristic per canonical domain.
The integrity check now distinguishes three states:

| State    | Meaning                                                     | Alert? |
|----------|-------------------------------------------------------------|--------|
| `ok`     | Domain attestation is within its expected cadence.          | No     |
| `broken` | Attestation is stale **and** upstream work should have run. | Yes    |
| `quiet`  | Attestation is stale **but** no upstream work was due.      | No     |

Default behaviour is preserved: any domain without a heuristic falls back to
the legacy "stale = broken" logic.

## Per-domain heuristics

All queries are parameterized via psycopg2 placeholders (`%s`). No string
concatenation. The "gate window" referenced below matches the
`make_db_gate(min_hours=...)` value in `app/enrichment_worker.py`.

### `psi_discoveries`

**Expected cadence:** every 24h.
**Why "no work" is normal:** `psi_expansion` only attests when
`discover_protocols()` or `promote_eligible_protocols()` returned > 0. Most
cycles produce neither — the protocol backlog is sparse and the gate
(`protocol_collateral_exposure` snapshot, 24h) closes when the task ran
recently. See `app/enrichment_worker.py:_run_psi_expansion`.

**Heuristic:** quiet if either:
1. `psi_expansion` ran inside the gate window (its gate_check is the freshness
   of `protocol_collateral_exposure`, ≥24h), and
2. The protocol backlog has zero unenriched candidates ready for promotion.

```sql
SELECT
  (SELECT MAX(snapshot_date)::timestamptz
     FROM protocol_collateral_exposure)                AS last_exposure_snapshot,
  (SELECT COUNT(*) FROM psi_protocol_backlog
     WHERE promotion_eligible = TRUE
       AND COALESCE(promoted, FALSE) = FALSE)          AS pending_promotions
```

If `last_exposure_snapshot` is within the last 24h **and** `pending_promotions = 0`,
classify as `quiet` (no upstream work was available). Otherwise treat as `broken`.

**Edge cases:**
- If `psi_protocol_backlog` is missing (table not yet migrated), skip the
  pending check and rely on the snapshot timestamp alone.
- If `protocol_collateral_exposure` has never been collected, the gate is
  always open, so a stale `psi_discoveries` is genuinely `broken`.

### `provenance`

**Expected cadence:** every 24h (sweep), but proofs are continuous.
**Why "no work" is normal:** the prover writes to `provenance_proofs` only
when a fresh source emits content. Some sources emit hourly (price feeds),
others every few hours (transparency reports).

**Heuristic:** quiet if a provenance proof was written in the last 4h, even
if `state_attestations` for `provenance` is older.

```sql
SELECT MAX(proved_at) AS last_proof FROM provenance_proofs
```

If `last_proof > NOW() - INTERVAL '4 hours'`, classify as `quiet`.

**Edge cases:**
- An empty `provenance_proofs` table is `broken` (prover never ran). The
  existing `_provenance_proof_freshness` rule already covers this.

### `actors`

**Expected cadence:** every 24h.
**Why "no work" is normal:** `classify_all_active` only re-classifies
wallets whose holdings changed since their last `classified_at`. After the
initial pass, most cycles return zero re-classifications.

**Heuristic:** quiet if the slow worker's last actor-classification cycle
ran in the last hour (continuous classification), even if no rows were
written.

```sql
SELECT MAX(created_at) AS last_cycle
FROM collector_cycle_stats
WHERE collector_name = 'actor_classification'
```

If `last_cycle > NOW() - INTERVAL '1 hour'`, classify as `quiet`.

**Edge cases:**
- If the `collector_cycle_stats` row is missing because the cycle has not
  yet recorded one, the heuristic falls through to `broken`.

### `rpi_components`

**Expected cadence:** every 48h.
**Why "no work" is normal:** the RPI scorer is gated by
`SELECT MAX(computed_at) FROM rpi_scores` with `min_hours=24`. When the
gate is closed, `rpi_scoring` does not run and therefore does not write to
`rpi_components`. This is by design.

**Heuristic:** quiet if `rpi_scores.computed_at` is fresh (< 24h, the gate
window).

```sql
SELECT MAX(computed_at) AS last_rpi_run FROM rpi_scores
```

If `last_rpi_run > NOW() - INTERVAL '24 hours'`, classify as `quiet`.

**Edge cases:**
- The table being empty means RPI has never run — a genuine `broken`.

### `sii_components`

**Expected cadence:** every 2h.
**Why "no work" is normal:** the fast cycle attests `sii_components` per
stablecoin. If a single stablecoin's collectors all gated out (no peg/liq
deltas), no attestation is written for that coin. But in practice the fast
cycle should be writing **some** rows every cycle.

**Heuristic:** quiet if the SII scoring cycle finished within the last
hour (continuous scoring).

```sql
SELECT MAX(computed_at) AS last_score FROM scores
```

If `last_score > NOW() - INTERVAL '1 hour'`, classify as `quiet`. Otherwise
treat as `broken`.

**Edge cases:**
- An empty `scores` table is unambiguous `broken`.

### `divergence_signals`

**Expected cadence:** every 4h.
**Why "no work" is normal:** the divergence detector attests only when
signals are produced. Quiet markets produce zero signals for stretches of
time. See `app/enrichment_worker.py:_run_divergence` — `attest_state` is
called only `if signals`.

**Heuristic:** quiet if the divergence detector ran within the last 4h
(its expected cadence), even if no signals were emitted.

```sql
SELECT MAX(created_at) AS last_run
FROM collector_cycle_stats
WHERE collector_name = 'divergence_detection'
```

If `last_run > NOW() - INTERVAL '4 hours'`, classify as `quiet`.

**Edge cases:**
- If `divergence_detection` is not present in `collector_cycle_stats` (the
  task is still part of the enrichment pipeline rather than the collector
  registry), fall through to `broken` rather than incorrectly suppressing
  the alert.

## Domains without a heuristic (default behaviour preserved)

The following domains keep the legacy "stale = broken" behaviour because
either (a) they are expected to have continuous activity, or (b) we have
not yet validated a safe heuristic:

- `psi_components`, `cda_extractions`, `wallets`, `wallet_profiles`, `edges`
- `smart_contracts`, `flows`, `cqi_compositions`, `discovery_signals`
- `governance_events`, all Circle 7 component domains
  (`lsti_components`, `bri_components`, `dohi_components`, `vsri_components`,
  `cxri_components`, `tti_components`)
- `contract_upgrades`, `contagion_events`, `validator_performance`,
  `sanctions_screening`, `enforcement_records`, `parent_company_financials`,
  `governance_proposals`, `contract_dependencies`,
  `contract_dependencies_snapshot`, `protocol_parameter_changes`,
  `protocol_parameter_snapshots`, `clustered_concentration`,
  `oracle_readings`, `oracle_stress_events`

If a domain in this list begins emitting frequent false alarms, add a
heuristic here and in `app/integrity_heuristics.py`.

## Implementation

`app/integrity_heuristics.py` exposes a single function:

```python
def classify_freshness(domain: str, age_hours: float, expected_hours: float) -> str:
    """Return 'ok' | 'broken' | 'quiet'."""
```

Call sites:
- `app/coherence.py:_check_freshness` — wraps each issue with the
  classifier; only `broken` results are appended to the issues list. `quiet`
  results are logged at INFO level.
- `app/integrity.py` — left unchanged; that module's per-domain freshness
  uses richer signals (row counts, coherence rules) and isn't subject to
  the same false-alarm class.

## Versioning

When changing a heuristic, bump the doc version
(`integrity_heuristics_v1` → `v2`) and note the change. The classifier
returns its version in `classify_freshness(..., return_version=True)` so
audit logs can capture the rule that suppressed an alert.
