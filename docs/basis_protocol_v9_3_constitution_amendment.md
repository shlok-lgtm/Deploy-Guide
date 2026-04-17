# Basis Protocol — V9.3 Constitution Amendment

**Status:** draft, tied to branch `claude/track-record-disputes-5RhQP`
(commit `737fbbe`). Nothing in this amendment is ratified until the
operational gates below are closed.

This amendment extends V9 with four new commitments. Each is stated here
at two levels: **what shipped as code** in this branch (honest, narrow),
and **what the capability will become** once operational rollout is
finished. The code-level status is authoritative. The full-capability
status is aspirational and MUST NOT be referenced as live until the
"gates" list at the bottom clears.

## Article I — Track record is public and on-chain

**Commitment:** Every consequential call that Basis makes — a divergence
signal, an RPI delta, a coherence drop, a score move above the event
threshold — produces a row in `track_record_commitments`, and the hash
of that row is anchored on the Oracle contract on Base and Arbitrum.
After 30, 60, and 90 days a deterministic outcome value is written
against each row; the outcome ruleset is published at
`docs/methodology_track_record_outcomes.md`.

**What shipped in this amendment:**

- `migrations/074_track_record_commitments.sql` — schema.
- `app/track_record.py` — event detection (`detect_divergence_signals`,
  `detect_rpi_delta_events`, `detect_coherence_drops`,
  `detect_score_change_events`) and outcome scoring (`score_outcomes`).
- `scripts/backfill_track_record_events.py` — 30-day backfill CLI.
- `src/BasisSIIOracle.sol` — `publishTrackRecord`, `getTrackRecord`,
  `trackRecordCount` with write-once guards.
- `keeper/index.ts` — step 8 fetches pending commits and anchors them.
- `docs/methodology_track_record_outcomes.md` — the ruleset document.
- SSR page `/track-record` rendered by `app/server.py`.

**What has NOT yet happened (pending gates):**

- The new Oracle bytecode is not deployed. The live Base and Arbitrum
  contracts do not contain `publishTrackRecord`.
- The backfill script has not been executed against production. Event
  count detected: **0**. Events committed on-chain: **0**.
- The outcome ruleset is published as text but not yet hashed on-chain.
  It remains mutable by anyone with write access to `app/track_record.py`.

Claiming "Basis has a public on-chain track record" requires all three
of those to be closed.

## Article II — Historical calls are reproducible

**Commitment:** For each major crisis in the covered-entity universe,
Basis publishes an input vector and a pinned score such that any third
party can, with this repo and a Python interpreter, produce the same
score and the same hash. The crisis replay library is a deterministic
consistency check.

**What shipped in this amendment:**

- `crisis_replays/` with 15 self-contained replay directories.
- `crisis_replays/run.py` — reference implementation; `--all` verified OK.
- `migrations/075_crisis_replays.sql` — table for persisting replay
  metadata.
- `app/crisis_replays_loader.py` — loads replays from disk into Postgres.
- SSR page `/crisis-replays` and detail page `/crisis-replays/{slug}`.

**Honest scope disclosure (now in `crisis_replays/README.md` and each
per-crisis `README.md`):** replays verify consistency, not historical
truth. Input vectors are plausible synthetic approximations written by
`scripts/generate_crisis_replays.py`; they are NOT extracted from
archived primary sources. Upgrading to full historical re-derivation is
explicitly out of scope for this amendment.

## Article III — Historical scores have a confidence surface

**Commitment:** For every index in the covered universe (PSI, RPI,
LSTI, BRI, DOHI, VSRI, CXRI, TTI), Basis will publish a weekly series
reconstructed walk-forward from backfilled inputs, with a confidence
tag on every point.

**What shipped in this amendment:**

- `migrations/076_historical_score_backfill.sql` — schema with
  `confidence_tag`, `input_vector`, `input_vector_hash`,
  `computation_hash`.
- `app/historical_score_backfill.py` — `backfill_entity`,
  `backfill_index`, `backfill_all`, `get_series`, `_confidence_tag`
  tiering (`high` / `medium` / `low` / `sparse` / `bootstrap`).
- `scripts/backfill_historical_scores.py` — CLI.
- SSR surface under `/historical-scores`.

**What has NOT yet happened:**

- No run. No rows. Entity counts per index: **0**.
- Not all eight index definitions have been independently verified as
  shippable against the backfill harness. LSTI/BRI/DOHI/VSRI/CXRI/TTI
  each need at least one end-to-end smoke run before this article can
  be considered fulfilled.

## Article IV — Disputes are first-class and public

**Commitment:** Anyone with an Ethereum address can dispute any
published Basis score. The dispute ID, submission hash, counter-evidence
hash, and resolution hash are each anchored on the Oracle contract.
The methodology is published at `docs/methodology_disputes.md` and is
frozen by hash once the ratification gates close.

**What shipped in this amendment:**

- `migrations/077_disputes.sql` — `disputes` and `dispute_commitments`
  tables.
- `app/disputes.py` — `submit_dispute`, `attach_counter_evidence`,
  `resolve_dispute`, `get_dispute`, `pending_dispute_commits`,
  `mark_dispute_commit_published`; four content hashes per dispute.
- `app/server.py` — `POST /api/disputes`, `GET /api/disputes`,
  `GET /api/disputes/{id}`, `POST /api/disputes/{id}/counter`,
  `POST /api/disputes/{id}/resolve`, plus SSR pages `/disputes` and
  `/disputes/{id}`.
- `src/BasisSIIOracle.sol` — `publishDisputeHash`,
  `getDisputeCommitment` with write-once per `(disputeId,
  transitionKind)`.
- `keeper/index.ts` — step 9 fetches pending dispute commitments and
  anchors them under bytes4 tags `SUBM`, `CTRE`, `RSLV`.
- `docs/methodology_disputes.md` — public contract.

**What has NOT yet happened:**

- Oracle deployment is pending (same blocker as Article I).
- No dispute has been submitted. No commitment has been anchored.
- The methodology document is published as text but not hashed on-chain.

## Ratification gates

This amendment moves from "draft" to "ratified" only when **all** of the
following are true. Until then, external communication must reference
these capabilities using the narrow code-level language above.

1. `BasisOracle` with `publishTrackRecord` and `publishDisputeHash` is
   deployed to Base mainnet and Arbitrum mainnet; both deployment tx
   hashes are recorded in `docs/deployments.md`.
2. `scripts/backfill_track_record_events.py --days 30` has been run
   against production Postgres and produced ≥10 rows; those rows have
   been committed on-chain; the tx hashes are queryable from the SSR
   `/track-record` page.
3. `scripts/backfill_historical_scores.py` has been run for every one
   of the eight indices in `DEFINITION_MAP` without error, producing
   ≥1 row per index.
4. `publishMethodologyHash` is added to the Oracle and the three
   methodology documents (`methodology_track_record_outcomes.md`,
   `methodology_disputes.md`, `crisis_replays/README.md`) are each
   committed under distinct `methodologyId` values.
5. A third party reproduces at least one crisis replay on a clean
   checkout and the resulting `computation_hash` matches the one pinned
   in this repo.

These gates are enumerated to make it impossible to claim
"V9.3 is live" on the strength of code-shipment alone. Evidence for each
gate must be linkable to an on-chain tx, a DB row count, or a reproducer
artifact — not to a commit SHA.

## References

- `docs/bucket_a_verification_report.md` — honest V1–V6 status.
- `docs/methodology_track_record_outcomes.md` — Article I outcome rules.
- `docs/methodology_disputes.md` — Article IV lifecycle.
- `crisis_replays/README.md` — Article II consistency scope.
