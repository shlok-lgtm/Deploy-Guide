# Bucket A — Verification Report (V1–V6)

**Branch:** `claude/track-record-disputes-5RhQP`
**Code commit under review:** `737fbbe` (feat: track-record, crisis replays,
historical backfill, disputes)
**Date of this report:** 2026-04-17

This document answers the V1–V6 verification questions honestly. Where work
has not actually been executed against production systems, this report says
so. Nothing has been rounded up.

---

## V1 — `publishTrackRecord` mainnet deployment

**Question:** Is `publishTrackRecord` deployed to Base mainnet and Arbitrum
mainnet? What are the deployment transaction hashes?

**Answer: NOT DEPLOYED.**

Evidence:

- The function is present in source at
  `src/BasisSIIOracle.sol` (added in commit `737fbbe`).
- No deploy script references `publishTrackRecord`. `script/` contains only
  `Deploy.s.sol` (original `BasisOracle` constructor) and
  `DeployRating.s.sol`. Neither was updated for the new methods.
- No `broadcast/` directory exists in the repo, meaning no `forge script …
  --broadcast` run has been performed from this checkout.
- `forge` is not available in this environment, so compilation of the new
  source was not verified locally. Solidity-level bugs in the added
  `publishTrackRecord` / `publishDisputeHash` code paths cannot be ruled out
  until a build runs.
- The deployed oracle addresses referenced by `keeper/config.ts`
  (`BASE_ORACLE_ADDRESS`, `ARBITRUM_ORACLE_ADDRESS`) point to the
  **previous** `BasisOracle` bytecode, which does not contain
  `publishTrackRecord` or `publishDisputeHash`. Calling those selectors
  against the current live contract would revert.

**Status:** A1 on-chain deployment — **blocker, not done**. Code artifact
exists; operational rollout pending. The keeper cycle steps 8 and 9 that
`keeper/index.ts` executes will fail at the RPC layer against the current
mainnet contracts because the selectors do not exist.

---

## V2 — Track-record event backfill execution

**Question:** Has `scripts/backfill_track_record_events.py` been run against
production? How many events were detected, persisted, and committed on-chain?

**Answer: NOT RUN.**

Evidence:

- The script exists at `scripts/backfill_track_record_events.py`.
- It has never been invoked in this environment. There is no log artifact
  from the script and `DATABASE_URL` has not been used to reach a production
  Postgres instance from this session.
- Because migration `074_track_record_commitments.sql` has not been applied
  to production (no migration runner was executed either), the target table
  does not exist in production Postgres. The script would fail at the first
  `INSERT`.
- Because of V1, on-chain commitment counts are zero regardless.

**Status:** A1 acceptance criterion "10 real events anchored on-chain" —
**not met**. The detection code (`app/track_record.py`) is present and its
thresholds (>5 point SII delta, >10 point RPI delta, divergence signals,
coherence drops) are code-level defaults, not calibrated against a real
30-day window.

---

## V3 — Crisis replay determinism claim (Claim A vs Claim B)

**Question:** Which of these is true?
- **Claim A:** Replays use stored/generated input vectors and verify that
  recomputation is *consistent* with those inputs. They do not re-derive
  scores from primary historical data.
- **Claim B:** Replays re-derive scores from archived primary source data
  (on-chain snapshots, historical price feeds, raw governance records).

**Answer: Claim A is true. Claim B is false.**

Evidence:

- `scripts/generate_crisis_replays.py` contains a hard-coded `REPLAYS` list
  of 15 crises. Each entry carries component values that are **plausible
  synthetic approximations** of what SII inputs likely looked like at the
  event, not values extracted from archived primary sources.
- Each `crisis_replays/<slug>/inputs.json` was written by that generator, so
  the component values are the generator's approximations, not raw archived
  data.
- `crisis_replays/run.py` verifies:
  ```
  sha256(canonical(inputs.json))                    == input_vector_hash
  sha256(input_vector_hash || version || scores)    == computation_hash
  ```
  i.e., it verifies that the pinned hashes match the stored inputs. It does
  not verify that the stored inputs match historical reality.
- There is no reference to an archival data source (e.g., chain snapshot at
  block N, CoinGecko historical endpoint, forum archive URL) in any
  `inputs.json` or `README.md`.

**What "deterministic replay" therefore means in this repo:** given the
stored input vector, the scoring engine will always produce the same score
and hash, byte-identically, across machines and time. That is the
reproducibility guarantee. It is not a guarantee that the input vector is
the canonical historical truth of the event.

**Remediation in this PR:** a disclosure paragraph has been added to
`crisis_replays/README.md` and to each of the 15 per-crisis `README.md`
files stating Claim A explicitly.

---

## V4 — Historical score backfill execution

**Question:** Has `scripts/backfill_historical_scores.py` been run for each
of PSI, RPI, LSTI, BRI, DOHI, VSRI, CXRI, TTI? How many entities and how
many weekly rows were written per index?

**Answer: NOT RUN for any index.**

Evidence:

- Script exists at `scripts/backfill_historical_scores.py` with `--index`
  and `--max-entities` flags.
- It has not been invoked in this environment.
- Migration `076_historical_score_backfill.sql` has not been applied to
  production, so the target table does not exist there.
- `app/historical_score_backfill.py` has `DEFINITION_MAP` keyed by eight
  index slugs. Three of those eight (LSTI, BRI, DOHI, VSRI, CXRI, TTI) are
  referenced by the keeper/index definitions layer but I have not
  independently confirmed each has a complete, shippable index definition
  file in `app/index_definitions/`. The backfill runner will raise for any
  slug whose definition is stubbed.

**Status:** A3 acceptance criterion "retroactive backfill with confidence
surface for all eight indices" — **not met**. Code path exists; execution
pending. Confidence tag tiers (`high`/`medium`/`low`/`sparse`/`bootstrap`)
are defined but untested against real coverage percentages.

---

## V5 — Outcome tagging ruleset

**Question:** What is the exact rule used to label a track-record event
"correct" / "partially correct" / "wrong" at t+30/60/90d? Is the ruleset
itself committed on-chain so it cannot be retroactively tuned?

**Answer: Rule is automated and content-addressable in this commit; it is
NOT yet committed on-chain.**

The rule (as implemented in `app/track_record.py::score_outcomes`):

1. At commit time the event has an `event_kind` (`divergence`, `rpi_delta`,
   `coherence_drop`, `score_change`) and a signed `expected_direction`
   (+1 if we flagged the entity was strengthening, −1 if weakening).
2. At t+30d, t+60d, t+90d we fetch the entity's score at that date.
3. `outcome_delta_Nd = (score_at_t_plus_N − score_at_commit) ×
   expected_direction`.
4. Label:
   - `correct`           if `outcome_delta_Nd ≤ −5` (event was weakening and
                         score did drop ≥5, or inverse for strengthening)
   - `wrong`             if `outcome_delta_Nd ≥ +5`
   - `partially_correct` if `|outcome_delta_Nd| < 5`
   - `pending`           if the t+Nd date is in the future

The ±5 threshold is the same number used as the score-change detection
threshold. This is documented in `docs/methodology_track_record_outcomes.md`
(created in this PR).

**On-chain commitment of the ruleset itself:** **NOT DONE.** No
`publishMethodology(rulesetHash)` call exists on the Oracle contract. The
ruleset can be retroactively tuned by editing
`app/track_record.py::score_outcomes`. To close this gap:

- Add `publishMethodologyHash(bytes32 methodologyId, bytes32 hash)` to the
  Oracle (write-once per `methodologyId`).
- Hash the canonical text of `docs/methodology_track_record_outcomes.md`
  and commit it.
- Reference that hash in every track-record commitment.

That work is **not** in this branch.

---

## V6 — Bucket B4 scope check

**Question:** Confirm that B4 (48-hour issuer pre-score response window) is
not in this Bucket A branch.

**Answer: Confirmed — B4 is NOT in this branch.**

Evidence:

- `git log --oneline` on `claude/track-record-disputes-5RhQP` shows only
  commit `737fbbe` past `main`. That commit touches track-record, crisis
  replay, historical backfill, and dispute code paths.
- There is no code path or route mentioning "pre-score", "issuer response",
  or a 48h window added in this branch.
- Grep for `pre_score|issuer_response|response_window|48h|48_hour` in
  `app/`, `keeper/`, `frontend/`, `migrations/` (074–077) returns no hits
  tied to Bucket B4.

**Status:** B4 belongs to Bucket B and will be addressed in a separate
branch.

---

## Summary — honest status of Bucket A

| Item | Code shipped | Tests | Prod DB migrated | Mainnet deployed | Script executed | Acceptance met |
|------|--------------|-------|------------------|------------------|-----------------|----------------|
| A1 track record | yes | no automated | no | **no** | **no** | **no** |
| A2 crisis replays | yes (15) | `run.py --all` OK | no (DB loader exists) | n/a | generator only | re-derivation yes / historical-truth no |
| A3 historical backfill | yes | no | no | n/a | **no** | **no** |
| A4 disputes | yes | no | no | **no** | n/a | **no** |

Bucket A should be treated as **code-complete but operationally not yet
shipped**. Canonical documents (V9.3 amendment, doc updates batch) must
reflect that and not imply these capabilities are live.
