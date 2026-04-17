# Track-Record Outcome Tagging — Methodology

This document is the public contract for how Basis scores a committed
track-record event after 30, 60, and 90 days. It describes exactly what
`app/track_record.py::score_outcomes` does — no more, no less.

## Definitions

A **track-record event** is a row in `track_record_commitments` that was
anchored on the Oracle contract via `publishTrackRecord`. The columns
relevant to outcome tagging are:

- `event_type` — one of `divergence`, `rpi_delta`, `coherence_drop`,
  `score_change`.
- `entity_slug` — the entity the signal was about.
- `event_timestamp` — when the event fired (not when it was committed).
- `score_before`, `score_after` — the entity's score immediately before
  and at the event (both floats or nullable).
- `direction` — string: `"up"`, `"down"`, or anything else (treated as
  directionless).

## Outcome computation

A sweeper (`score_outcomes()`) runs on every keeper cycle. For each row
where `outcome_90d IS NULL` and `event_timestamp` is at least 30 days in
the past, it processes each horizon (30, 60, 90 days) independently.

```
baseline = score_after IF score_after IS NOT NULL ELSE score_before

for horizon in (30, 60, 90):
    target_t = event_timestamp + horizon days
    if target_t is in the future:     leave outcome_Nd NULL (pending)
    after    = _score_at(entity_slug, target_t)   # most recent score on or before target
    if after is None:                 skip this horizon (no data)

    delta    = after − baseline
    if direction == "down":  signed = −delta   # falling vindicates a "down" call
    if direction == "up":    signed =  delta   # rising vindicates an "up" call
    else:                    signed = abs(delta)  # magnitude only
    outcome_Nd       = signed           # stored as a float
    outcome_Nd_at    = target_t         # when the outcome was sampled
```

## Column semantics

| column          | type    | meaning                                        |
|-----------------|---------|------------------------------------------------|
| `outcome_30d`   | float   | signed delta at t+30d; positive = call vindicated |
| `outcome_60d`   | float   | same, at t+60d                                 |
| `outcome_90d`   | float   | same, at t+90d                                 |
| `outcome_30_at` | timestamp | when the t+30d sample was taken              |
| `outcome_60_at` | timestamp | when the t+60d sample was taken              |
| `outcome_90_at` | timestamp | when the t+90d sample was taken              |

## Reader-side labels (not stored)

No label string is written to the database. The values are raw signed
floats, and the reading application picks a threshold. The reference
threshold — used by the SSR page at `/track-record` and the API — is
5 index points:

| condition                 | label               |
|---------------------------|---------------------|
| `outcome_Nd ≥ +5`         | `correct`           |
| `outcome_Nd ≤ −5`         | `wrong`             |
| `−5 < outcome_Nd < +5`    | `partially_correct` |
| `outcome_Nd IS NULL`      | `pending`           |

The 5-point threshold matches the default score-change detection
threshold in `detect_score_change_events`: an event is only worth
committing iff a move of that size is meaningful, so the same magnitude
is also the bar for confirming the call.

## Directionless events

When `direction` is neither `"up"` nor `"down"` (e.g., a coherence drop
that is about data hygiene), the stored value is `abs(delta)`. It can
never be negative, so such events will never be labeled `wrong` — only
`correct` or `partially_correct`. This is intentional: a coherence-drop
signal is a "something is off, look closer" claim, not a directional
prediction.

## Tuning resistance

**This rule is not yet committed on-chain.** A contributor with write
access to `app/track_record.py::score_outcomes` can change the sign
convention or the sampling logic. A contributor editing the reader-side
threshold can retroactively re-label prior events.

To close that gap we plan to:

1. Add `publishMethodologyHash(bytes32 methodologyId, bytes32 hash)` to
   the Oracle contract, write-once per `methodologyId`.
2. Compute `sha256(canonical(docs/methodology_track_record_outcomes.md))`
   and anchor it under
   `methodologyId = keccak256("methodology:track-record-outcomes:v1")`.
3. Persist that hash on every `track_record_commitments` row as
   `outcome_methodology_hash`, binding each committed event to a frozen
   ruleset.

Until that ships, the on-chain audit trail covers the *events* but not
the *ruleset* used to grade them. This is an acknowledged gap, tracked
against V5 of `docs/bucket_a_verification_report.md`.
