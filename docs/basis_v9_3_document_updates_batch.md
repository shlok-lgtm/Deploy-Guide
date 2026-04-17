# Basis V9.3 — Document Updates Batch

This batch lists every document that must be updated to reflect V9.3.
Each entry is a concrete diff description ("what to change", "from what",
"to what") plus the **honesty constraint**: the update must NOT claim
the capability is live until the corresponding ratification gate in
`basis_protocol_v9_3_constitution_amendment.md` has closed.

Where a document is not yet in the repo, the entry is marked
`NEW` and the batch provides the full file to create.

---

## 1. `README.md` (top level)

**Action:** append a `## What's new in V9.3` section.

**Claim in narrow, code-level language:**

> V9.3 adds track-record commitments, crisis replays, historical-score
> backfill, and on-chain-anchored disputes at the code and schema level.
> On-chain deployment and production data backfill are in progress; see
> `docs/bucket_a_verification_report.md` for the current gate status.

**Forbidden phrasings** (would round up beyond current evidence):

- "Basis now publishes its track record on-chain"  (mainnet not deployed)
- "Every historical score is reproducible"         (backfill not run)
- "Any user can dispute a score"                   (endpoint exists but
                                                    Oracle not deployed)

**Permitted phrasings:**

- "Infrastructure for on-chain track record is in place; production
  deployment pending."
- "Crisis replays verify consistency of stored input vectors; historical
  primary-source re-derivation is out of scope for this release."

---

## 2. `STRATEGY.md`

**Action:** update the "Near-term capabilities" list.

**Add:**

- "Track-record commitments (code-complete, pending mainnet deploy)"
- "Dispute infrastructure (code-complete, pending mainnet deploy)"
- "Historical score backfill (code-complete, pending execution)"
- "Crisis replay library — 15 events, consistency-based (shipped)"

**Do NOT** move these to the "Shipped" list until the corresponding
gates in the constitution amendment close.

---

## 3. `AGENTS.md`

**Action:** add two new agent entry points.

**Add section:**

```
### Dispute agent
Endpoint:   POST /api/disputes
Purpose:    Submit a dispute against any published score hash.
Payload:    entity_slug, score_hash_disputed, submitter_address,
            submission_payload (JSON), optional index_kind,
            score_value_disputed.
Side effect: writes a row to `disputes` and a SUBM row to
             `dispute_commitments`.
Methodology: docs/methodology_disputes.md

### Track-record agent
Endpoint:   GET /api/track-record
Purpose:    Read the list of consequential events Basis has committed,
            with their 30/60/90-day outcomes.
Methodology: docs/methodology_track_record_outcomes.md
```

---

## 4. `docs/methodology_disputes.md`  (already exists)

**Action:** add a line in "Hash construction" referring to the pending
on-chain hash commitment of this document itself.

**Append:**

> Once V9.3 ratification gate 4 closes, the canonical form of this
> document will itself be hashed and anchored under
> `methodologyId = keccak256("methodology:disputes:v1")`. Until then the
> document is mutable in Git and should be pinned by commit SHA when
> cited externally.

---

## 5. `docs/methodology_track_record_outcomes.md`  (NEW, shipped)

Already created in this PR. No further edits needed; references the
pending on-chain hash commitment in its "Tuning resistance" section.

---

## 6. `docs/bucket_a_verification_report.md`  (NEW, shipped)

Already created in this PR. This is the ground-truth reference that
every other document in this batch points back to for status.

---

## 7. `docs/basis_protocol_v9_3_constitution_amendment.md`  (NEW, shipped)

Already created in this PR. Canonical amendment text; honesty-first
wording; five ratification gates enumerated.

---

## 8. `CLAUDE.md`

**Action:** add the four new articles to the "Architectural Patterns"
list, AND add a note in the "Do NOT" section to prevent over-claiming.

**Add to "Architectural Patterns":**

```
9. Track-record commitments — every consequential call is anchored on
   the Oracle with a deterministic outcome score at t+30/60/90d.
   (Code-complete; pending deploy.)
10. Crisis replay library — 15 crises, deterministic consistency check.
    Inputs are stored synthetic approximations, not archived primary
    sources.
11. Historical score backfill — walk-forward weekly reconstruction with
    confidence tags. Code-complete; pending execution.
12. Disputes — anyone can submit; four content hashes per dispute are
    anchored on-chain. Code-complete; pending deploy.
```

**Add to "Do NOT":**

```
- Do not claim V9.3 capabilities as "live" in user-facing copy, docs,
  or PR descriptions. Use the narrow code-level phrasing from
  docs/basis_v9_3_document_updates_batch.md until the ratification
  gates in the V9.3 constitution amendment are closed.
```

---

## 9. `frontend/src/App.jsx`  (UI copy, not yet updated)

**Action:** if a "What's new" banner is added for V9.3, its copy MUST
read exactly one of:

- "V9.3 infrastructure shipped — operational rollout in progress."
- "V9.3 — code complete, on-chain activation pending."

**Forbid:** "V9.3 is live" / "Now publishing track record on-chain" /
"All historical scores are now reproducible".

No frontend change is in this PR; this entry documents the rule so the
next frontend PR cannot drift.

---

## 10. `app/server.py` — route-level public copy

**Action:** the SSR headers rendered by `_render_track_record_html`,
`_render_crisis_replays_html`, `_render_disputes_html` MUST include a
visible banner while V9.3 is pre-ratification:

```
<div class="v93-banner">
  Basis V9.3 — infrastructure live, mainnet anchoring pending.
  See docs/bucket_a_verification_report.md.
</div>
```

This is a text-only banner; no CSS change required to ship it.
A future PR removes the banner once the gates close.

Not applied in this PR (server.py is production code and was touched
only for the new route handlers). Tracking as a follow-up.

---

## Honesty checklist (apply to every doc update in this batch)

Before merging any document that references V9.3, confirm:

- [ ] The update does not use the word "live" unless the corresponding
      ratification gate has closed.
- [ ] The update does not imply anchoring is in place unless an Oracle
      tx hash exists.
- [ ] The update does not imply a historical backfill exists unless a
      row count in production Postgres is cited.
- [ ] The update references `docs/bucket_a_verification_report.md` for
      current gate status.
- [ ] The update preserves Claim A (consistency-based) framing for
      crisis replays.

If any box fails, the update is rounded up and must be revised.

---

## Files changed by this PR (V9.3 doc set)

```
crisis_replays/README.md                       (edited: scope disclosure)
crisis_replays/<15 subdirs>/README.md          (edited: scope disclosure)
docs/bucket_a_verification_report.md           (new)
docs/methodology_track_record_outcomes.md      (new)
docs/basis_protocol_v9_3_constitution_amendment.md  (new)
docs/basis_v9_3_document_updates_batch.md      (new — this file)
```

README.md, STRATEGY.md, AGENTS.md, CLAUDE.md, frontend/src/App.jsx, and
app/server.py banner copy are **not** edited by this PR — their updates
require sign-off on the exact language and are enumerated above as
follow-up work.
