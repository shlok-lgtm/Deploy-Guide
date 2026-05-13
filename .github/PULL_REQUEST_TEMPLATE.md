## Summary

<!-- 1–3 bullets on what changed and why. -->

## Substrate verification

Per lesson 7 (verification must cite substrate, not expectation), no PR
claims its change took without quoting substrate. Fill exactly one of
the two sections below. If neither applies (pure docs/refactor with no
runtime contract), state that explicitly under "N/A" and explain why.

### Pre-deploy (theoretical / pure docs / no runtime contract)

The query the substrate should answer after deploy, and the elapsed
cadence to wait before checking:

```sql
-- query the deploy verifier will run after the next cadence elapses
```

Expected post-deploy: <!-- e.g. "domain X freshness < 30 min, COUNT(*) increments" -->

### Post-deploy (code touches a running system; result already known)

```sql
-- query
```

Result:

```
-- output
```

### N/A

<!-- Leave the two sections above blank and explain why no substrate is
applicable. The CI gate (.github/workflows/pr-substrate-gate.yml)
requires this section header to appear when the others are empty. -->

## Wrapper ran-branch verification (v9.12 refactors only)

For PRs that wire a module-canonical wrapper (scheduled or unscheduled)
into the slow/fast cycle, substrate verification MUST distinguish the
wrapper's `ran` branch from any fallback writer (heartbeat helper,
sibling scheduler, multi-writer triangle, or pre-existing module path).

Acceptable signatures:
- `record_count` differs from fallback-writer signature (e.g.,
  wrapper writes N=stablecoin_count rows per cycle; heartbeat writes
  N=1)
- `batch_hash` uniqueness pattern matches wrapper's payload shape
  (heartbeat tends to produce repeating hashes when payload is fixed)
- Direct substrate-table row counts advance past pre-deploy baseline
  at the wrapper's specific cadence

PRs MUST quote the actual signature observed and explain why fallback
writers cannot produce it. "≥1 attestation row in 4h" is INSUFFICIENT
— heartbeat fallback satisfies it vacuously.

Reference: PRs #220, #221 (open follow-up issues from the May-12 audit
batch). Lesson 12 (`docs/basis_punchlist_2026_05_11.md`).

## Test plan

<!-- Bulleted checklist of TODOs for testing this PR. -->

- [ ]

<!--
Reminder: don't push to `main` directly. One thing per PR. After merge,
update docs/sessions/<date>-<context>.md if this PR was part of an
orchestrated session.
-->
