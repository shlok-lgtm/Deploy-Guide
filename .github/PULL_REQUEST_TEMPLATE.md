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

## Test plan

<!-- Bulleted checklist of TODOs for testing this PR. -->

- [ ]

<!--
Reminder: don't push to `main` directly. One thing per PR. After merge,
update docs/sessions/<date>-<context>.md if this PR was part of an
orchestrated session.
-->
