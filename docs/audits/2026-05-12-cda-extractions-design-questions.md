# `cda_extractions` — design questions (2026-05-12)

Holdout from the v9.12 P2 main.py-legacy bundle. The standard hoist
(delete `main.py:167`, route through module-canonical wrapper) does
**not** fix the substrate problem this audit surfaces.

See `docs/audits/2026-05-12-p2-main-py-legacy-investigation.md` for
the bundle-scope investigation.

## Substrate

```sql
SELECT COUNT(*) FROM state_attestations WHERE domain = 'cda_extractions';
-- 0
```

Paired underlying table:

```sql
SELECT COUNT(*) AS total,
       COUNT(*) FILTER (WHERE extracted_at > NOW() - INTERVAL '7 days') AS in_7d,
       MAX(extracted_at) AS last
FROM cda_vendor_extractions;
-- total=267, in_7d=82, last=2026-05-11T17:07:17.405Z
```

So extractions are landing in the underlying table (82 in 7d), but
**not a single `state_attestations` row has ever been written for
domain `cda_extractions`**.

## The dual-writer pair (both broken in the same way)

`main.py:161-169`:

```python
from app.state_attestation import attest_state
from app.database import fetch_all as _fa
cda_rows = _fa("SELECT asset_symbol, field_name, extracted_value, source_url "
               "FROM cda_vendor_extractions "
               "WHERE extracted_at > NOW() - INTERVAL '2 hours'")
if cda_rows:
    attest_state("cda_extractions", [dict(r) for r in cda_rows])
```

`app/services/cda_collector.py:1289-1296`:

```python
from app.state_attestation import attest_state
recent = await asyncio.to_thread(fetch_all,
    "SELECT asset_symbol, field_name, extracted_value, source_url "
    "FROM cda_vendor_extractions "
    "WHERE extracted_at > NOW() - INTERVAL '2 hours'"
)
if recent:
    await asyncio.to_thread(attest_state, "cda_extractions",
                             [dict(r) for r in recent])
```

Identical SELECT, identical 2-hour window, identical attest pattern.

## Why both writers miss

`run_collection()` walks the entire issuer registry sequentially with
inter-issuer sleeps (`cda_collector.py:1281-1282` — `await asyncio.sleep(2)`)
plus per-step delays. On prod with the current registry size (≈ a dozen
active issuers) the full cycle exceeds 2 hours wall-clock when even one
issuer hits the Reducto PDF parsing path or the Parallel Task deep
research path.

By the time the closing SELECT fires, the earliest extractions inserted
in this cycle have already aged past the 2h window. Both ends of the
DUAL_WRITER see `recent = []`, both no-op, and `attest_state(domain,
[])` short-circuits to `return ""` (`app/state_attestation.py:63-64`).
The cycle finishes; nothing lands in `state_attestations`.

## Why hoisting doesn't help

A standard #179/#193/#198-shape hoist would:
- Delete `main.py:161-169`
- Wrap `cda_collector.run_collection` in a `run_collection_scheduled()`
  that adds freshness gate + `skipped_fresh / ran / error` attest

The problem: the **inside** of `run_collection` is broken too. The
existing `cda_collector.py:1289-1309` attest block has the same window
bug. Hoisting moves where the bug lives but doesn't fix it.

## Design questions

**Q1.** Should `run_collection` attest **all extractions inserted in
this run**, regardless of wall-clock window? Implementation: collect
inserted IDs into a list during the per-issuer loop, then run
`SELECT ... WHERE id = ANY(%s)` at the end. Pro: deterministic. Con:
buffers in memory.

**Q2.** Or should `run_collection` attest a **summary payload** (count
of extractions + cycle outcome + per-issuer success counts) regardless
of any SELECT? Matches #198 `skipped_fresh / ran / error` shape. Pro:
no window race. Con: loses per-extraction hash detail.

**Q3.** Or should the window expand to match expected cycle length
(say `INTERVAL '6 hours'` to cover worst-case)? Pro: smallest diff.
Con: still racy; coupled to cycle length tuning.

**Q4.** Coherence cadence says 24h (`coherence.py:69`). If we attest
per-cycle (~1/h) is that overweight signal? Or right (since each
cycle's freshness is meaningful)?

## Recommendation

Q2 + Q3 combined: build the `run_collection_scheduled` wrapper that
attests `skipped_fresh / ran / error` always (Q2), and inside `ran`
also keep an aggregated extraction-count attest (no SELECT — count
the inserts as they happen inside `_store_extraction`).

Estimated diff: ~80-120 lines (wrapper + inserted-count threading).
Substrate gate verification: post-deploy
`SELECT COUNT(*) FROM state_attestations WHERE domain = 'cda_extractions'
AND cycle_timestamp > NOW() - INTERVAL '2 hours'`
should return > 0 within 2 hours of next CDA cycle.

## Halt condition for the eventual refactor PR

After deploy, if the substrate query above still returns 0 after 4
hours, halt — `run_collection_scheduled` is not on the live path
(lesson 6 family).

## References

- #198 (exchange_snapshots scheduled-wrapper shape — same pattern)
- `app/coherence.py:30,69` (consumer)
- `app/pulse_generator.py:212,226,249` (consumer)
- `app/ops/entity_views.py:219` (consumer)
- `app/state_attestation.py:63-64` (the empty-records short-circuit)
- `docs/audits/2026-05-12-p2-main-py-legacy-investigation.md` (parent)
