# `actors` — design questions (2026-05-12)

Holdout from the v9.12 P2 main.py-legacy bundle. The standard hoist
fails because **two different writers stamp the same domain key
`actors` with semantically different payloads**.

See `docs/audits/2026-05-12-p2-main-py-legacy-investigation.md` for
the bundle-scope investigation.

## Substrate

```sql
SELECT COUNT(*), MAX(cycle_timestamp) FROM state_attestations WHERE domain = 'actors';
-- 114 rows, last 2026-05-12T10:19:17.679Z
```

Healthy cadence — but the 114 rows are a **mixed signal** from two
different writers tracking two different underlying tables.

## The multi-writer triangle

Five entry points touch the `actors` domain across the codebase:

| Caller | Function | Attests `actors`? | Payload shape |
|---|---|---|---|
| `main.py:307-321` | `run_agent_cycle()` -> attest | YES, caller-side | `{"assessments": N, "severities": {silent/notable/alert/critical: M}}` |
| `app/worker.py:1446-1447` | `run_agent_cycle()` (no attest) | NO | — |
| `app/worker.py:2299-2300` | `classify_all_active()` | YES, inside module | `{"classified": N, "reclassified": M, "by_type": {...}}` |
| `app/worker.py:2703-2707` | `classify_all_active()` | YES, inside module | same |
| `app/enrichment_worker.py:603-605` | `classify_all_active(limit=300)` | YES, inside module | same |

`run_agent_cycle()` (`app/agent/watcher.py`) writes the
`assessment_events` table (severity classification, broadcast
decisions, daily-cycle assessment). It is **not** about actor type.

`classify_all_active()` (`app/actor_classification.py`) writes
`wallet_graph.actor_classifications` with `actor_type` (autonomous_agent,
human, contract_vault). It is **the canonical "actors" writer**.

Both share domain key `actors` in `state_attestations`. Consumers
treat that key as a single freshness signal.

## Where consumers expect what

`app/coherence.py:34,73` lists `actors` with 24h cadence in
`ALL_DOMAINS` / `DOMAIN_FREQUENCIES`. The check is freshness-only
(`MAX(cycle_timestamp)`); it doesn't care which writer produced the
row.

`app/pulse_generator.py:250` lists `actors` in the daily-pulse state
root domain list.

`app/integrity.py:420` — `freshness_query` for actor classification
runs on `wallet_graph.actor_classifications.classified_at`, not on
`state_attestations`. So the integrity check side-steps the
multi-writer entirely; the consumer that does suffer is coherence.

## Why hoisting doesn't help

A clean `main.py:317` deletion would remove the `assessments` shape
from the domain. Coherence would then see only the `classify_all_active`
attests, which is what it actually wants. But:

1. There's no canonical wrapper home for `run_agent_cycle` that
   matches the #198 shape (the agent watcher is event-triggered, not
   freshness-gated).
2. The `assessments` data is still meaningful as a freshness signal
   for **assessment_events**, just not under domain `actors`.

## Design questions

**Q1.** Should `run_agent_cycle()` attest a **separate domain**
(`assessments` or `assessment_events`)? Migration impact: would need
to add the new domain to `coherence.ALL_DOMAINS` + `pulse_generator`
domain list + any report consumers. Maybe 5-10 lines.

**Q2.** Or should `main.py:317` simply delete — since
`worker.py:1446-1447` already calls `run_agent_cycle` (no attest) AND
the consumers' real signal for "actors are being classified" already
comes from the `classify_all_active` writers? The cost: agent-cycle
freshness becomes invisible.

**Q3.** Is there value in **collapsing** the four `classify_all_active`
call sites (worker.py:2299, worker.py:2703, enrichment_worker.py:603,
plus implicit from agent) down to a single scheduled entry? They're
called from different cadences (every cycle vs slow cycle vs
enrichment task vs background) — but all do the same work and all
attest the same way.

**Q4.** What's the relationship between agent-cycle assessments
(`assessment_events`) and actor classifications
(`wallet_graph.actor_classifications`)? Should they share a domain
key intentionally (as a unified "behavioral signals" surface) or
intentionally split?

## Recommendation

Q1 is the cleanest fix: introduce domain `assessments`, repoint
`main.py:317` (or the wrapper that replaces it) to write to that
domain instead of `actors`. Then `actors` becomes the
classification-only signal it was meant to be.

The Q3 collapse (consolidating four `classify_all_active` callers) is
a separate cleanup — worth surfacing as its own draft issue, not
gating this one.

## Halt condition for the eventual refactor PR

After deploy, the post-substrate should show:
- `domain = 'actors'` rows from `classify_all_active` only (payload
  has `classified` / `by_type` keys, never `assessments` / `severities`).
- New `domain = 'assessments'` rows from agent cycles, with
  `assessments` / `severities` payload.

If `actors` is still receiving `severities` payloads, the agent-cycle
caller wasn't updated. If `assessments` rows don't appear, the new
domain isn't on the live path.

## References

- `app/agent/watcher.py::run_agent_cycle` (assessment writer)
- `app/actor_classification.py:387-401` (classification writer)
- `app/worker.py:1446-1447,2299-2300,2703-2707` (worker callers)
- `app/enrichment_worker.py:603-605` (enrichment caller)
- `app/coherence.py:34,73` (consumer — freshness only)
- `app/pulse_generator.py:250` (consumer — state-root list)
- `docs/audits/2026-05-12-p2-main-py-legacy-investigation.md` (parent)
