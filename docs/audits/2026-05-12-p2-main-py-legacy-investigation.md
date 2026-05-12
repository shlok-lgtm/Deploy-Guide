# v9.12 P2 main.py-legacy investigation — 2026-05-12

Scope: four `attest_state(...)` call sites in `main.py::run_worker_loop`
(lines 167, 301, 317, 482 on origin/main `1667d0e`) flagged as DUAL_WRITER
candidates for the standard v9.12 hoist (delete caller-side inline, route
through module-canonical wrapper per the #179 / #193 / #198 pattern).

Verdict: **3 of 4 surface design questions; only 1 is mechanical.**

This doc is the gate report. The single mechanical hoist landed in the
PR titled `refactor(v9.12): P2 main.py-legacy — wallet_profiles single
mechanical hoist`. The three holdouts get their own design-q docs and
draft issues.

## Substrate (raw rows, verbatim)

```
projectId: small-scene-57890564  (BasisProtocol Neon)
ran_at:    2026-05-12T10:19Z
```

```sql
SELECT domain, COUNT(*) AS rows, MAX(cycle_timestamp) AS last_at,
       MIN(cycle_timestamp) AS first_at
FROM state_attestations
WHERE domain IN ('cda_extractions','wallet_profiles','actors','edges')
   OR (domain LIKE 'data_layer:%'
       AND domain ~ 'cda|wallet_profile|actor|edge')
GROUP BY 1 ORDER BY 1;
```

```
domain            | rows | last_at                  | first_at
actors            |  114 | 2026-05-12T10:19:17.679Z | 2026-04-10T05:16:26.409Z
wallet_profiles   |   51 | 2026-05-12T08:15:34.319Z | 2026-04-12T10:02:59.330Z
(cda_extractions and edges return zero rows)
```

Underlying-table fresh-record counts (paired):

```
cda_vendor_extractions:        267 total, 82 in 7d, last 2026-05-11T17:07
wallet_graph.wallet_edges:  1,011,156 total, 348,852 updated in 7d, last 2026-05-12T09:23
assessment_events:                       769 in 7d, last 2026-05-12T10:13
wallet_graph.wallet_profiles:         93,061 updated in 7d, last 2026-05-12T09:26
```

So `cda_extractions` and `edges` have **healthy underlying-table churn
but zero `state_attestations` rows ever**. That's a coherence-sweep
red flag — `app/coherence.py:30,33` lists both domains in `ALL_DOMAINS`
with expected cadences (24h for cda, 12h for edges).

## Per-domain diagnostic table

Schema-compatibility column refers to whether the existing module-side
attest payload would slot into the same `state_attestations` row shape
the consumers expect (`app/coherence.py`, `app/report.py::_get_state_hashes`,
`app/pulse_generator.py`).

| domain | main.py call | module-canonical writer | module attests already? | same domain key? | wrapper exists? | substrate rows | verdict |
|---|---|---|---|---|---|---|---|
| `cda_extractions` | `main.py:167` | `app/services/cda_collector.py::run_collection` | YES (`cda_collector.py:1289-1309`) — same `INTERVAL '2 hours'` window, same domain key | YES | NO `run_*_scheduled` wrapper | **0** | **design-q** |
| `wallet_profiles` | `main.py:301` | `app/indexer/profiles.py::rebuild_all_profiles` | YES (`profiles.py:191-210`) — covers `built > 0` -> records and `built == 0` -> `ran_no_results` | YES | NO wrapper (`rebuild_all_profiles` IS the entry) | **51** | **mechanical** |
| `actors` | `main.py:317` | conflated — see below | NO inside `app/agent/watcher.py`; YES inside `app/actor_classification.py::classify_all_active:387-401` (different writer, same domain) | YES domain key, but **semantically different payload** | NO wrapper | **114** | **design-q** |
| `edges` | `main.py:482` | `app/indexer/edges.py::run_edge_builder` | YES (`edges.py:447-464`) — gated `if total_edges > 0` (NEW edges only) | YES | NO wrapper | **0** | **design-q** |

## Why each design-q is a design-q (not just a hoist)

### cda_extractions — symmetric empty-window gate

Both writers use:

```python
"SELECT ... FROM cda_vendor_extractions WHERE extracted_at > NOW() - INTERVAL '2 hours'"
```

and `attest_state(...)` short-circuits to `return ""` if records is
empty (`app/state_attestation.py:63-64`). The daily CDA cycle takes
materially more than 2h on prod; by the time `run_collection()`
returns and the SELECT fires, the in-cycle inserts have already aged
past the 2h window in many cases. Result: both DUAL_WRITER ends see
zero records, both no-op, `state_attestations` stays empty.

A pure hoist (delete `main.py:167`) doesn't change this — the
identical bug exists at `cda_collector.py:1296`. The design question
is "how should `cda_extractions` attestation actually work":

1. Drop the 2h window, attest **all extractions from this run** by
   tracking inserted IDs in `run_collection()` (refactor scope).
2. Or attest a **summary payload** (count + cycle outcome) at the
   end of `run_collection()` regardless of window (matches the
   #198 / #193 `skipped_fresh / ran / error` shape).
3. Or change the window to match expected cycle length (~6h).

Holdout doc: `docs/audits/2026-05-12-cda-extractions-design-questions.md`.

### actors — multi-writer triangle with semantic conflation

`main.py:317` writes `{"assessments": N, "severities": {...}}` to
domain `actors` after `app/agent/watcher.py::run_agent_cycle()`, which
produces **assessment events** (table `assessment_events`).

`app/actor_classification.py:387-401` writes
`{"classified": N, "reclassified": M, "by_type": {...}}` to domain
`actors` from a **different writer** (`classify_all_active`) which
populates `wallet_graph.actor_classifications`.

Both are stamped under domain `actors`. Consumers
(`app/coherence.py:73`, `app/pulse_generator.py`, `app/report.py:327`)
treat the domain as a single freshness signal — they don't know they're
reading from two different writers with different payload shapes.

Three more writers compound it: `app/worker.py:1446-1447` calls
`run_agent_cycle()` (no attest), `worker.py:2299-2300` calls
`classify_all_active()` (attests internally), `worker.py:2703-2707`
calls `classify_all_active()` again (attests internally),
`enrichment_worker.py:603-605` calls `classify_all_active()`
(attests internally).

So actors substrate (`114 rows, last 2026-05-12T10:19`) is composed of
agent-cycle assessments **and** actor-classification batches mixed.
A naive hoist removes one without addressing the conflation.

Design questions:
1. Should `run_agent_cycle()` attest a separate domain (`assessments`
   or `assessment_events`)? Migration impact: coherence.py + report.py
   + pulse_generator.py both reference `actors`.
2. Or should `main.py:317` simply delete (since worker.py:1446 already
   calls `run_agent_cycle` and consumers see actor_classification
   attests already)?
3. What's the canonical wrapper home for the four agent/classify call
   sites — single entry, or two?

Holdout doc: `docs/audits/2026-05-12-actors-design-questions.md`.

### edges — gate skips the steady state

`main.py:482`: `if edge_result.get('total_edges_created', 0) > 0`
`edges.py:450`: `if total_edges > 0` (same gate, same semantics)

`total_edges` is computed from `result["edges_upserted"]` at
`edges.py:419` — that's the **upsert count returned per wallet**, not
inserted-only. In steady state with 1M+ existing edges, almost every
edge for a known wallet is an update of an existing row. New edges
are rare; the `> 0` gate rarely opens.

Substrate confirms: 348,852 edges updated in 7d, but `state_attestations`
has zero `edges` rows ever. Both writers gate identically; neither fires
in steady state. A pure hoist doesn't fix it.

Design questions:
1. Should the attest fire **always at end-of-run**, with payload like
   `{"chain": ..., "wallets_processed": ..., "edges_upserted": ...,
   "new_edges_inserted": ...}`? (Matches #193 `skipped_fresh / ran /
   error` shape.)
2. Or split `edges_upserted` into `inserted` vs `updated` and only
   attest on inserts? (Original intent, but the field doesn't exist
   on the explorer response.)
3. Coherence cadence says 12h — but main.py gate is 10h between
   `run_edge_builder` invocations. Should these be coupled?

Plus a related substrate finding: the attest call at `edges.py:451`
passes `chain` as the **third positional arg** to `attest_state`,
which is the `entity_id` parameter. That's actually correct (chains
are entities), but it means each chain produces a separate-entity
attestation row, and the consumer queries that ask for `domain='edges'
AND entity_id IS NULL` would never match. This is a small design Q on
its own. (`app/state_attestation.py:88` — `entity_id IS NULL` branch.)

Holdout doc: `docs/audits/2026-05-12-edges-design-questions.md`.

### wallet_profiles — the only mechanical case

`main.py:301`: gated `if profile_result.get('built', 0) > 0` -> attest
`profiles.py:194`: same gate -> attest with `{"built", "total"}`
`profiles.py:196-197`: ALSO has `else` branch -> attest with
`{"status": "ran_no_results"}`

So the module covers both gates; the caller-side is a pure duplicate.
Removing `main.py:298-303` removes one of the two writes per cycle
without changing the substrate cadence.

Substrate confirms: `wallet_profiles` shows 51 rows since 2026-04-12,
last 2026-05-12T08:15 — fresh and within the 24h cadence in
`coherence.py:71`.

This is the single mechanical hoist that landed in the PR.

## Out-of-worker consumers (Lesson 10)

Full-repo grep for the four domain strings turned up these consumers
that a single-file grep would miss:

```
app/coherence.py:30,33    -> ALL_DOMAINS list (freshness sweep)
app/coherence.py:69,72    -> DOMAIN_FREQUENCIES (24h / 12h cadence)
app/report.py:112,327     -> _get_state_hashes(...) for SII + wallet reports
app/pulse_generator.py:249-250 -> daily-pulse state-root domain list
app/integrity.py:409      -> "edges" integrity check definition
app/ops/entity_views.py:219 -> entity-view CDA extraction surface
app/ops/entity_routes.py:313 -> entity routes consume CDA list
```

All of these read from `state_attestations`. The 0-row substrate for
`cda_extractions` and `edges` means these consumers are silently
serving stale/missing freshness data. That makes the design-q decisions
above urgent rather than cosmetic.

## What landed in the PR

Just `main.py:297-303` deletion (the wallet_profiles inline). All
other changes are the comment block referencing this audit doc.

```
 main.py | 18 ++++++++++++------
 docs/audits/2026-05-12-p2-main-py-legacy-investigation.md | (this file)
 docs/audits/2026-05-12-cda-extractions-design-questions.md | (new)
 docs/audits/2026-05-12-actors-design-questions.md | (new)
 docs/audits/2026-05-12-edges-design-questions.md | (new)
```

Total diff: well under 500 lines (the bundle target). The original
"4 mechanical -> bundle PR" path is **not viable** because three of
the four are not mechanical.

## References

- #179 (dex_pool_ohlcv pilot — original v9.12 pattern)
- #193 (peg_monitor scheduled wrapper — 3-domain coupled write)
- #198 (exchange_snapshots — most recent #179 application)
- #199 (main.py:342-344 silent-swallow fix — adjacent, different lines)
- `app/state_attestation.py` (attest API surface)
- `app/coherence.py` (consumer of all four domains)
