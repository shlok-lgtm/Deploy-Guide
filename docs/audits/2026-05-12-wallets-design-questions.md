# `wallets` v9.12 module-canonical design questions — 2026-05-12

**Status:** investigation-only. Diagnostic for the v9.12 DUAL_WRITER
sweep targeting the `wallets` `state_attestations.domain`. The
reference pattern (PRs #198, #193, #179) hoists the attest into a
single module-canonical scheduled wrapper. The wallets domain does
not fit that mechanical shape; this doc enumerates the divergences
so the operator can decide consolidation behavior per question before
any refactor PR is opened.

**Pattern source:** mirrors `2026-05-12-psi-discoveries-design-questions.md`.
Cite substrate, surface decisions, defer the code change.

**Important framing.** There is no `wallets` table being attested.
`wallets` is a `state_attestations.domain` value. Every writer calls
`attest_state("wallets", [payload_dict])`. The underlying data tables
are `wallet_graph.wallets`, `wallet_graph.wallet_holdings`,
`wallet_graph.wallet_risk_scores` — but the `wallets` attestation
domain is a freshness/audit-trail token, not a row-level provenance
proof.

The v9.12 migration plan entry at
`docs/audits/2026-05-11-module-canonical-migration-plan.md` (line 34)
lists the wallets domain as:

> `wallets` | `worker.py:2082`, also `worker.py:2787` (heartbeat) | — (driven by `app/indexer/pipeline.py`) | **P1** | Wave 1 + Wave 3 + Wave 5b.

That row is **substantively inaccurate**. The live worker.py attest
is at line **1978** (not 2082). The slow-cycle heartbeat is at line
**2644-2646** inside `_emit_slow_cycle_heartbeats` (not 2787 — that
appears in the table from copy-paste of the dex_pool_ohlcv row). The
module site claim "(none)" is wrong: `app/indexer/pipeline.py:813`
already attests inside `run_pipeline_batch` (PR #142 hoist). The
domain has **four** independent writer paths, not the two listed.

---

## 1. Call-site inventory

| # | file:line | role | introduced | when it fires |
|---|---|---|---|---|
| A | `app/worker.py:1957-1980` | Worker-side scheduler attest in `run_scoring_cycle`. Status payload: `"ok"` (result returned) / `"early_return"` (`{"error": …}` returned) / `"failed"` (raised). | #155 (Wave-3 P4) | Every fast cycle, after `run_pipeline_batch(500)` at `worker.py:1936`. Fires regardless of pipeline outcome — decouples freshness signal from indexer success. |
| B | `app/indexer/pipeline.py:811-836` | Module-internal attest inside `run_pipeline_batch`. Always-attest (PR #142 dropped the `if indexed > 0` gate). Payload: `{cycle: "batch_reindex", processed, scored, balances_updated}`. | #142 (staleness-tail bug 3d) | At the end of `run_pipeline_batch`, only on the **successful-return** path. Four early-return paths in the function (`pipeline.py:669` missing key, `pipeline.py:697` query failure, `pipeline.py:705` empty list, silent timeout) all bypass this attest — which is precisely why call site A was added in #155. |
| C | `main.py:197-208` | Main-thread caller-side attest in `run_worker_loop`. Payload: `{cycle: "batch_reindex", processed, scored}`. | predates #142/#155 (kept "for the main-thread path (different methodology context)" per the inline comment) | Inside `main.py:182-210` try/except, after the (intended) call to `run_pipeline_batch`. **Currently structurally dead** — see Finding F1 below. |
| D | `app/worker.py:2642-2650` | Slow-cycle heartbeat in `_emit_slow_cycle_heartbeats`. Payload: `{status, via: "run_slow_cycle_parallel", succeeded?, total_tasks?, error?}`. | #163 (Wave-5b backfill) | Once per `run_slow_cycle_parallel` invocation (called from `run_scoring_cycle` at the slow-cycle stage). Loops over `("wallets", "web_research", "psi_discoveries", "rpi_components")` and emits each unconditionally. |

The non-attesting callers of `run_pipeline_batch` for completeness:

- `app/enrichment_worker.py:583-598` — enrichment task `wallet_reindex` (priority 3, group "wallet", 900s budget, batch_size=400). No separate attest in the task wrapper; relies on path B firing inside the function.
- `app/server.py:3640-3652` — admin endpoint `POST /api/admin/reindex` runs `run_pipeline_batch` as a background task. No attest, by design (operator-triggered, not a cron tick).

## 2. Substrate evidence

### 2.1 Domain totals (`wallet*` only)

Raw output of `SELECT domain, COUNT(*), MAX(cycle_timestamp), MIN(cycle_timestamp), MAX-MIN AS span FROM state_attestations WHERE domain ILIKE '%wallet%' GROUP BY 1 ORDER BY 2 DESC;` at 2026-05-12T09:30Z:

```
domain           | rows | latest                       | earliest                     | span
-----------------+------+------------------------------+------------------------------+----------
wallets          |  993 | 2026-05-12T09:10:44.441Z     | 2026-04-09T04:48:27.004Z     | 33 days
wallet_profiles  |   51 | 2026-05-12T08:15:34.319Z     | 2026-04-12T10:02:59.330Z     | 29 days
```

No `wallet_holdings`, `wallet_risk_scores`, `wallet_edges` domain
strings — those data tables exist but are not attested separately.
`edges` (no `wallet_` prefix) is the per-chain edge-builder domain
attested by `app/indexer/edges.py:451`.

### 2.2 Daily cadence — pre/post the 2026-05-01 stall

```
day        | rows | first_of_day                 | last_of_day
-----------+------+------------------------------+------------------------------
2026-05-12 |    8 | 2026-05-12T00:13:30.787Z     | 2026-05-12T09:10:44.441Z
2026-05-11 |    4 | 2026-05-11T17:39:55.055Z     | 2026-05-11T21:59:55.525Z
2026-05-01 |   35 | 2026-05-01T00:06:38.540Z     | 2026-05-01T19:40:40.389Z
2026-04-30 |   48 | 2026-04-30T00:06:51.535Z     | 2026-04-30T23:37:40.462Z
2026-04-29 |   47 | 2026-04-29T00:08:32.948Z     | 2026-04-29T23:36:41.777Z
2026-04-28 |   44 | 2026-04-28T00:09:54.910Z     | 2026-04-28T22:37:45.952Z
2026-04-27 |   43 | 2026-04-27T01:09:34.903Z     | 2026-04-27T23:36:54.963Z
...
2026-04-19 |   43 | 2026-04-19T00:07:14.792Z     | 2026-04-19T23:37:45.846Z
```

The 2026-05-01 → 2026-05-11 gap is the wallet-reindex 900s timeout
incident (PR #155/#181). Pre-stall cadence was **~44 rows/day** —
i.e. **~1.8 attestations per hour**, with `run_scoring_cycle` cadence
being ~hourly. That's roughly **2× the cycle rate**, consistent with
paths A and B both firing on every cycle plus occasional path D
slow-cycle heartbeats. Post-stall (May 11-12) cadence is **4-8
rows/day** — most attest paths still fire but path B fires less
often (the pipeline now completes inside budget and many of its
early-return paths no longer trigger).

### 2.3 Hourly cadence — last 24h

```
hour                          | count
------------------------------+-------
2026-05-12T09:00:00.000Z      | 1
2026-05-12T07:00:00.000Z      | 2
2026-05-12T05:00:00.000Z      | 1
2026-05-12T04:00:00.000Z      | 1
2026-05-12T03:00:00.000Z      | 1
2026-05-12T02:00:00.000Z      | 1
2026-05-12T00:00:00.000Z      | 1
2026-05-11T21:00:00.000Z      | 2
2026-05-11T19:00:00.000Z      | 1
2026-05-11T17:00:00.000Z      | 1
```

Irregular cadence (not perfectly hourly) reflects worker-cycle drift
and the fact that not all paths fire on every cycle. The "double-row"
hours (07:00, 21:00) are likely path A + path D coinciding within a
slow-cycle.

### 2.4 Coherence registration

```python
# app/coherence.py:31
ALL_DOMAINS = [..., "wallets", ...]

# app/coherence.py:70
DOMAIN_FREQUENCIES = {..., "wallets": 4, ...}  # hours
```

Coherence checks staleness with `2 × DOMAIN_FREQUENCIES[domain]` =
**8 hours**. Current cadence (multiple writers, ~hourly each) is well
inside that window. Any consolidation that produces fewer than one
attest per 8 hours in a steady state would trip coherence.

## 3. Findings (lesson-10 — full-file reads)

### F1. `main.py:185` is structurally dead code.

`run_pipeline_batch` is declared `async def` at `pipeline.py:638`.
`main.py:185` calls it synchronously without `await`:

```python
reindex_result = run_pipeline_batch(batch_size=500)
```

This binds `reindex_result` to a coroutine object. The next line
(`reindex_result.get('processed', 0)`) raises `AttributeError`,
which the outer `except Exception as e: logger.warning(…)` at
`main.py:209-210` swallows. The caller-side attest at lines 197-208
**never executes**.

Production substrate (path A entity_id always NULL, no payloads
matching `main.py`'s `{cycle, processed, scored}` 2-field shape vs
pipeline.py's 4-field shape) is consistent with main.py path C being
dead. The comment at `main.py:194-196` claiming "we keep this
caller-side attestation for the main-thread path" is a stale
artifact from before `run_pipeline_batch` was async-converted.

**Implication for refactor:** path C cannot be relied on as a
freshness backstop. It is silently broken and has been since the
pipeline was converted to async. Any consolidation should either fix
the call (`asyncio.run(run_pipeline_batch(...))` consistent with the
sibling `asyncio.run(run_scoring_cycle())` at `main.py:174`) and
keep the attest, or delete the block entirely. **This is a design
question, not a mechanical answer.**

### F2. Pipeline does not have a natural `skipped_fresh / ran / error` shape.

The reference pattern (#198, #193, #179) has a single substrate-age
gate at the top of the wrapper:

```python
if table_age < FRESHNESS_THRESHOLD:
    attest("skipped_fresh"); return
try: work(); return "ran"
except: attest("error"); return
```

`run_pipeline_batch` has **five exit paths**, not three:

| line | exit condition | proposed status mapping |
|---|---|---|
| `pipeline.py:669` | `ETHERSCAN_API_KEY` missing | `error` (config error, not freshness) |
| `pipeline.py:697` | stale-wallets query raises | `error` |
| `pipeline.py:705` | `wallet_list = []` ("All wallets fresh — nothing to reindex") | `skipped_fresh`? or `ran_no_work`? |
| silent timeout inside `_scan_and_score` | `wait_for` exceeded | `error` (covered by path A's `failed` branch today) |
| `pipeline.py:837` happy return | `indexed >= 0` | `ran` |

The "wallets fresh" branch (line 705) is the closest analog to
`skipped_fresh`, but its meaning is **fundamentally different** from
the reference pattern's: it does not check a single substrate
timestamp, it checks the result of an EVM-only stale-wallets query
that filters out Solana addresses and uses a 24h cutoff. There is no
single `MAX(timestamp)` query that captures it.

### F3. Path A's three-status payload is a more accurate model than the reference wrapper.

`worker.py:1959-1980` already does what a clean v9.12 wrapper would
do, with a more precise three-status outcome that wraps both the
success and the exception path:

```python
status: "ok" if reindex_result is not None and no error in result
       "early_return" if reindex_result.get("error")
       "failed" if the call raised
```

This is structurally closer to the goal-state of a module-canonical
wrapper than the actual reference precedents. The cleanest
refactor would **move path A's logic into pipeline.py** (creating a
`run_wallets_scheduled` that wraps `run_pipeline_batch` and replaces
its inner attest), but doing so collides with the existing inner
attest at path B.

### F4. Path D (slow-cycle heartbeat) is a load-bearing backstop, not redundant noise.

`_emit_slow_cycle_heartbeats` at `worker.py:2602-2652` writes
`wallets` even when `run_slow_cycle_parallel`'s enrichment pipeline
errored out before reaching the `wallet_reindex` task. The brief
says "Leave worker.py:2787 heartbeat (that's Phase 2.3, separate
concern)" — confirming this is intentionally out of scope. But
**the heartbeat at 2644-2646 IS the wallets heartbeat** (the brief's
"2787" reference is from the migration-plan doc and reflects the
dex_pool_ohlcv heartbeat line, not the wallets line). The wallets
heartbeat is **inside** `_emit_slow_cycle_heartbeats` rather than
inline at 2787. Confirm with operator: is the brief's "leave 2787
in place" instruction meant to cover the entire
`_emit_slow_cycle_heartbeats` helper, or only its OHLCV branch?

## 4. Design questions

### Q1. Does v9.12 actually apply here?

**Context.** The reference v9.12 pattern (#198, #193, #179) is
"hoist worker inline into module wrapper, delete worker inline."
For wallets, the module already has both the work function and the
attest — the issue is **not** "inline in worker, module silent."
The issue is **four attest paths exist** and the v9.12 pattern
doesn't naturally map to consolidation across them.

**Options.**

- **A.** Consolidate all four into one `run_wallets_scheduled` in
  `pipeline.py` that owns the five-branch status payload. Delete
  paths A, C, D's wallets row. This is the largest, riskiest
  change — it removes the structural redundancy that was deliberately
  added in #155 and #163 as freshness backstops.
- **B.** (recommended default) Skip v9.12 for wallets. The current
  state is already "module attests + worker attests + heartbeat" —
  the v9.12 invariant ("module is the only writer") cannot be
  satisfied without losing the failure-mode coverage that PRs #155
  and #163 explicitly added. Fix the dead main.py path C, document
  paths A+B+D as intentional layered backstops, and remove `wallets`
  from the v9.12 migration table.
- **C.** Phase the work: (i) fix F1 (main.py dead code) as a tiny
  standalone PR; (ii) re-evaluate v9.12 applicability after Phase
  2.3 collapses the slow-cycle heartbeat dispatcher.

**Recommended:** **B** with a follow-up to **C(i)**. The v9.12 pattern
was designed for the data_layer single-table-writer case (peg,
exchange, ohlcv). The wallets domain is a multi-stage pipeline with
deliberate freshness redundancy added under fire (#155 was triaged in
direct response to the May-1 stall). Forcing it into the v9.12 shape
removes the layered defense.

### Q2. If consolidating (option A above), which payload schema wins?

The three live payloads differ:

```python
# Path A (worker.py:1959-1977)
{cycle: "batch_reindex_worker", status, processed, scored,
 balances_updated, errors, remaining, [error_message]}

# Path B (pipeline.py:816-821)
{cycle: "batch_reindex", processed, scored, balances_updated}

# Path D (worker.py:2623-2628)
{status, via: "run_slow_cycle_parallel", succeeded, total_tasks, [error]}
```

**Options.**

- **A.** Path A schema (richest; carries `status` distinguishing
  ok/early_return/failed; carries `remaining` for backlog visibility).
- **B.** Path B schema (slimmest, current canonical inner attest;
  loses `status` — consumers must infer from absence).
- **C.** New unified schema combining A + a `source` field
  distinguishing scheduler-origin (`worker_scheduler`,
  `main_thread`, `enrichment_task`, `slow_cycle_heartbeat`,
  `admin_endpoint`).

**Recommended:** **C** if consolidating, **A** if keeping path A
as canonical. The `remaining` field is the most operationally useful
signal in the payload (it indicates whether the indexer is draining
or losing ground) and only path A captures it.

### Q3. What does `skipped_fresh` mean for a pipeline that "drains" rather than "snapshots"?

The reference wrappers operate on a single table whose freshness is
binary (peg_snapshots_5m is fresh if `MAX(timestamp) > now - 50min`).
The wallet indexer drains a queue of 848K wallets at ~400/cycle —
there's no moment where the queue is "fresh enough to skip work."
The closest analog is the existing `pipeline.py:705` branch ("all
wallets fresh, nothing to reindex"), which fires only when every
EVM wallet's `last_indexed_at` is within 24h. Under current cadence
that condition is essentially unreachable (848K wallets ÷ 400/cycle
× 1h cadence ≈ 88 days to refresh once).

**Options.**

- **A.** Adopt `wallet_list = []` as `skipped_fresh`. Will fire ~never
  in production; the status is semantically correct but operationally
  meaningless.
- **B.** Redefine `skipped_fresh` for this domain as
  "no_stale_wallets_in_batch_window" (i.e. the SELECT at
  `pipeline.py:684-691` returned 0 rows) — same thing but named for
  what's actually checked.
- **C.** Drop `skipped_fresh` from the wrapper for this domain. Use
  only `ran` / `error`. Document in code that the wallets pipeline
  has no freshness-skip branch.

**Recommended:** **C**. The wallets pipeline's drain semantics don't
map to substrate-age gating. Forcing a `skipped_fresh` branch
introduces a status value that never fires in production.

### Q4. How to reconcile path D (slow-cycle heartbeat) with the wrapper?

If a `run_wallets_scheduled` becomes the single canonical writer,
path D inside `_emit_slow_cycle_heartbeats` is either (a) redundant
(the wrapper attested earlier in the cycle), or (b) the only
attestation when the enrichment pipeline fails before reaching the
wallet_reindex task. Today it covers case (b).

**Options.**

- **A.** Delete `wallets` from `_emit_slow_cycle_heartbeats`'s loop
  (line 2644). Trust the wrapper to attest on every cycle including
  failure. **Risk:** if `run_pipeline_batch` itself hangs past its
  wait_for budget, no attest fires this cycle. PR #155 explicitly
  added the worker-side attest to address this scenario.
- **B.** Keep the heartbeat as a third-tier backstop. Document it
  as "fires only when path A failed to fire" — but in practice it
  fires on every slow cycle today.
- **C.** Move the heartbeat into the wrapper's `error` branch with
  an upstream-error payload, so the wrapper is the single writer
  even when the upstream pipeline collapses.

**Recommended:** **C**, contingent on Q1 being answered "consolidate."
If Q1 is answered "skip v9.12 for wallets," then path D stays as-is.

### Q5. Should `main.py` path C be fixed or deleted?

Per F1 above, `main.py:185` is a coroutine-never-awaited bug. The
attest at lines 197-208 never runs.

**Options.**

- **A.** Fix the call: `reindex_result = asyncio.run(run_pipeline_batch(batch_size=500))`.
  Caller-side attest then fires as designed. **Doubles the
  attestation cadence** (path A + path C both write per cycle).
- **B.** Delete the entire block at `main.py:180-210`. The
  `run_scoring_cycle` call at line 174 already triggers path A via
  `worker.py:1936`. The main-thread reindex was redundant even
  before it broke.
- **C.** Leave the dead code in place. Document the dead-ness in a
  comment. Not recommended — code rot.

**Recommended:** **B**. The block has been dead for unknown weeks/months
and there is no observable production impact. Deleting it removes
~30 lines of stale code without changing any behavior.

## 5. Halt rationale

This diagnostic surfaces all four design-doc halt conditions per the
v9.12 brief:

- **Pipeline doesn't attest the way #198 expects.** `pipeline.py:638`
  already has its own attest at line 813 (PR #142). The "hoist the
  attest into the wrapper" instruction is moot — the attest is
  already there. The instruction would instead be "delete the worker
  inline AND the main.py inline AND the slow-cycle heartbeat branch,"
  which is a different and larger refactor.
- **Domain key shape:** `wallets` is consistent across writers. **No
  collision** with `wallet_profiles`/`edges`/`wallet_holdings`/
  `wallet_risk_scores`. This one halt condition does not trip.
- **Multiple writer overlap:** four writers (A, B, C, D) on the same
  domain, each added in a different wave under a different failure
  scenario. Consolidating one without explicit per-path sign-off
  creates the kind of inconsistency the v9.13 coupled-write doc
  warned about.
- **Consumer outside the worker:** `main.py:185-208` (broken, but
  intended) and `app/server.py:3640-3652` (admin endpoint, no attest
  but invokes the same function). Coherence (`app/coherence.py:31,70`)
  is a passive consumer expecting `wallets` rows within 8h.

Per the brief: HALT before any code commit.

## 6. References

- PR #142 — `fix(wallets): always attest batch reindex, even with processed=0` — added the inner attest at `pipeline.py:811-836`.
- PR #155 — `fix(wallets): worker-side attest decoupled from run_pipeline_batch` — added path A at `worker.py:1957-1980` because path B has four bypassed early-return paths.
- PR #163 — `fix(wave5b): slow-cycle heartbeats for ohlcv/wallets/web_research/psi_discoveries` — added path D inside `_emit_slow_cycle_heartbeats`.
- PR #179 — v9.12 pilot for `dex_pool_ohlcv` (scheduled wrapper pattern).
- PR #181 — `fix(wave9a): wallet_reindex timeout — caller passed batch_size=5000` — fixed underlying 900s timeout that caused the 2026-05-01 → 2026-05-11 stall.
- PR #193 — v9.13 coupled-write for `peg_monitor`.
- PR #198 — v9.12 for `exchange_snapshots`.
- `docs/audits/2026-05-11-module-canonical-migration-plan.md` — migration table (entry for `wallets` is substantively inaccurate per §1 above; correct it as part of any follow-up).
- `docs/audits/2026-05-12-psi-discoveries-design-questions.md` — sibling design-doc precedent.
