# RPI Integration Options — Phase 2.3a Prerequisite

**Date:** 2026-05-15
**Type:** Investigation (research-only, no code change)
**Goal:** Identify where to wire `run_rpi_scoring_scheduled()` (PR #245)
into a live scheduler so Phase 2.3a can remove `rpi_components` from the
slow-cycle heartbeat without leaving the domain unwritten.
**Status:** Drives the Phase 2.3a integration PR in a follow-up session.
The simple "call-site flip" the brief anticipated **does not exist in
current substrate** — see §Recommended option and §Halt notes.

---

## TL;DR

The wrapper `run_rpi_scoring_scheduled()` is deployed and correct, with
zero callsites (confirmed). The brief's plan — flip the enrichment task
`_run_rpi`'s inline attest to call the wrapper — is **blocked**: the
`rpi_scoring` enrichment task chronically exceeds its 900s budget and is
cancelled *before reaching its attest tail*. `_run_rpi`'s attest has
produced **zero** rows in 7 days. Wiring the wrapper into that same tail
position inherits the same dead code path.

`rpi_components` is currently kept fresh **only** by the slow-cycle
heartbeat. Removing the heartbeat today, with no working module-canonical
writer, sends the domain dark within ~96h (coherence warning threshold).

Phase 2.3a cannot be a standalone flip. It must be sequenced behind a
timeout fix. Details below.

---

## Substrate snapshot

Queried `small-scene-57890564` (BasisProtocol, prod) on 2026-05-15
~10:00Z. All read-only.

### (a) `rpi_components` attestation writers, 7d

| writer_id | rows_7d | rows_24h | avg_rc | latest |
|---|---|---|---|---|
| `null` | 27 | 0 | 1.89 | 2026-05-13 23:04:34Z |
| `heartbeat.slow_cycle` | 14 | 11 | 1.00 | 2026-05-15 08:44:42Z |

- **No `module.rpi_scorer` rows** — confirms the #245 wrapper has zero
  callsites.
- **No `enrichment.rpi_scoring` rows** — the enrichment task `_run_rpi`'s
  inline attest is not firing.
- The `null`-writer rows predate the #241 `writer_id` labeling rollout
  (their latest is 2026-05-13 23:04Z, before labeling). `avg_rc 1.89`
  means that pre-labeling history mixed heartbeat-shape rows
  (`record_count=1`) and enrichment-shape rows (`record_count=N`
  protocols) — i.e. the enrichment attest *did* fire intermittently
  before ~05-13, but has fired **0 times in the labeled era**.
- In the last 24h, **only the heartbeat writes `rpi_components`** (11
  rows).

### (b) `enrichment.rpi*` writers, 7d

Empty result set. The enrichment task path attests `rpi_components`
zero times in 7 days.

### (c) `rpi_scores` source-of-truth table

| total_rows | latest | staleness | fresh_24h | fresh_7d |
|---|---|---|---|---|
| 247 | 2026-05-15 06:03:50Z | ~3h57m | 13 | 65 |

RPI scoring **is** happening — `rpi_scores` is fresh (<4h), with one
full protocol sweep (13 rows, matching `RPI_TARGET_PROTOCOLS`) in the
last 24h. The `rpi_scores >7d static` halt condition is **NOT**
triggered.

### (d) `cycle_errors` for RPI phases, 7d

| cycle_phase | pattern | occurrences |
|---|---|---|
| `rpi_forum_scraper` | `column "mentioned_vendors" is of type jsonb but expression is of type text[]` | 572 |
| `rpi_scorer` | `column "vendor_mentions" does not exist` | 39 |
| `rpi_incident_detector` | `HTTPSConnectionPool(host='api.llama.fi'...) Read timed out` | 26 |
| `enrichment_rpi_scoring` | `task rpi_scoring exceeded 900s budget` | 9 |
| `rpi_revenue_collector` | `HTTPSConnectionPool(host='api.llama.fi'...) Read timed out` | 1 |

Two schema-drift bugs (`rpi_forum_scraper` 572×, `rpi_scorer` 39×),
external-API timeouts (`rpi_incident_detector` 26×), and **9 enrichment
task timeouts** in 7d.

---

## Wrapper location and signature

`app/rpi/scorer.py:696`

```python
async def run_rpi_scoring_scheduled(cycle_ts: datetime | None = None) -> dict
```

- **Freshness gate:** `_RPI_FRESHNESS_MINUTES = 1380` (23h), keyed on
  `SELECT MAX(computed_at) FROM rpi_scores` (`scorer.py:693`, `:730`).
- **writer_id:** `"module.rpi_scorer"` (hard-coded in all attest calls).
- **Attest branches** — all write `state_attestations.domain='rpi_components'`:
  - `skipped_fresh`: `rpi_scores` <23h old → `[{status, table_age_minutes}]`,
    `record_count=1`.
  - `ran`: gate open → calls `run_rpi_scoring`, attests one
    `{slug, score}` record per protocol (`record_count=N`); if zero
    protocols scored, attests `[{status:"ran", protocols_scored:0}]`.
  - `error`: exception in `run_rpi_scoring` → `[{status:"error", ...}]`.
- `cycle_ts` is accepted but unused (`_ = cycle_ts`, `:725`) — reserved
  for caller-symmetry with `run_psi_discovery_monitor_scheduled` (#230).
- Return: status dict on every branch (`skipped_fresh` / `ran` / `error`).

The wrapper wraps **only** `run_rpi_scoring` (the scoring step). It does
**not** perform upstream collection (governance proposals, forum scrape,
docs scoring, incident detection). It assumes those tables are already
populated.

**A3 signature note vs. reality:** A3 placed the wrapper "immediately
above `run_rpi_scoring` at line 651." Actual: wrapper at `:696`,
`run_rpi_scoring` at `:826`, separated by a section-header comment block.
The wrapper *is* above `run_rpi_scoring`; "line 651" is the start of the
wrapper's doc-comment block. Gate value (1380 min / 23h) matches A3's
description exactly. No material discrepancy.

### `run_rpi_scoring` (`scorer.py:826`)

Sync function. Pre-fetches revenues (`get_all_revenues`), syncs
auto-derived lens components, loops `RPI_TARGET_PROTOCOLS` calling
`collect_raw_values` → `score_rpi_base` → `store_rpi_score` (writes
`rpi_scores`). Returns `list[dict]`. No attestation. Side effect: one
`rpi_scores` row per protocol via `store_rpi_score`.

---

## Existing RPI flow in production

### Enrichment task `_run_rpi` (`app/enrichment_worker.py:392`)

Registered as task `rpi_scoring`: `timeout_seconds=900`, `group="rpi"`,
`priority=2`, `gate_check=_db_gate("SELECT MAX(computed_at) FROM
rpi_scores", min_hours=24)`.

Body (in order): `collect_snapshot_proposals` → `collect_tally_proposals`
→ `collect_parameter_changes` → `scrape_all_forums` +
`update_vendor_diversity_lens` → `score_all_docs` →
`run_incident_detection` → `run_rpi_scoring` → **inline
`attest_state("rpi_components", ..., "enrichment.rpi_scoring")`**.

### Why the enrichment attest is dead — the core finding

The pipeline runs each task as `asyncio.wait_for(task.func(...),
timeout=900)` (`enrichment_worker.py:90-92`). On timeout, `wait_for`
raises `TimeoutError` and cancels the coroutine **at its next `await`
point**.

`_run_rpi`'s heavy step is `await asyncio.to_thread(run_rpi_scoring)`.
When the 900s budget elapses during that call:

1. The coroutine is cancelled at that `await` — **the `attest_state`
   line after it never executes.**
2. But `asyncio.to_thread` runs `run_rpi_scoring` on a real OS thread,
   which **cannot be force-cancelled** — it continues to completion in
   the background, finishing every `store_rpi_score` write.

Result, exactly as substrate shows:
- `rpi_scores` stays fresh — the background thread completes the 13-row
  sweep (substrate c: 13 fresh_24h).
- `rpi_components` enrichment attest never fires — the coroutine died
  before reaching it (substrate b: empty; substrate d: 9× "exceeded
  900s budget").

The 900s budget is consumed upstream of scoring by the broken collectors
— `rpi_forum_scraper` alone logged **572 schema-drift errors in 7d**
(each error is a failed-then-retried path), plus `rpi_incident_detector`
llama.fi timeouts (26×).

`_run_rpi`'s 24h task gate also means it only runs ~once/day (substrate
c: one 13-protocol sweep per 24h) — when `rpi_scores` crosses 24h. So
even a *working* `_run_rpi` attest would produce ~1 row/24h.

### Heartbeat `_emit_slow_cycle_heartbeats` (`app/worker.py:2617`)

Fires once per slow cycle from `run_slow_cycle_parallel` (`:2706`, after
`run_enrichment_pipeline()` returns). Domain tuple at `:2669`:
`("wallets", "web_research", "rpi_components")`. Writes
`base_payload = {"status": <ok|pipeline_failed|ran_no_result>, "via":
"run_slow_cycle_parallel"}` with `writer_id="heartbeat.slow_cycle"`,
`record_count=1`.

**Precedent:** `psi_discoveries` was *already* removed from this tuple
and re-routed through its module wrapper `run_psi_discovery_monitor_
scheduled` (called at `:2696`, #230/#223). That is the exact migration
shape Phase 2.3a wants for `rpi_components` — *if* a working wrapper
callsite exists.

### `run_slow_cycle` RPI block (`app/worker.py:1705`)

`run_slow_cycle` has its own inline RPI block (24h gate, full collection
+ `run_rpi_scoring` at `:1765`, **no attest**). `run_slow_cycle` is
dead-in-steady-state (only reached as the exception fallback inside
`run_slow_cycle_parallel`; see the separate dispatcher-collapse
investigation). Not a viable host.

### Budget management

`grep -rn rpi app/budget/` → **none**. RPI scoring is not under
`app/budget/` daily-cycle management. Its only resource control is the
enrichment task's `timeout_seconds=900`. (`run_fast_cycle`'s sibling
wrappers — `run_psi_scoring_scheduled` `:1080`, `run_peg_monitoring_
scheduled` `:1259`, `run_exchange_collection_scheduled` `:1195` — are
called bare `await`, unbudgeted, in `run_fast_cycle`.)

### Coherence freshness requirement — the cadence bound

`app/coherence.py`: `DOMAIN_FREQUENCIES["rpi_components"] = 48` (hours).
`_check_freshness` (`:141`) flags a domain only when
`age_hours > expected_hours * 2`, i.e. **`rpi_components` warns at 96h**
stale, alerts at 192h.

This is the decisive cadence number: **any writer attesting
`rpi_components` at least once per 96h satisfies coherence.** A daily
(24h) writer clears it with 4× margin. The per-slow-cycle heartbeat
cadence is far more than coherence needs.

---

## Architectural options

### Option A — wire wrapper into the worker.py slow-cycle heartbeat

Replace `rpi_components` in the `_emit_slow_cycle_heartbeats` domain
tuple with a wrapper call (mirroring how `psi_discoveries` is handled at
`worker.py:2696`).

- **Viable** — the heartbeat runs in `run_slow_cycle_parallel` *after*
  `run_enrichment_pipeline()` returns; it is **not** inside the 900s
  enrichment budget. A wrapper call here executes.
- **Pro:** minimum surface; mirrors the `psi_discoveries` precedent;
  natural heartbeat→wrapper swap point; per-cycle `skipped_fresh`
  cadence.
- **Con — gate mismatch:** the wrapper's gate is 23h; `_run_rpi`'s task
  gate is 24h. In the 1h window where `rpi_scores` age is 23–24h, the
  heartbeat wrapper call hits the **`ran`** branch and scores —
  *without* the upstream collection (which only `_run_rpi` does) — then
  writes `rpi_scores`, which **closes `_run_rpi`'s 24h gate**. The
  collection task is then starved for another day. (Today the
  collection is already 572-errors degraded, but starving it entirely
  is a behavior change.)
- **Con:** `_run_rpi`'s own (dead) inline attest still exists →
  `rpi_components` would have two *intended* writers until that is also
  removed.
- **Fit:** acceptable as an interim if Phase 2.3a must ship before the
  timeout fix, accepting the gate-mismatch caveat.

### Option B — wire wrapper into `_run_rpi`'s tail (the brief's plan)

Replace `_run_rpi`'s final `run_rpi_scoring` + inline attest with
`run_rpi_scoring_scheduled()`. Keep the upstream collection in `_run_rpi`.

- **Blocked by substrate.** `_run_rpi`'s tail is unreachable: the task
  is cancelled at the `run_rpi_scoring` `await` after 900s, before the
  tail. A wrapper call placed at that tail inherits the identical dead
  path — substrate b would stay empty, just relabeled.
- **Pro (once unblocked):** the v9.12-idiomatic end state — single
  canonical writer (`module.rpi_scorer`), collection stays gated with
  scoring as a unit, governance inputs fresh before scoring. Double gate
  (task 24h + wrapper 23h) is redundant but harmless: when `_run_rpi`
  runs, `rpi_scores` is >24h so the wrapper's 23h gate is always open
  (the wrapper's `skipped_fresh` branch is simply dead via this path).
- **Cadence:** ~1 row/24h. Against the 96h coherence bound, 4× margin —
  sufficient on its own.
- **Fit:** correct long-term target, but **only after the 900s timeout
  is fixed**.

### Option C — both (heartbeat + `_run_rpi` tail)

- Inherits Option B's blocker (the `_run_rpi`-tail half is dead) **and**
  Option A's gate-mismatch. Strictly worse than picking A or B. Not
  recommended in any sequencing.

### Option D — operator-decision-required (this investigation lands here)

The pre-flight + substrate reveal an architectural complication the
brief's premise did not account for: **the wrapper's intended host
(`_run_rpi`'s tail) is dead code in production due to a chronic 900s
timeout, and `_run_rpi`'s existing attest has already been silent for
the entire 7-day window.** Phase 2.3a is therefore not a self-contained
"call-site flip" — it is entangled with a timeout bug and two
schema-drift bugs in the RPI collectors. The sequencing decision
(fix-first vs. interim-Option-A) is an operator call. Surfaced, not
unilaterally decided — see §Halt notes.

---

## Recommended option

**Do not ship Phase 2.3a as a standalone call-site flip.** Sequence it:

**PR-1 — fix the `rpi_scoring` 900s timeout (bug fix, prerequisite).**
Root cause is upstream-of-scoring budget burn, chiefly
`rpi_forum_scraper` (572 schema-drift errors/7d: `mentioned_vendors`
jsonb-vs-text[] mismatch) and `rpi_scorer`'s `vendor_mentions`
missing-column error (39×). Options for the fixer: repair the
schema-drift INSERTs; and/or split the upstream collection into its own
enrichment task separate from scoring; and/or raise `timeout_seconds`.
Investigation-scoped recommendation: **fix the schema drift first** —
572 failed-and-retried forum-scraper paths are the most likely single
budget sink, and the drift is a real bug regardless of Phase 2.3a.

**PR-2 — Option B call-site flip.** Once `_run_rpi` completes within
budget, replace its `run_rpi_scoring` + inline attest with
`run_rpi_scoring_scheduled()`. `module.rpi_scorer` becomes the single
canonical writer at ~24h cadence (4× inside the 96h coherence bound).

**PR-3 — remove `rpi_components` from the heartbeat tuple**
(`worker.py:2669`). Safe only after PR-2 substrate-verifies
(`module.rpi_scorer` rows appearing in `state_attestations`). PR-2 and
PR-3 may be combined into one PR if the operator wants the flip and the
heartbeat removal atomic.

**Substrate-cited reasoning:** Option B is the v9.12 single-writer end
state and is blocked *only* by the timeout — a bug worth fixing on its
own merits (substrate d: 9 timeouts, 572+39 schema errors). Option A
would let Phase 2.3a ship sooner but trades the timeout bug for a
gate-mismatch behavior change (23h wrapper vs 24h task → collection
starvation) — swapping one defect for another. Because coherence only
needs 96h cadence and `_run_rpi` already delivers ~24h once unblocked,
there is **no cadence pressure** forcing the faster-but-dirtier Option A.
The heartbeat is not at risk of imminent removal; it can stay until PR-2
lands.

If — and only if — an operator constraint requires decoupling Phase
2.3a from the timeout fix, Option A is the fallback, with the
gate-mismatch caveat accepted and ideally the wrapper gate raised from
1380 to ≥1440 min so it never pre-empts `_run_rpi`'s 24h gate.

---

## Phase 2.3a PR shape

Not one PR — **three, sequenced** (PR-2 + PR-3 optionally merged):

| PR | Scope | Files | Est. diff | Gate to next |
|---|---|---|---|---|
| PR-1 | Fix `rpi_scoring` 900s timeout (schema drift in forum scraper / scorer; optionally split collection task) | `app/rpi/forum_scraper.py`, `app/rpi/scorer.py` lens query, possibly `app/enrichment_worker.py`; likely a migration | medium — schema-dependent | 24–48h soak: substrate d `enrichment_rpi_scoring` timeouts → 0; substrate b `enrichment.rpi_scoring` rows appear |
| PR-2 | Option B flip — `_run_rpi` tail → `run_rpi_scoring_scheduled()`, drop inline attest | `app/enrichment_worker.py` (~15 lines) | small | 24–48h soak: substrate a shows `module.rpi_scorer` rows; `enrichment.rpi_scoring` rows stop |
| PR-3 | Remove `rpi_components` from `_emit_slow_cycle_heartbeats` tuple | `app/worker.py:2669` (1 line) | trivial | 96h soak: coherence freshness check stays green for `rpi_components` |

PR-2 + PR-3 are combinable (the flip + heartbeat removal as one atomic
change) once PR-1 has soaked. PR-1 must stand alone and soak first — it
is the actual blocker.

Not in scope for any of these: `_run_rpi_expansion` (weekly task,
`enrichment_worker.py:466`, separate domain) and the `rpi_incident_
detector` llama.fi timeouts (external-API flakiness, not a budget bug).

---

## Halt notes

Operator decisions required before dispatching the Phase 2.3a work:

1. **The brief's premise is invalidated.** Phase 2.3a was scoped as a
   call-site flip swapping `_run_rpi`'s working attest for the wrapper.
   Substrate shows `_run_rpi`'s attest has been **dead for 7+ days**
   (timeout cancels the coroutine before the tail). The flip cannot
   proceed without first fixing the timeout. Confirm the
   PR-1 → PR-2 → PR-3 sequencing, or direct otherwise.

2. **Interim Option A vs. sequenced Option B.** Recommendation is B
   (after PR-1). Option A could ship Phase 2.3a sooner but introduces a
   23h/24h gate mismatch that starves `_run_rpi`'s collection. Since
   coherence only needs 96h cadence and the heartbeat is under no
   pressure to be removed, the recommendation is to **not** take the
   Option A shortcut. Operator confirms.

3. **Wrapper gate value.** `_RPI_FRESHNESS_MINUTES = 1380` (23h) is 1h
   *below* `_run_rpi`'s 24h task gate. Harmless under Option B (the
   wrapper is called from inside `_run_rpi`, where `rpi_scores` is
   always >24h, so `skipped_fresh` is dead). But if Option A is ever
   chosen, the 23h value must be raised to ≥1440 min so a heartbeat
   wrapper call cannot pre-empt `_run_rpi`. Flagging per the brief's
   gate-mismatch halt condition.

4. **Phase numbering collision.** This brief calls the call-site flip
   "Phase 2.3a." The parallel dispatcher-collapse track also uses "Phase
   2.3a" for the `run_slow_cycle` deletion, with "2.3b" covering the
   `rpi_components` wrapper + heartbeat-helper removal. The RPI work
   described here corresponds to that track's **2.3b**. Recommend the
   operator reconcile the numbering before dispatch to avoid two PRs
   both labelled "2.3a."

5. **Out-of-scope bugs surfaced.** `rpi_forum_scraper` schema drift
   (572×) and `rpi_scorer` `vendor_mentions` missing column (39×) are
   pre-existing bugs independent of Phase 2.3a. PR-1 must address at
   least the forum-scraper drift (primary budget sink). The `rpi_scorer`
   error and the `rpi_incident_detector` llama.fi timeouts may warrant
   their own tickets.

**Halt conditions NOT triggered:** `rpi_scores` is fresh (~4h, not >7d
static — RPI scoring works); the wrapper signature matches A3's
description; the `_run_rpi` task exists as A3 implied. The triggered
condition is the timeout making the wrapper's intended host unreachable
(→ Option D writeup above), plus the 1380-min gate mismatch (note 3).

---

## References

- PR #245 — `run_rpi_scoring_scheduled` wrapper (deployed 2026-05-14
  02:10Z, zero callsites confirmed)
- PR #241 — `writer_id` labels on wrapper + heartbeat callsites
- `app/rpi/scorer.py:696` — wrapper; `:826` — `run_rpi_scoring`
- `app/enrichment_worker.py:392` — `_run_rpi`; `:90` — task timeout wrap
- `app/worker.py:2617` — `_emit_slow_cycle_heartbeats`; `:2669` — domain
  tuple; `:1697` — dead `run_slow_cycle`
- `app/coherence.py:90` — `rpi_components` 48h frequency; `:141` — 2×
  warn threshold
- #198 (`exchange_collector`), #193 (`peg_monitor`), #240
  (`psi_collector`), #230 (`psi_discovery_monitor`) — module-canonical
  wrapper precedents
- `docs/audits/2026-05-11-module-canonical-migration-plan.md` — Phase
  2.3 staging requirement
