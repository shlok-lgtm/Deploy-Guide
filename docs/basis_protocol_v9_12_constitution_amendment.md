# Constitution Amendment v9.12 — Module-Canonical Live Path

**Date:** 2026-05-11
**Status:** Proposed (resolves the deferred question in v9.11)
**Supersedes:** Resolves the "Open question (deferred)" section of v9.11; the answer is module-canonical.

## Forcing function

v9.11 codified that worker.py inline implementations are the canonical live path and modules in app/data_layer/ + app/collectors/ are enrichment-pipeline utilities. The amendment explicitly deferred whether to refactor away the duplication.

Subsequent waves (5a, 5b, 6, 7) on 2026-05-11 surfaced the same pattern at successively deeper layers: module → main.py → worker.py:2468 → run_slow_cycle → run_slow_cycle_parallel. Each "fix" patched a layer, deployed, and verified dead because the next layer up was the actual live path. Seven domains hit this pattern in one day. The recursion has no natural floor — v9.11's rule "trace from worker.py outward" works once but doesn't help when worker.py itself has dispatcher/sequential duplication.

The duplication itself is the bug; placement is the symptom.

## Decision

**Modules in app/data_layer/ and app/collectors/ are canonical.** app/worker.py becomes a thin scheduler/orchestrator that imports from modules. Inline write implementations in worker.py are deleted.

Concretely, after this amendment lands and is implemented:

1. For every domain where worker.py currently has an inline INSERT or attest call: the inline implementation is replaced with a call to the canonical module function. The module function is the only place that writes to that table or attests for that domain.

2. worker.py's main loop becomes: schedule → call module function → handle result. No SQL in worker.py. No table-specific knowledge in worker.py.

3. The db_gate pattern (check MAX(updated_at), skip if fresh) either lives inside the module (if the module's API is "call me, I'll decide") or is removed entirely (if the scheduler is the only thing deciding cadence). Modules should not gate on freshness internally if the scheduler is the cadence authority. Pick one per module, not both.

4. run_slow_cycle and run_slow_cycle_parallel collapse to a single scheduler entry point. The "parallel dispatcher with sequential fallback" pattern is removed unless there's a substantive reason to keep both (and if kept, the fallback must call the same module functions as the primary).

## Rationale

Module-canonical (this amendment) vs worker-canonical (the alternative considered):

- **Test isolation.** Module functions are testable; worker.py inline blocks are not (they require the full main loop).
- **Single source of truth per operation.** No "which file is the live one" question because there's only one file.
- **Scheduler/work separation.** worker.py is a scheduler. Schedulers are simple; work is complex. Mixing them is what produced today's 7-layer recursion.
- **Modules already exist and have the right names.** The work is mostly deletion + a few imports, not greenfield.
- **Future operations have a clear home.** New collectors go in app/collectors/. New data-layer writes go in app/data_layer/. worker.py grows only with new schedule entries, not new logic.

Worker-canonical was rejected because: worker.py is already over 2500 lines, hard to navigate, mixes scheduling/work/scoring/attestation/freshness, and the inline duplication grew organically rather than by design. Choosing worker-canonical would mean codifying the accident.

## Implication: db_gates become a code smell, not a freshness mechanism

v9.11 codified that db_gates close in steady state because worker.py keeps tables fresh. Under v9.12, that's no longer true — modules are the only writers, so the gate's input is the module's own output. A gate that gates on the module's own writes is either always closed (useless) or always open (also useless).

The right pattern under v9.12 is: scheduler decides cadence; module does work unconditionally when called. If the work decides "nothing to do, return early with status=no_op_needed," that's the module's choice and the attest still fires with that status.

## Migration plan (not in scope for this amendment)

Implementation lives in a separate PR series (likely several PRs given the surface area). The order proposed:

1. Audit: list every (domain, current_canonical_location, current_live_location, module_function_name) tuple from today's Waves 1-7 evidence.
2. For each row: refactor worker.py to call the module function; delete the inline implementation; verify substrate.
3. After full sweep: delete the run_slow_cycle / run_slow_cycle_parallel duplication.

Each PR is one domain (or a tightly grouped set) to limit blast radius. Each is verified with substrate per lessons 7, 8, 9 before moving to the next.

## References

- v9.11 (the deferred-question parent amendment)
- 2026-05-11 punchlist Waves 1-7 (the recursion evidence)
- docs/audits/2026-05-11-data-layer-cadence-audit.md (existing inventory partial)
