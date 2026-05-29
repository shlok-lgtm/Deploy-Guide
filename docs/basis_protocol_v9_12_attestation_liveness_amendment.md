# Basis Protocol — V9.12 Constitution Amendment

**Attestation Liveness Semantics — Output Attestation, Not Loop Attestation**

**Version:** 9.12 | **Date:** May 27, 2026
**Applies to:** V8 Constitution (base) + V8.5, V9.1, V9.2, V9.4, V9.5, V9.6, V9.8, V9.9, V9.9.5, V9.9.6, V9.9.7, V9.9.8, V9.9.9, V9.11
**Extends:** V8 Constitution — Universal State Attestation primitive (`attest_state`, 13 domains) and Part 2 (The Attestation Chain)
**References:** Health-Instrumentation Audit 2026-05-27 (branch `claude/basis-health-instrumentation-aGpGo`, commit `4ad9108`); April 12 silent-failure cluster; Wave A/B silent-zero collector remediation; the resolved `psi_discoveries`/`rpi_components` attestation-relocation fixes
**Purpose:** Close a class of defect in which an attestation reports a domain healthy because its *cycle ran*, while the *work the cycle was supposed to produce* is dead. Refine what a fresh attestation, and the `record_count` field, are permitted to mean. Bind the rule to every domain, not only those already caught.

---

## Preamble

The V8 constitution establishes Universal State Attestation as a foundational primitive: every domain of novel state is hashed at capture via `attest_state(domain, records, entity_id)`, and Part 2 asserts that "Every step from raw data to delivered report is attested. No gaps."

The 2026-05-27 health-instrumentation audit found that the *no gaps* claim was true in form and false in substance. The attestation chain had no missing links — but a link could attest itself fresh while producing nothing. The defect is not a missing attestation; it is an attestation that means less than the protocol assumed it meant.

A single instance of this flaw silently hid at least three independent failures:

1. **`mempool_observations` capture** — dead 19 days (last real capture 2026-05-08, cause: Alchemy API limit). The domain nonetheless showed ~985 attestations over 7 days and ~570,000 attestation rows total, because the capture loop kept emitting `{status: no_rows_yet, captured: 0}` placeholder heartbeats every ~10 minutes, all carrying the same repeating `batch_hash`. The heartbeat attested that the loop executed. It did not attest that anything was captured.

2. **`parent_company_financials`** — collector healthy (registry filled, table populated), heartbeat permanently silent because attestation was gated on `if quarters_stored > 0`, and `ON CONFLICT DO NOTHING` holds that count at zero after the initial fill.

3. **`wallet_holder_discovery`** — collector healthy (170,778 rows in 7 days), heartbeat silent because attestation was gated on `if total_new > 0` (wallets *promoted* to the graph), which stops once the graph saturates while writes continue.

The same audit also showed the inverse failure: the `discovery_signals` domain (#12) attested fresh every ~30 minutes via a system-wide heartbeat while its `entity_discovery` output stream had not produced for days — the aggregate heartbeat masked a per-stream silence. (In that specific case the silence was a healthy 168h weekly cadence, not a stall — but the instrument could not have told the difference, which is the point.)

This is the same lineage as the April 12 cluster and the Wave A/B silent-zero collectors. It is the most consequential member of that lineage because it is not a bug in one collector — it is a flaw in what attestation is permitted to mean, and therefore latent in every collector that has not been audited against it.

---

## The defect, stated precisely

An attestation is **loop-attesting** when its freshness and its `record_count` reflect that the producing cycle executed, regardless of whether the cycle produced the output it exists to produce.

An attestation is **output-attesting** when its freshness reflects the recency of actual output in the domain's output table, and its `record_count` reflects the true state of that output — not a delta, not a placeholder, not a loop tick.

The constitution's attestation primitive did not distinguish these. Three pathologies follow from the conflation:

- **Placeholder heartbeat** — the loop emits an attestation on every tick even when it captured nothing (the `no_rows_yet` early-return pattern). Liveness is reported for a dead stream.
- **Delta-gated attestation** — the attestation only fires when *new* output is produced (`new/stored/promoted/inserted > 0`). A healthy steady-state or saturated collector goes silent and reads as dead.
- **Aggregate masking** — a single system-wide heartbeat covers a domain that carries multiple output streams (multiple `signal_type`s or writers under one domain). One stream dying is invisible because the others keep the aggregate fresh.

---

## Amendment

### 1. Attestation liveness is defined by output, not by loop execution.

The Universal State Attestation primitive is refined: a fresh attestation for a domain MUST mean that the domain's output is current as measured against that domain's declared cadence — not that the producing process executed. An attestation that can fire while the domain's output table has not advanced past its cadence threshold is non-conformant and MUST be corrected.

### 2. `record_count` reflects true output state, never a delta or a placeholder.

The `record_count` field in `state_attestations` MUST reflect the actual state of the domain's output (e.g., rows present, latest output recency), not the count of *newly added* records and not a fixed placeholder value. Delta-gated attestation (`if new > 0: attest`) and placeholder attestation (`attest({captured: 0})` on an empty cycle) are both prohibited. Attestation fires on cycle completion with a `record_count` that describes output reality; an empty cycle attests the true (possibly unchanged) state, it does not skip and it does not emit a zero placeholder that reads as liveness.

### 3. Multi-stream domains require per-stream liveness.

Any attestation domain whose single cycle covers more than one output stream (more than one `signal_type`, writer, or output table) MUST be monitored per stream against each stream's own declared cadence. A fresh aggregate domain heartbeat is NOT sufficient to declare the domain healthy if any covered stream has exceeded its cadence threshold. `discovery_signals` (domain #12) is the canonical multi-stream domain and the reference case; the same rule binds any future multi-stream domain.

### 4. Every monitored stream declares its cadence.

Each output stream subject to liveness monitoring MUST declare an expected cadence (the interval within which fresh output is expected). Health is evaluated against that declared cadence with margin: WARNING at greater than 1× cadence, ALERT at greater than 2×. A stream with a 168h weekly cadence and a stream with a 10-minute cadence are both monitorable and distinguishable only because each declares its own interval — the absence of a declared cadence is itself a non-conformance, because it forces the optimistic-default behavior this amendment exists to forbid.

### 5. The canonical remediation pattern.

The conformant implementation, established by the 2026-05-27 audit, queries the output table directly against a per-stream cadence registry rather than reading the attestation heartbeat. `writer_id` (V9.x writer-discriminator work) labels heartbeats by writer but is insufficient on its own, because output streams within a domain are not one-to-one with writers (one writer may emit several `signal_type`s). The check MUST be at the output-table/stream level. Reference implementation: `_check_output_streams()` in `app/coherence.py` and the rewritten per-stream `check_discovery_freshness()` in `app/ops/tools/health_checker.py`.

### 6. Conformance is verified, not assumed.

Conformance with this amendment is established by a regression scenario, not by inspection. The scenario `heartbeat_attests_while_output_stream_dead` (to be authored by a separate scenarios-session per Holdout Isolation) is the canonical guard: it mutates a multi-stream domain so one stream is silently skipped while another keeps firing, keeps the aggregate heartbeat fresh, and asserts that a conformant health check reports the silent stream DEGRADED within one cadence window. This scenario is to be treated as protected test content and gates the relevant surfaces per the Scenarios & Regression Harness rules.

---

## Scope and boundaries

- This amendment refines the *semantics* of the existing attestation primitive. It does not add a primitive, a domain, or an entity class. The 13 attestation domains are unchanged in number and identity; what changes is the meaning of a fresh attestation within each.
- This amendment does not itself remediate any specific collector. The audit branch (`aGpGo`) carries the instrument fix and the `parent_company_financials`/`wallet_holder_discovery` corrections; a separate antipattern sweep enumerates remaining delta-gated collectors. Those are operational work tracked in the punchlist, governed by — not constituted by — this amendment.
- The mempool capture outage (cause: Alchemy API limit) is an operational disposition (restore vs. retire), not a constitutional matter. It appears here only as the reference instance that exposed the flaw. No canon claim depends on mempool capture.
- Where this amendment and the V8 attestation primitive appear to conflict, V8 governs the *existence and structure* of attestation and this amendment governs the *liveness semantics* layered on top. They are complementary: V8 says every domain is attested; V9.12 says an attestation must mean output is live.

---

## Why this is constitutional, not operational

A bug fix belongs in the punchlist. A change to what a primitive is permitted to mean belongs in the constitution. The attestation chain is the foundation of Basis's accumulated-state moat — "13 domains of attested state hashed at capture," "all timestamped, hashed, and anchored on-chain daily." The value of that moat rests entirely on attestation meaning what it claims. An attestation that can report fresh while its domain is dead does not merely create an operational blind spot; it devalues the moat, because attested history that cannot distinguish live from dead is history a competitor's equivalent claim could also make. Output-attestation, enforced and scenario-guarded, is what makes "attested" load-bearing. That is why it is written into the constitution and not only fixed in code.

---

*Basis Protocol · shlok@basisprotocol.xyz · Constitution Amendment V9.12 · May 27, 2026*
