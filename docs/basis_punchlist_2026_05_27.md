# Basis Protocol — Consolidated Punchlist

**Date:** May 27, 2026 (health-instrumentation audit — heartbeat output-vs-loop defect found and fixed; three hidden failures surfaced; SII/PSI auto-discovery Phase 0 investigation completed)
**Supersedes:** May 13 punchlist (`basis_punchlist_2026_05_13.md`)
**Anchored to:** V9.11 base + V9.12 + the May 27 instrumentation audit (branch `claude/basis-health-instrumentation-aGpGo`, commit `4ad9108`, no PR yet) + SII/PSI Phase 0 (branch `claude/adoring-franklin-HuNxw`, investigation only, awaiting sign-off)

---

## Section 0 — The headline finding

**Basis's attestation heartbeats verified that a *cycle ran*, not that the *work inside it produced output*.** This is a new canonical bug class — `heartbeat_attests_while_output_stream_dead` — and it is the most consequential one found to date because a single defect hid at least three independent real failures behind green status:

1. **entity_discovery** — looked dead (5 days silent), was actually a healthy 168h weekly cycle the instrument mislabeled as a 30-min cycle. *Latent false-alarm.*
2. **mempool capture** — DEAD 19 days (last real capture 2026-05-08), but the domain showed 985 attestations/7d because the loop kept emitting `{status: no_rows_yet, captured: 0}` placeholder heartbeats every ~10 min. *Real outage, fully masked.*
3. **wallet_holder_discovery** — collector healthy (170,778 rows/7d), heartbeat silent because it gated attestation on "new wallets promoted > 0" instead of "collector ran." *Real instrumentation blind spot.*

The instrument lied in the optimistic direction. That is the root defect; the three failures above are its symptoms. The same class previously hid the April 12 cluster and the Wave A/B silent-zero collectors — this audit is the same lineage, now named and tested.

**Fix shipped (branch, not yet merged):** `app/coherence.py` `_check_output_streams()` queries output tables directly against a per-stream cadence registry (WARNING >1× cadence, ALERT >2×); `health_checker.py` `check_discovery_freshness()` rewritten per-stream, aggregate-MAX path retired; `tests/test_discovery_streams_freshness.py` (6 tests incl. a replay of the 2026-05-21 fingerprint). **Action: open the PR; it must pass the regression matrix (`invoke-regression-matrix`) before merge.**

---

## Section A — Verdicts (substrate-cited, from the audit)

1. **entity_discovery — LIVE, healthy weekly cadence. No fix needed.** 168h gate (`enrichment_worker.py:1156-1163`); fires May 14 03:00 → May 21 04:52 (7d apart); next due ~May 28. All five Circle 7 streams (bri/cxri/lsti/tti/vsri) write in one `_store_discovered_entities()` call. The earlier "5-day stall, #265-shape" read was wrong — it was the instrument, not the cycle.

2. **mempool capture — DEAD 19 days. Known cause: Alchemy API limit.** `mempool_observations` `MAX(seen_at) = 2026-05-08`. `emit_24h_summary` returns early at the `no_rows_yet` branch (`mempool_watcher.py:844-849`), so `_attest_capture_status` never fires real data — the ~570K attestation rows are repeating placeholder heartbeats with the same `batch_hash`. **Cause is the Alchemy API limit (mempool RPC access cut off ~May 8).** Disposition needed: restore Alchemy access / upgrade plan, OR formally retire mempool capture as a capability. *Not a canon contradiction — see Section B.*

3. **parent_company_financials — LIVE collector, broken heartbeat (same shape as #2 in the bug class).** Registry has 3 active companies, table has 12 rows (filled correctly). Attestation gated on `if quarters_stored > 0` (`parent_company_financials.py:282`); `ON CONFLICT DO NOTHING` keeps it 0 after the initial fill. **Also a latent crash:** `ae`/`e` typo at line 291 (`except Exception as e` then `logger.warning(f"... {ae}")`) → `NameError` on first real failure. Both fixed by the Part B instrument change + a one-line typo fix.

4. **wallet_holder_discovery — LIVE collector, broken heartbeat.** 170,778 rows/7d. Attestation gated on `if total_new > 0` (`holder_ingestion_collector.py:401`) — counts wallets *promoted* to the graph, not rows written; stops attesting once the graph saturates while writes continue. Same gating antipattern.

5. **protocol_parameter_changes + protocol_parameter_snapshots — OFF BY DESIGN, verified.** Silence matches PR #252 exactly: `PROTOCOL_PARAMETER_REGISTRY` contains only `aave` + `compound-finance`, both in `KILLED_PROTOCOLS`; loop runs but every protocol is `continue`'d. `contract_dependencies*` unaffected (firing today). Decoder fix still tracked in #251. Nothing else in parameter surveillance died alongside.

---

## Section B — Canon-vs-reality flags (per project rules #2 and #7)

**1. Deck claim is unsupported as written — DECISION pending (architect).**
`basis_pitch_deck_v11_5_1.md:216`: *"SII and PSI are auto-expanding. New entities discovered, scored, and added to coverage automatically. No manual onboarding."*
Phase 0 (May 27, branch `claude/adoring-franklin-HuNxw`) refined the picture:
- **PSI: discovery already exists** (`psi_collector.py:1336-1675` — DeFiLlama pool scan, $10M TVL gate, enrichment, promotion via `is_category_complete`) — it's just isolated from the Circle 7 `entity_discovery` framework and `sii`/`psi` are absent from `CATEGORY_INDEX_MAP` (`entity_discovery.py:32-43`). PSI's gate is **invisible to telemetry** (`psi_promotion_attempted` event has no producer in code — the May 13 question "stalled or correctly rejecting?" has been unanswerable from telemetry the whole time).
- **SII: no issuer-feed discovery at all.** Only intake is `wallet_graph.unscored_assets`, populated when a scanned wallet already holds a token — exactly the StablR-shaped gap. No reader consumes DeFiLlama `peggedAssets` as a discovery feed.
- **The load-bearing finding for the build:** **no `published`/visibility column exists on any serving path.** Phase 0 enumerated **82 read sites** across `app/server.py`, `app/report.py`, `app/pulse_generator.py`, `app/divergence.py`, and `app/ops/entity_views.py` that serve scores purely by "row exists in scored table." `scoring_enabled` and `status` gate scoring/status, not serving. **Currently the policy "score but don't display until approved" is unenforceable** — auto-discovery without a publication gate would leak unapproved entities across all 82 endpoints on first cycle.
**Disposition (architect decision, see Section E):**
- **Build path** (recommended): publication-gate first (`stablecoins.published` + new `protocol_publication_state` table for PSI, default FALSE on new entities, backfill existing to TRUE, admin endpoint to flip), then route PSI through the registry, then add SII `peggedAssets` feed. SII is only worth building *because* the gate makes the noisy feed (~120 candidates, mostly wrapped/dead) safe.
- **Rescope path**: edit deck:216 to "SII curated, PSI auto-discovered" — leaves the unenforceable-policy problem unaddressed but is honest about today.
- The gate is the hard, load-bearing piece; discovery is the cheap follow-on. Whichever path is chosen, the 82-site serving gap is now a known structural defect regardless of whether discovery is built.

**2. Mempool capture death is NOT a canon contradiction.** Grep for `mempool|mev|front-run|pending tx` across deck, one-pager, business plan, and both constitutions returns nothing. No external claim depends on mempool capture. This lowers its disposition from "false claim" to "operational gap" — decide whether the capability is worth the Alchemy spend, but nothing in canon needs editing.

**3. CLAUDE.md migration counter is stale — IMMEDIATE fix.** Phase 0 found CLAUDE.md says next migration is 085; actual latest is 111. Any CC session that trusts CLAUDE.md will collide. Project rule #7 (numbers must match across docs and reality). One-line fix; do it before another session steps on it.

**4. PSI promotion gate has no telemetry — STRUCTURAL gap.** `psi_promotion_attempted` event has no producer anywhere in code. The May 13 question "is the gate stalled or correctly rejecting 116+ candidates?" has been unanswerable from telemetry the entire time. Same root lesson as the instrumentation audit (instrument the output, not the loop) — applied to promotions. Add a rejection-reason column or `assessment_event` write into Phase 2 scope so future audits aren't blind.

**5. Carried-forward unresolved canon items (from May 13, still open):**
- **One-pager dashboard reconciliation** never ran (May 13 Section A item 3). Still pending.
- **Collector count** discrepancy: dashboard 16, business plan "32," deck "51." Still unreconciled (May 13 Section B item 7).
- **CQI Contagion pool coverage** 20% (3/15) vs. deck's protocol-wide framing (May 13 Section B item 5).

---

## Section C — New canonical bug class for V9.12 (or V9.13)

`heartbeat_attests_while_output_stream_dead` should be written into the amendment chain as a named class alongside the April 12 / Wave A-B silent-zero family. Definition: *an attestation that fires on loop execution rather than output production will report green while a covered output stream is dead; domains whose single cycle covers multiple output types are the high-risk surface.* The Part B fix (per-stream output-table cadence check) is the canonical remediation pattern. This belongs in the constitution because it changes what "attested" is allowed to mean.

---

## Section D — Scenario obligation

**Slug:** `heartbeat_attests_while_output_stream_dead` (proposed; NOT yet authored — requires a separate scenarios-session per holdout isolation).
**bug.patch shape:** a domain with multiple writers (e.g. `discovery_signals`); mutate the worker so one writer (entity_discovery) is silently skipped while another (large_mint_burn) keeps firing every cycle, keeping the aggregate heartbeat and `MAX(detected_at)` fresh.
**stage_1.sql shape:** assert that after the cycle, a per-stream check reports DEGRADED for the silent stream despite the fresh aggregate heartbeat.
This is arguably the most important scenario in the suite — the class has already hidden three real failures. Architect to dispatch the scenarios-session.

---

## Section E — Next actions (ordered, dependencies enforced)

**Immediate (parallel, all unblock the rest):**
1. **Open the PR** for `claude/basis-health-instrumentation-aGpGo` (4ad9108). Must pass `invoke-regression-matrix` before merge. *Everything downstream waits on this.*
2. **Fix the CLAUDE.md migration counter** (085 → 112). One-line edit, do before another CC session steps on it.
3. **Dispatch the antipattern sweep** (CC prompt drafted): grep all collectors for attestation gated on `new/stored/promoted > 0` — find the rest of the class before more alerts hit.
4. **Dispatch the scenarios-session** to author `heartbeat_attests_while_output_stream_dead`.

**Architect decisions (gating, not CC work):**
5. **SII/PSI build-vs-rescope** (Section B item 1). Phase 0 done; architect must approve:
   - Build path (recommended): publication-gate first, then PSI registry routing, then SII `peggedAssets` feed.
   - OR rescope path: edit deck:216 to match reality.
   - The 82-site serving gap is a real structural defect either way.
6. **Mempool capture: restore Alchemy access vs. retire the capability** (Section A item 2). Also audit whether the same Alchemy limit is throttling *other* Alchemy-backed collectors.

**Once #1 merges and #5 is approved (in order — gates are hard):**
7. SII/PSI Phase 1 — scenarios-session authors `sii_psi_discovery_unpublished_until_approved`; matrix must show RED against `main` before build.
8. SII/PSI Phase 2 — build per approved shape. New discovery streams MUST register in the per-stream cadence registry from PR #1 (otherwise the same blind-heartbeat defect ships into the new code). Include the promotion-telemetry fix from canon flag #4.

**Carried forward (still open from May 13):**
9. One-pager reconciliation sweep.
10. Collector-count reconciliation (16 vs 32 vs 51).
11. Keeper first on-chain state root — gated on dark-factory + automated-analysis being trustworthy, which #1, #3, #5, #7, #8 collectively advance.
12. Wave B silent-zero collectors; V9.12 finalize/upload; PSI promotion-gate diagnostic (subsumed by #4/#8 above — track here for closure).

---

## The honest prioritization snapshot

May 27 was an instrumentation-integrity day. The finding that matters: **our health signal was structurally optimistic** — it confirmed loops were turning, not that work was getting done — and that single defect had been quietly hiding real failures (one dead capability, two blind collectors) while every dashboard read green. The fix makes attestation mean what it claims to mean: output liveness, checked at the table.

Phase 0 of the SII/PSI auto-discovery investigation also landed today and inverted the apparent scope. What looked like "build a discovery feed" is actually **"build a publication gate"** — the discovery feeds are cheap, but the deck-claimed policy ("auto-discover and add to coverage") is currently unenforceable: no `published` column exists on any serving path, and 82 read sites would expose any new entity the instant it was scored. The load-bearing work is the gate. Discovery is the easy follow-on. SII auto-discovery is only safe to wire in *because* the gate makes a noisy feed (DeFiLlama `peggedAssets`, ~120 candidates) harmless to consume.

This is the precondition for the strategic sequence on the board. We cannot green-light unattended on-chain keeper commits, or tell the market the dark factory is self-feeding, while (a) the instrument that's supposed to tell us "it's working" reports green on dead subsystems, and (b) the system has no mechanism to keep newly-discovered entities out of public surfaces until reviewed. Four things now need to be true in sequence: (1) the instrument tells the truth — PR for branch `aGpGo`; (2) the antipattern class is swept and a scenario guards it — next two dispatches; (3) the publication gate is built and the SII/PSI auto-expansion claim is either made true through gated discovery or made honest through deck rescope; (4) PSI promotion gets the telemetry it currently lacks. Then the keeper, then GTM.

Two corrections recorded for discipline. First, earlier in this session the entity_discovery silence was diagnosed as a #265-shape stall — it was not, it was a healthy weekly cycle misread by the same optimistic instrument the audit exists to fix. Second, the build prompt I dispatched assumed `psi_promotion_attempted` events existed in telemetry — Phase 0 found no producer in code; that query was unanswerable from the start. Both recorded here so the thread's mistakes don't propagate as fact.

---

*Basis Protocol · shlok@basisprotocol.xyz · Confidential · May 27, 2026*
