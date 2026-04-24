# Basis Analytic Engine — Step 0 Package (v0.2)

**Version:** 0.2 (pre-implementation, post-review)
**Status:** Contract document. Incorporates schema-review findings and document-level residuals from v0.1. Review → approve → S0 runs against this.
**Last updated:** April 24, 2026

---

<!-- §0 -->

## 0. Purpose of this document

This is the pre-implementation contract for the analytic engine. Before any CC session writes code:

1. Schema is final (no `dict` types at typed boundaries, no ambiguous fields, versioned)
2. Peer discovery semantics are decided
3. LLM integration has determinism + governance specified
4. Coverage fixtures are pinned
5. Auth model is listed per endpoint
6. Execution plan reflects the C4/C5 split and resolved router-conflict pattern

All downstream sessions (S0, P1, P2, P3, G1, P4, S4, S5, G2) consume this document as input. Any change post-approval requires a schema-amendment commit with explicit justification.

**What changed from v0.1:** 20 residual fixes from the review pass — 12 schema-level (Signal baseline window, required Interpretation.model_id, UUID-linked methodology/follow-up, AnalysisAccepted, AnalysisCreate split, status-default alignment, Literal ThresholdType, engine_llm_usage table, superseded_by_id, archived_at, ConfidenceLevel semantics, cache-hit timestamp semantics) and 8 document-level (model ID pinning, complete migration-088 table list, two named templates, asyncio runtime, router aggregation, realistic P2 timeline, existing-module isolation, degraded-analysis operator visibility). See §10 for the full changelog.

**Migration number update (v0.2a):** Initial v0.2 referenced migration `085` based on the CLAUDE.md guidance. On disk, migrations 085, 086, and 087 are already taken. **S0's migration ships as `088_engine_core.sql`.** All §3, §7, §9, §10 references to "085" should be read as "088."

---

<!-- §1 -->

## 1. Analysis Schema (v2) — Final Pydantic

```python
"""
Basis Analytic Engine — canonical schemas.
Location: app/engine/schemas.py

These models are the contract for every engine endpoint, renderer, and
storage row. Breaking changes require a schema-amendment commit and
invalidate cached analyses (see §3 LLM Governance).
"""

from __future__ import annotations
from datetime import date, datetime
from typing import Any, Literal, Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field, model_validator

# ─────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────

CoverageType = Literal["live", "backfilled", "sparse", "none"]
Density = Literal["multiple_daily", "daily", "weekly", "sparse", "single"]
CoverageQuality = Literal[
    "full-live",
    "partial-live",
    "partial-reconstructable",
    "sparse",
    "none",
]
AnalysisType = Literal[
    "incident_page",
    "retrospective_internal",
    "case_study",
    "internal_memo",
    "talking_points",
    "one_pager",
    "nothing",
]
Severity = Literal["low", "medium", "high", "critical"]
ObservationKind = Literal["value", "trend", "delta", "z_score"]
MethodologyObservationType = Literal[
    "weighting_issue",
    "coverage_gap",
    "measure_defect",
    "data_quality",
    "methodology_ambiguity",
]
FollowUpType = Literal["engineering", "methodology", "data_source", "product"]
AnalysisStatus = Literal["pending", "draft", "approved", "archived", "dismissed"]
ArtifactStatus = Literal["draft", "approved", "published", "discarded"]
EventStatus = Literal["new", "queued", "analyzed", "delivered", "dismissed"]
ThresholdType = Literal["score_drop", "score_spike", "coverage_lapse"]

# EventWindow now includes "baseline" for analyses with no event_date.
# Invariant (enforced by Signal model_validator):
#   event_date is None  ↔ only `baseline` populated
#   event_date is not None ↔ only pre_event / event_window / post_event populated
EventWindow = Literal["baseline", "pre_event", "event_window", "post_event"]

# Confidence semantics:
#   high         — LLM produced interpretation with strong signal + peer data
#   medium       — LLM produced interpretation with partial signal or missing peers
#   low          — LLM produced interpretation but structural caveats apply
#                  (sparse data, missing peer_set, methodology gaps)
#   insufficient — Engine could not run interpretation at all (API down,
#                  coverage=none → template fallback, or budget cap hit)
ConfidenceLevel = Literal["high", "medium", "low", "insufficient"]

# ─────────────────────────────────────────────────────────────────
# Coverage
# ─────────────────────────────────────────────────────────────────

class EntityCoverage(BaseModel):
    """Basis's coverage of a single entity in a single index."""
    index_id: str
    entity_slug: str
    entity_name: Optional[str] = None
    coverage_type: CoverageType
    live: bool
    density: Density
    earliest_record: Optional[date] = None
    latest_record: Optional[date] = None
    unique_days: int
    data_source: str  # "generic_index_scores" | "historical_protocol_data" | "scores" | etc.
    available_endpoints: list[str] = Field(default_factory=list)

class RelatedEntity(BaseModel):
    """Entity adjacent to the queried one — suggestion only, not a declaration
    that Analysis should use it as a peer. Peer sets are explicit per §2."""
    index_id: str
    entity_slug: str
    relation: str  # e.g., "same_index_different_protocol", "same_category"

class CoverageResponse(BaseModel):
    identifier: str
    matched_entities: list[EntityCoverage]
    related_entities: list[RelatedEntity]
    adjacent_indexes_not_covering: list[str]
    coverage_summary: str
    coverage_quality: CoverageQuality
    recommended_analysis_types: list[AnalysisType]
    blocks_incident_page: bool
    blocks_reasons: list[str]
    # Attestation: snapshot of what this response reflects at fetch time.
    data_snapshot_hash: str  # "sha256:<hex>" of sorted matched_entities + timestamps
    computed_at: datetime

# ─────────────────────────────────────────────────────────────────
# Observations
# ─────────────────────────────────────────────────────────────────

class Observation(BaseModel):
    """A single factual observation pulled from Basis's data for a given
    entity+index+measure in a given time window.

    Anomaly and peer-divergence are flags on this object, not separate lists
    in Signal. Renderers filter by flag when they want a subset.
    """
    index_id: str
    entity_slug: str
    measure: str  # canonical name, e.g., "peg_volatility_7d", "overall_score"
    window: EventWindow
    kind: ObservationKind
    metric_value: float
    reference_value: Optional[float] = None  # baseline, peer avg, prior period, etc.
    unit: str  # "score_0_100" | "pct" | "usd" | "eth" | "ratio" | "count" | "days"
    at_date: Optional[date] = None
    window_start: Optional[date] = None
    window_end: Optional[date] = None
    is_anomaly: bool = False
    anomaly_z_score: Optional[float] = None
    peer_divergence_magnitude: Optional[float] = None
    peer_slugs_compared: list[str] = Field(default_factory=list)
    notes: Optional[str] = None

class Signal(BaseModel):
    """Structured view of all observations for an analysis.

    Event-conditional layout:
      - When Analysis.event_date is None: only `baseline` is populated.
      - When Analysis.event_date is not None: `baseline` is empty and
        pre_event / event_window / post_event are populated.

    Enforced at the Analysis level via model_validator (see below) so that
    renderers can trust which list to read.
    """
    baseline: list[Observation] = Field(default_factory=list)
    pre_event: list[Observation] = Field(default_factory=list)
    event_window: list[Observation] = Field(default_factory=list)
    post_event: list[Observation] = Field(default_factory=list)

# ─────────────────────────────────────────────────────────────────
# Interpretation (structured, not a single string)
# ─────────────────────────────────────────────────────────────────

class Interpretation(BaseModel):
    """Structured interpretation generated by the analysis engine.
    Renderers select fields by name rather than parsing a blob."""
    event_summary: str
    pre_event_story: Optional[str] = None
    event_story: Optional[str] = None
    post_event_story: Optional[str] = None
    cross_peer_reading: Optional[str] = None
    what_this_does_not_claim: str
    headline: str  # one-sentence top-line suitable for talking points / titles
    confidence: ConfidenceLevel
    confidence_reasoning: str  # why this confidence level

    # Determinism + attestation. Required, no None.
    prompt_version: str  # e.g., "v1", referencing engine_prompts.version
    input_hash: str  # "sha256:<hex>" of (Signal JSON + CoverageResponse JSON + prompt_version)
    model_id: str  # e.g., "claude-sonnet-4-6" or "template:SHAPE_NO_COVERAGE_MEMO"

    # Time this interpretation was first produced. On cache hit, this reflects
    # the original generation time, not the current request time. Served-at
    # is implied by the enclosing Analysis.created_at; divergence between the
    # two indicates a cache hit.
    generated_at: datetime
    from_cache: bool = False

# ─────────────────────────────────────────────────────────────────
# Methodology Observations and Follow-ups
# ─────────────────────────────────────────────────────────────────

class MethodologyObservation(BaseModel):
    id: UUID = Field(default_factory=uuid4)  # stable reference for FollowUp linkage
    observation_type: MethodologyObservationType
    finding: str
    severity: Severity
    affected_indexes: list[str]
    evidence: str  # concrete citation: measure name + value(s) + date

class FollowUp(BaseModel):
    follow_up_type: FollowUpType
    description: str
    affected_indexes: list[str]
    priority: Severity
    estimated_effort: str  # "1 PR" | "multi-week" | "unknown"
    linked_methodology_observation_id: Optional[UUID] = None  # references MethodologyObservation.id

# ─────────────────────────────────────────────────────────────────
# Artifact recommendation
# ─────────────────────────────────────────────────────────────────

class ArtifactRecommendation(BaseModel):
    recommended: AnalysisType
    supports: list[AnalysisType]  # types that would also work; renderers validate
    reasoning: str
    # Types explicitly disallowed for this analysis (e.g., "incident_page" when
    # there's no live pre-event coverage). Derivable from coverage_quality by default.
    blocked: list[AnalysisType] = Field(default_factory=list)
    blocked_reasons: list[str] = Field(default_factory=list)

# ─────────────────────────────────────────────────────────────────
# Analysis — create vs read split
# ─────────────────────────────────────────────────────────────────

class AnalysisCreate(BaseModel):
    """Shape used to construct an Analysis before it's persisted. No id, no
    created_at, no status. Engine writes these at persist time."""
    analysis_version: str = "v0.1"
    entity: str
    event_date: Optional[date] = None
    peer_set: list[str] = Field(
        description="REQUIRED per §2. Empty list is permitted; it triggers a "
                    "methodology observation flagging the missing peer set and "
                    "bumps Interpretation.confidence down one level."
    )
    context: Optional[str] = None  # operator-provided framing, passed to interpretation
    coverage: CoverageResponse
    signal: Signal
    interpretation: Interpretation
    methodology_observations: list[MethodologyObservation]
    follow_ups: list[FollowUp]
    artifact_recommendation: ArtifactRecommendation
    inputs_hash: str  # "sha256:<hex>" of (entity + event_date + peer_set + coverage.data_snapshot_hash)
    previous_analysis_id: Optional[UUID] = None
    supersedes_reason: Optional[str] = None

    @model_validator(mode="after")
    def _validate_signal_window_invariant(self) -> "AnalysisCreate":
        has_event = self.event_date is not None
        baseline_populated = bool(self.signal.baseline)
        windowed_populated = bool(
            self.signal.pre_event or self.signal.event_window or self.signal.post_event
        )
        if has_event and baseline_populated:
            raise ValueError(
                "Signal.baseline must be empty when event_date is set; "
                "use pre_event / event_window / post_event instead."
            )
        if not has_event and windowed_populated:
            raise ValueError(
                "Signal.pre_event / event_window / post_event must be empty "
                "when event_date is None; use `baseline` instead."
            )
        return self

class Analysis(AnalysisCreate):
    """Persisted Analysis. id, created_at, status, and revision fields are
    always populated."""
    id: UUID
    created_at: datetime
    status: AnalysisStatus  # starts "pending", transitions to "draft" after LLM completes
    superseded_by_id: Optional[UUID] = None  # populated when a newer Analysis supersedes this row
    archived_at: Optional[datetime] = None  # set when status flips to "archived"
    human_reviewer: Optional[str] = None
    review_notes: Optional[str] = None

class AnalysisSummary(BaseModel):
    """Compact form for list endpoints."""
    id: UUID
    entity: str
    event_date: Optional[date]
    created_at: datetime
    status: AnalysisStatus
    recommended_artifact_type: AnalysisType
    confidence: ConfidenceLevel
    headline: str
    previous_analysis_id: Optional[UUID] = None
    superseded_by_id: Optional[UUID] = None

# ─────────────────────────────────────────────────────────────────
# Request + response bodies
# ─────────────────────────────────────────────────────────────────

class AnalyzeRequest(BaseModel):
    entity: str
    event_date: Optional[date] = None
    peer_set: list[str] = Field(
        description="REQUIRED. Empty list permitted; see §2."
    )
    context: Optional[str] = None
    save: bool = True  # set False for dry-run (no persist, returns ephemeral Analysis)
    force_new: bool = False  # if True, archive any existing Analysis for (entity, event_date)
                             # and create a new row; previous_analysis_id auto-populated.

class AnalysisAccepted(BaseModel):
    """202 response from POST /api/engine/analyze. The Analysis row is
    pre-allocated with status="pending"; the LLM call runs asynchronously.
    Poll poll_url until status != "pending"."""
    analysis_id: UUID
    status: Literal["pending"]
    poll_url: str  # e.g., "/api/engine/analyses/{id}"
    estimated_ready_at: Optional[datetime] = None

class RenderRequest(BaseModel):
    analysis_id: UUID
    artifact_type: AnalysisType

# ─────────────────────────────────────────────────────────────────
# Artifacts
# ─────────────────────────────────────────────────────────────────

class ArtifactResponse(BaseModel):
    id: UUID
    analysis_id: UUID
    artifact_type: AnalysisType
    rendered_at: datetime
    content_markdown: str
    suggested_path: Optional[str] = None
    suggested_url: Optional[str] = None
    status: ArtifactStatus
    published_url: Optional[str] = None
    warnings: list[str] = Field(default_factory=list)

# ─────────────────────────────────────────────────────────────────
# Events + Watchlist (used by C4)
# ─────────────────────────────────────────────────────────────────

class EngineEventCreate(BaseModel):
    source: str  # "defillama_hacks" | "watchlist" | "manual"
    event_type: str  # "exploit" | "score_drop" | "score_spike" | "coverage_lapse"
    entity: str
    event_date: Optional[date] = None
    severity: Severity
    raw_event_data: dict[str, Any]  # source-specific payload; heterogeneous by design

class EngineEvent(EngineEventCreate):
    id: UUID
    detected_at: datetime
    analysis_id: Optional[UUID] = None
    artifact_id: Optional[UUID] = None
    status: EventStatus
    delivered_at: Optional[datetime] = None
    operator_response: Optional[str] = None

class WatchlistEntry(BaseModel):
    id: UUID
    entity_slug: str
    index_id: Optional[str] = None  # None = all covering indexes
    threshold_type: ThresholdType
    threshold_value: float
    active: bool = True
    created_at: datetime
```

### Schema design notes — why each decision

- **`coverage: CoverageResponse` on `Analysis`**, not `dict`. The boundary between analyze and render is typed; drift is caught at Pydantic validation, not at render time.
- **`Interpretation` is structured**, not a blob. Fields like `event_summary`, `cross_peer_reading`, `what_this_does_not_claim`, `headline` map cleanly to renderer needs. Talking points uses `headline`; internal memos use `event_summary` + `what_this_does_not_claim`; case studies use everything.
- **`Observation` carries `unit` and `kind`**. Unit is the difference between "15.0 is a score" and "15.0 is a percentage." Kind distinguishes raw value from trend slope from z-score.
- **Anomaly and peer divergence as flags on `Observation`**, not separate lists in `Signal`. Eliminates the double-counting problem.
- **`Signal.baseline` for no-event analyses**, windowed lists for event analyses. Model validator enforces mutual exclusivity so renderers can trust which list to read.
- **`AnalyzeRequest.peer_set` is required**, not optional. Forces explicit declaration of what comparison is being made. Empty list permitted (produces a partial analysis with a methodology observation flagging the gap — see §2).
- **`force_new=true` is the only trigger for re-analysis**. Pipeline (C4) never auto-supersedes. Operator owns the revision decision.
- **`inputs_hash` and `coverage.data_snapshot_hash`** for attestation. An Analysis can be replayed; a reviewer six months later can verify the exact inputs that produced it.
- **`previous_analysis_id` + `superseded_by_id`** form a doubly-linked revision chain. Either direction of lookup is a single column read.
- **`archived_at` on `Analysis`** makes the uniqueness-constraint-with-status-filter (`(entity, event_date)` unique WHERE `status != 'archived'`) observable without digging through audit logs.
- **`Interpretation.model_id` is required**, not Optional. Every interpretation has a named producer — either a pinned Claude model ID or `template:SHAPE_NAME` for fallback paths. Attestation requires this.
- **`Interpretation.generated_at` + `from_cache`** record the original generation time even on cache hit. Divergence between `generated_at` and the enclosing `Analysis.created_at` tells a reviewer "this interpretation was served from cache on a later analysis."
- **`MethodologyObservation.id` + `FollowUp.linked_methodology_observation_id`** — stable UUID reference, not a list index. Survives reordering and partial filtering.
- **`AnalysisCreate` vs `Analysis` split** — standard Pydantic pattern. Read-path consumers never see `id=None`. S0 commits both models together.
- **`Analysis.status="pending"` default** — aligns with §3's async contract. The row is written first, the LLM call runs in the background, the status flips to `"draft"` on completion.
- **`WatchlistEntry.threshold_type: ThresholdType`** — Literal enum, matches `EngineEvent.event_type` values to eliminate string drift between watch trigger and generated event.
- **Uniqueness constraints** (declared in migration 088, see §3):
  - `engine_analyses`: `(entity, event_date)` UNIQUE WHERE `status != 'archived'`
  - `engine_events`: `(source, entity, event_date, event_type)` UNIQUE
  Both enforced at the DB level. `force_new=true` archives the existing analysis row (sets `archived_at`) before inserting the new one.

---

<!-- §2 -->

## 2. Peer Discovery — Option 3 (explicit), with pipeline fallback

### Decision

For v1, `AnalyzeRequest.peer_set` is **required and operator-supplied**. No auto-discovery. The schema surfaces suggested peers via `CoverageResponse.related_entities`, but suggestion ≠ declaration.

### Why not auto-discover in v1

Auto-discovery requires a peer-group taxonomy Basis doesn't yet have. For LSTI, all entities in the index are plausible peers. For PSI, "protocols of similar shape and scale" is a category question (Jupiter Perps vs dYdX vs Hyperliquid — same market, different architecture). For BRI, "peers of LayerZero" is actively contested.

Inventing peer groups in code without operator curation produces wrong comparisons silently. Forcing explicit declaration surfaces the decision every time.

### Pipeline handling (C4)

When C4 auto-triggers an analysis from a detected event, it supplies `peer_set=[]`. The analysis completes with:

- Empty `peer_divergence_magnitude` on all observations
- Empty `peer_slugs_compared` on all observations
- `Signal` still populated (baseline or windowed per event_date)
- `MethodologyObservation` added with `observation_type="coverage_gap"`, finding="Entity lacks a declared peer set; cross-peer observations were not computed. Peer curation is required to strengthen this analysis."
- `FollowUp` added: "Declare peer set for {entity} in curation table or via operator input for future analyses."
- `Interpretation.cross_peer_reading` left `None`
- `Interpretation.confidence` bumped down one level (high → medium, medium → low) to reflect missing peer context

A partial analysis with a flagged gap is valuable. A stalled pipeline waiting for peer curation is not.

### Forward path (v2+)

- **v1.1:** `entity_peer_groups` table. Operator populates via admin endpoint. Coverage endpoint returns suggested peers from the table. `AnalyzeRequest.peer_set` remains required but can be defaulted from the table if caller sends null.
- **v1.2:** Index-type defaults. `LSTI` peers = all LSTI entities. `SII` peers = same tier stablecoins. Hardcoded fallbacks when no group entry exists.
- **v2+:** Learned or structural peer-group inference. Out of scope for this phase.

---

<!-- §3 -->

## 3. LLM Integration — Determinism and Governance

### Core decision

Analysis interpretation is LLM-generated from day one (v1), with the following determinism structure:

- `temperature=0` on all Anthropic calls
- Input hash computed as SHA-256 of `(Signal JSON + CoverageResponse JSON + prompt_version)`, stored as `"sha256:<hex>"`
- Cache table stores `input_hash → interpretation_json`
- Same inputs produce same output via cache hit; different inputs trigger new call and new cache entry
- Prompt files are versioned, permanent, and in-repo

### Model selection and pinning

Default model: **`claude-sonnet-4-6`** (current Sonnet). Declared as a constant in `app/engine/config.py`:

```python
LLM_MODEL_ID = "claude-sonnet-4-6"
LLM_MODEL_FALLBACK_ID = None  # no automatic fallback; template path handles outages
```

Every analysis logs `Interpretation.model_id` to the pinned constant. Model swap = config change = traceable commit. Opus is available by swapping the constant; the schema tracks the exact ID per analysis so mixed-model histories remain attributable.

### Prompt governance

Prompts live at `app/engine/prompts/interpretation_prompt_v{N}.md`. Format: single markdown file per version with structured sections (System / Instructions / Output Format / Examples). Every version is:

- Committed to the repo, never deleted
- Referenced by `Interpretation.prompt_version` on every Analysis
- Code-reviewed like any other engineering change — not edited casually

Prompt changes:

- New use case or interpretation shape → new prompt version (v1 → v2)
- Typo fix or prose polish → still a new version. No "silent" edits.
- Deprecated versions remain in repo and remain in cache until their referenced Analyses are archived or superseded

### Fallback templates — exactly two

v1 does not perform shape-matching across many templates. Two narrow templates exist as structural fallbacks for paths where LLM interpretation is not appropriate or not available:

1. **`SHAPE_NO_COVERAGE_MEMO`** — used when `coverage_quality="none"`. There is nothing to interpret; the Analysis documents the coverage gap itself (see §6). Template-generated; `Interpretation.model_id = "template:SHAPE_NO_COVERAGE_MEMO"`.
2. **`SHAPE_API_UNAVAILABLE`** — used when the Anthropic API fails after retries during a non-zero-coverage analysis. Produces a minimal Interpretation with `confidence="insufficient"` and `confidence_reasoning="LLM API unavailable; interpretation is template-only. Retry via force_new=true when API recovers."`. Template-generated; `Interpretation.model_id = "template:SHAPE_API_UNAVAILABLE"`.

These are the only two templates in v1. No third template is permitted. Any pattern that feels like "we need another shape" is actually a prompt-version bump (v1 → v2), not a new template.

### Async runtime contract

LLM calls take 3–15 seconds. `POST /api/engine/analyze` is async:

1. Request handler computes `inputs_hash`, pre-allocates `engine_analyses` row with `status="pending"`, returns `AnalysisAccepted` (HTTP 202) with `analysis_id` and `poll_url`
2. Handler launches background task via `asyncio.create_task(run_interpretation(...))` — in-process, no external queue
3. Background task: checks `engine_interpretation_cache` by `input_hash` → on hit, populates Interpretation from cache (`from_cache=true`, `generated_at` = cached value) → on miss, calls Anthropic API, writes cache entry, populates Interpretation (`from_cache=false`, `generated_at=now()`)
4. Background task flips `status` from `"pending"` to `"draft"` and commits
5. Client polls `GET /api/engine/analyses/{id}` until `status != "pending"`

**Crash recovery:** If the app process restarts while an Analysis is `"pending"`, the row stays orphaned. A reaper task (runs every 5 minutes via APScheduler) finds rows with `status="pending"` and `created_at < now() - interval '10 minutes'`, flips them to `"draft"` with `Interpretation.model_id="template:SHAPE_API_UNAVAILABLE"` and `confidence="insufficient"`. Operator can retry via `force_new=true`.

**Escalation path:** If concurrency or cost demands it later, the `asyncio.create_task` site is the single point of change — swap for Celery/RQ/Arq without altering the API contract. State lives in DB, not memory, so the migration is safe.

### Cost and budget

- Per-analysis cost estimate: ~$0.02–$0.08 at Sonnet pricing, ~$0.10–$0.30 at Opus
- Monthly soft cap: `BASIS_ENGINE_LLM_MONTHLY_BUDGET_USD = 200` (env var; operator-adjustable)
- Daily hard ceiling: 50 LLM calls/day (caps runaway pipelines)
- When budget is hit, new analyses fall back to `SHAPE_API_UNAVAILABLE` template until next month or explicit operator override (admin endpoint)
- Counters maintained in `engine_llm_usage` (see schema below)

### Idempotency

`(entity, event_date)` uniqueness on `engine_analyses` (where not archived) prevents redundant analyses at the table level. A 10-point LSTI drop triggered three times in one day produces one analysis, not three. `force_new=true` is the explicit override, admin-only, and archives the prior analysis before inserting the new row.

### Migration 088 — engine_core (seven tables)

S0 ships migration 088 as a single file creating all seven engine tables:

1. **`engine_analyses`** — see §1 `Analysis` model. Includes unique constraint `(entity, event_date)` WHERE `status != 'archived'`.
2. **`engine_artifacts`** — see §1 `ArtifactResponse`. FK to `engine_analyses`.
3. **`engine_events`** — see §1 `EngineEvent`. Unique constraint `(source, entity, event_date, event_type)`. FK columns `analysis_id`, `artifact_id` nullable.
4. **`engine_watchlist`** — see §1 `WatchlistEntry`.
5. **`engine_prompts`** — prompt version registry:
   ```sql
   CREATE TABLE engine_prompts (
     version TEXT PRIMARY KEY,               -- "v1", "v2", ...
     file_path TEXT NOT NULL,                -- "app/engine/prompts/interpretation_prompt_v1.md"
     activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
     deprecated_at TIMESTAMPTZ,
     sha256 TEXT NOT NULL,                   -- hash of prompt file contents
     notes TEXT
   );
   ```
6. **`engine_interpretation_cache`** — deterministic interpretation cache:
   ```sql
   CREATE TABLE engine_interpretation_cache (
     input_hash TEXT PRIMARY KEY,            -- "sha256:<hex>"
     interpretation_json JSONB NOT NULL,
     prompt_version TEXT NOT NULL REFERENCES engine_prompts(version),
     model_id TEXT NOT NULL,
     token_input_count INT,
     token_output_count INT,
     created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
     hit_count INT DEFAULT 0
   );
   CREATE INDEX idx_interpretation_cache_prompt ON engine_interpretation_cache(prompt_version);
   ```
   Entries are never deleted in v1. Archival policy added later if cache size becomes a real problem.
7. **`engine_llm_usage`** — monthly cost rollups + cap enforcement state:
   ```sql
   CREATE TABLE engine_llm_usage (
     month DATE PRIMARY KEY,                 -- first day of month, UTC
     calls_count INT NOT NULL DEFAULT 0,
     tokens_input BIGINT NOT NULL DEFAULT 0,
     tokens_output BIGINT NOT NULL DEFAULT 0,
     cost_usd NUMERIC(10,4) NOT NULL DEFAULT 0,
     monthly_cap_usd NUMERIC(10,4) NOT NULL,
     cap_triggered_at TIMESTAMPTZ,
     operator_override_at TIMESTAMPTZ,
     notes TEXT
   );
   ```

Seven tables, one migration file. S0 does not split them.

---

<!-- §4 -->

## 4. Coverage Fixture Plan

### Purpose

Pin the Component 1 contract before P1 implements it. Fixture responses define what `CoverageResponse` actually looks like for known entities, importable by P2 and P3 during their mocked testing.

### Fixtures to extract (operator runs queries, pastes results)

Six canonical fixtures:

1. **`drift`** — PSI backfilled (844 days), dex_pool_data sparse (2 days), web_research_protocol single. Expected `coverage_quality="partial-reconstructable"`, `blocks_incident_page=true`.
2. **`kelp-rseth`** — LSTI live daily (~10+ days), nothing else. Expected `coverage_quality="partial-live"`, `blocks_incident_page=true` (only one live index).
3. **`usdc`** — SII live daily. Expected `coverage_quality="partial-live"` or `"full-live"` depending on index count. `blocks_incident_page=false` if `full-live`.
4. **`jupiter-perpetual-exchange`** — PSI backfilled only (peer of Drift). Expected `coverage_quality="partial-reconstructable"`.
5. **`layerzero`** — BRI live-ish, web_research_bridge single point. Expected `coverage_quality="partial-live"`.
6. **`this-entity-does-not-exist-xyz`** — expected `coverage_quality="none"`, empty lists, 404 at endpoint level.

### Extraction flow

For each entity, operator runs the coverage SQL (provided as the first post-approval deliverable — five queries per entity covering `generic_index_scores`, `historical_protocol_data`, `scores`, `pg_trgm` fuzzy match, and the adjacent-index negative-space check). Outputs get formatted into a Python fixtures file:

```python
# tests/fixtures/canonical_coverage.py
from app.engine.schemas import CoverageResponse
from datetime import date, datetime

DRIFT_COVERAGE = CoverageResponse(
    identifier="drift",
    matched_entities=[...],  # filled from production query
    related_entities=[...],
    adjacent_indexes_not_covering=[...],
    coverage_summary="Drift has backfilled PSI coverage...",
    coverage_quality="partial-reconstructable",
    recommended_analysis_types=["retrospective_internal", "case_study", "internal_memo"],
    blocks_incident_page=True,
    blocks_reasons=["No live pre-event coverage; pre-event claims require temporal reconstruction per V9.6 constitutional amendment"],
    data_snapshot_hash="sha256:...",
    computed_at=datetime(2026, 4, 22, 18, 0, 0),
)

KELP_RSETH_COVERAGE = CoverageResponse(...)
USDC_COVERAGE = CoverageResponse(...)
JUPITER_PERP_COVERAGE = CoverageResponse(...)
LAYERZERO_COVERAGE = CoverageResponse(...)
UNKNOWN_ENTITY_COVERAGE = None  # represents 404
```

Every P-session imports these fixtures. P1's real endpoint output must match the fixture for each entity, byte-for-byte after datetime normalization. Drift between Component 1's real output and the fixture is caught at P1 test time, not at G1 integration time.

### Fixture refresh policy

Fixtures are pinned at S0 and do not change during engine construction. Operator-initiated refresh cadence: **monthly** (first business day). If production data evolves in ways that invalidate fixtures between refreshes (new indexes added, entity coverage changes), a dedicated fixture-refresh commit updates them — after which affected tests re-run. Fixtures are not auto-regenerated; drift is a deliberate operator decision.

---

<!-- §5 -->

## 5. Auth Model — Per-Endpoint

Admin auth via the existing `X-Admin-Key` header pattern (see `app/rate_limiter.py` precedent).

| Endpoint | Access | Rationale |
|---|---|---|
| `GET /api/engine/coverage/{id}` | **Public** | Data already queryable via existing public endpoints; surfacing it as coverage doesn't expose new information |
| `POST /api/engine/analyze` | **Admin only** | Expensive (LLM costs); prevents external parties from triggering speculative analyses |
| `GET /api/engine/analyses/{id}` | **Admin only** | Interpretations may contain non-public reasoning about protocols |
| `GET /api/engine/analyses` (list) | **Admin only** | Same |
| `POST /api/engine/render` | **Admin only** | Coupled to `/analyze` — if the analysis is private, renderings of it are too |
| `GET /api/engine/artifacts/{id}` | **Depends on artifact_type and status** | `retrospective_internal`, `internal_memo` → always admin. Others → public once `status="approved"` or `"published"`; admin-only while `draft` |
| `POST /api/engine/artifacts/{id}/approve` | **Admin only** | Operator approval step |
| `POST /api/engine/artifacts/{id}/dismiss` | **Admin only** | Operator rejection step |
| `POST /api/admin/engine/events` | **Admin only** | Already path-prefixed |
| `GET /api/admin/engine/llm-usage` | **Admin only** | Budget inspection endpoint |
| `POST /api/admin/engine/llm-usage/override` | **Admin only** | Budget-cap override after cap trigger |
| Watchlist CRUD | **Admin only** | Already path-prefixed |

### Rate limiting

- Public endpoints (coverage) use the default public limit (10 req/min per IP)
- Admin endpoints use the keyed limit (120 req/min per admin key)
- `POST /api/engine/analyze` has an additional hard cap of 10 per hour per admin key to bound LLM cost (layered on top of the §3 daily 50-call ceiling and monthly $200 budget)

### Implementation note for P1/P2/P3

S0's per-component router files (`coverage_router.py`, `analyze_router.py`, `render_router.py`) wire the auth dependency into each route at declaration time. Sessions don't decide auth — they apply the decorator. Example:

```python
# app/engine/coverage_router.py
from fastapi import APIRouter, Depends
from app.auth import public_rate_limited
from app.engine.schemas import CoverageResponse

router = APIRouter()

@router.get(
    "/coverage/{identifier}",
    response_model=CoverageResponse,
    dependencies=[Depends(public_rate_limited)],
)
async def get_coverage(identifier: str) -> CoverageResponse:
    ...
```

vs:

```python
# app/engine/analyze_router.py
from fastapi import APIRouter, Depends
from app.auth import admin_required, admin_rate_limited, analyze_hourly_cap

router = APIRouter()

@router.post(
    "/analyze",
    status_code=202,
    dependencies=[
        Depends(admin_required),
        Depends(admin_rate_limited),
        Depends(analyze_hourly_cap),
    ],
)
async def analyze(payload: AnalyzeRequest) -> AnalysisAccepted:
    ...
```

### Router aggregation

S0 creates `app/engine/router.py` as the single engine entry point:

```python
# app/engine/router.py
from fastapi import APIRouter
from app.engine.coverage_router import router as coverage_router
from app.engine.analyze_router import router as analyze_router
from app.engine.render_router import router as render_router

router = APIRouter(prefix="/api/engine")
router.include_router(coverage_router)
router.include_router(analyze_router)
router.include_router(render_router)
```

`app/server.py` imports only this top-level router. P1/P2/P3 each own their sub-router file and never touch `router.py` or each other's files. Zero merge-conflict surface.

---

<!-- §6 -->

## 6. Zero-Coverage Analyses — First-Class, Not Silent

### Decision

`coverage_quality="none"` still produces an `Analysis` row. The Analysis:

- Has empty `Signal` (all four lists empty; validator trivially passes since nothing contradicts the event-window invariant)
- Has `Interpretation.event_summary` filled from a dedicated template
- Has `Interpretation.headline` that names the coverage gap, e.g., "Basis does not currently cover {entity}; this analysis documents the gap"
- Has `Interpretation.what_this_does_not_claim` = "This analysis makes no claim about {entity}'s pre- or post-event state. Basis has no coverage."
- Has `Interpretation.model_id = "template:SHAPE_NO_COVERAGE_MEMO"`, `confidence = "insufficient"`, `confidence_reasoning` citing no-coverage
- Has `methodology_observations` populated with a single `coverage_gap` finding, severity `high`
- Has `follow_ups` populated with:
  - Engineering: "Extend {appropriate_index} to cover {entity}"
  - Data source: "Identify data sources for {entity} to inform collector design"
- Has `artifact_recommendation.recommended="internal_memo"`
- Has `artifact_recommendation.supports=["internal_memo"]` only — nothing else is valid
- Has `artifact_recommendation.blocked` including every other artifact type, with blocked_reasons citing no coverage

### Why this matters

The whole premise of the engine is that methodology gaps surface structurally. Silent non-response to uncovered events looks like Basis missed them; explicit "we don't cover this, here's why, here's the follow-up" is the correct signal and the first step toward adding coverage. It also creates a log — a history of "exploits we didn't cover" becomes a prioritization input for what to build next.

---

<!-- §7 -->

## 7. Revised Execution Plan

### Dependency graph

```
                 ┌─────────────────────────────┐
                 │ S0 — Schema Foundation      │
                 │ (schemas + migration 088 +  │
                 │  canonical fixtures + auth  │
                 │  deps + per-component       │
                 │  router stubs + prompt v1   │
                 │  stub + reaper job)         │
                 └─────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
   ┌──────────┐         ┌──────────────┐    ┌──────────────┐
   │ P1       │         │ P2           │    │ P3           │
   │ Coverage │         │ Analysis +   │    │ Renderer     │
   │ endpoint │         │ LLM loop +   │    │ base + 2     │
   │          │         │ cache + async│    │ renderers    │
   │          │         │ status       │    │              │
   └──────────┘         └──────────────┘    └──────────────┘
       │                    │                      │
       └─────────┬──────────┴──────────────────────┘
                 ▼
         ┌──────────────┐
         │ G1           │
         │ Integration  │
         │ gate         │
         └──────────────┘
                 │
                 ▼
         ┌──────────────────────────┐
         │ P4 — Remaining 4         │
         │ renderers                │
         └──────────────────────────┘
                 │
                 ▼
         ┌──────────────────────────┐
         │ C4 (S4) — Detection +    │
         │ Pipeline                 │
         └──────────────────────────┘
                 │
                 ▼
         ┌──────────────────────────┐
         │ C5 (S5) — Operator       │
         │ Workflow (Slack +        │
         │ Approval + Git commit +  │
         │ degraded-tag rule)       │
         └──────────────────────────┘
                 │
                 ▼
         ┌──────────────────────────┐
         │ G2 — End-to-End          │
         │ Validation               │
         └──────────────────────────┘
```

### What changed from v0.1's execution plan

- **C4/C5 split confirmed.** Detection+Pipeline (C4/S4) and Operator Workflow (C5/S5) are separate stages. C4 can ship and run in draft-accumulation mode before C5 exists; operators retrieve drafts manually via API.
- **Per-component router files.** S0 creates `coverage_router.py`, `analyze_router.py`, `render_router.py` stubs plus the aggregating `router.py`. P-sessions add routes only to their own files. Zero merge conflicts by construction.
- **Canonical fixtures pinned at S0.** P2 and P3 import from `tests/fixtures/canonical_coverage.py`. Coverage drift caught during P1 test runs, not G1.
- **LLM integration inside P2.** Adds ~2 days to P2 but removes the template-matching dead end. P2 now realistically lands in **5–6 days** rather than 4.
- **Prompt file v1 stubbed at S0.** Content finalized during P2. `engine_prompts` seeded with v1 row as part of migration 088 data seeding.
- **Migration 088 bundles all seven engine tables** (see §3). No second migration file until C4 needs one (089 reserved for C4 work).
- **Unique constraints in migration 088:**
  - `engine_analyses (entity, event_date)` WHERE `status != 'archived'`
  - `engine_events (source, entity, event_date, event_type)`
- **Auth decisions from §5 applied at S0 router stub time.** Every route declaration includes its auth dependency from day one.
- **Reaper task in S0.** Orphaned `"pending"` analysis rows (> 10 min old) flipped to `"draft"` with `SHAPE_API_UNAVAILABLE`. APScheduler runs it every 5 minutes. Ships with S0 so P2's async contract has a safety net from day one.
- **Degraded-analysis operator visibility baked into C5.** Any delivered analysis with `confidence="insufficient"` or `model_id` starting with `"template:"` gets a distinct Slack tag: `[engine-degraded]`. Named in C5's scope so it's not forgotten.
- **Explicit isolation from existing modules.** v1 engine does not read from or modify `app/report.py`, `app/integrity.py`, `app/divergence.py`, or `app/discovery.py`. Future integration may consume their outputs as additional Observation sources via a registered adapter; out of scope for Step 0 through G2.

### Session ownership rules

- No session modifies existing models in `schemas.py` after S0. Additions (new model types) allowed only via a schema-amendment commit with operator sign-off.
- No session edits another session's router file.
- No session edits canonical fixtures.
- Any requested schema or fixture change halts the session and produces an amendment proposal.
- Every commit message follows `engine: <component> — <brief description>`.

### Estimated timeline

- **S0:** 1–1.5 days (larger than v0.1 due to seven-table migration, canonical fixtures, per-component router stubs, reaper job, prompt v1 stub)
- **P1:** ~2 days
- **P2:** 5–6 days (LLM + cache + async status + 10 tests)
- **P3:** ~2 days
- **P1/P2/P3 in parallel:** P2 dominates → ~5–6 days wall-clock
- **G1:** half-day
- **P4:** 1–2 days
- **C4/S4:** 3–4 days
- **C5/S5:** 2–3 days
- **G2:** 1 day

Total: ~13–18 days of focused work, compressible to 10–12 days if sessions run hot.

---

<!-- §8 -->

## 8. Operator decisions (baked in)

Recorded from the v0.1 review pass. These are decided, not open:

1. **Model choice:** Sonnet default (`claude-sonnet-4-6`), Opus available via config swap. Pinned in `app/engine/config.py`.
2. **Monthly LLM budget:** $200/month soft cap, 50 calls/day hard ceiling. Operator can override post-trigger via admin endpoint. Acknowledged as "expensive but okay for now."
3. **Prompt file format:** single markdown file per version, structured sections (System / Instructions / Output Format / Examples).
4. **GitHub write access for approval flow (C5):** PAT scoped to `basis-protocol/basis-hub` only, stored as Railway env var, rotation quarterly.
5. **Slack channel for C5 delivery:** single channel. No per-severity routing in v1.
6. **Test fixtures maintenance:** operator-run refresh monthly (first business day). No automated drift detection in v1.
7. **Analysis list pagination:** simple `limit` parameter in v1 via `AnalysisSummary` list endpoint. Cursor/offset pagination deferred until list volume demands it.

---

<!-- §9 -->

## 9. Post-approval actions

Once this document is approved:

1. Operator runs coverage extraction queries (I'll provide the exact SQL — five queries per entity × six entities), pastes results
2. I convert pastes to `tests/fixtures/canonical_coverage.py`
3. Operator confirms migration 088 is the next available number (`ls migrations/ | tail -5`) — verified at v0.2a time: 085, 086, 087 already applied, 088 is clear
4. Operator verifies `ANTHROPIC_API_KEY` is set in Railway env
5. Operator creates PAT for the approval flow, stores as `BASIS_ENGINE_GITHUB_PAT` env var
6. Operator provisions Slack webhook URL, stores as `BASIS_ENGINE_SLACK_WEBHOOK`
7. S0 CC prompt is drafted incorporating final decisions and launches

---

<!-- §10 -->

## 10. Changelog

### v0.2 — April 24, 2026

Schema-level fixes (12):

1. `EventWindow` gains `"baseline"` variant; `Signal` gains `baseline` list; model_validator enforces event-window mutual exclusivity against `Analysis.event_date`
2. `Interpretation.model_id` required (was Optional) — every interpretation names its producer
3. `MethodologyObservation` gains stable `id: UUID`; `FollowUp` links via `linked_methodology_observation_id: Optional[UUID]` instead of fragile list index
4. `AnalysisAccepted` model added for 202 response from `POST /analyze`
5. `AnalysisCreate` / `Analysis` split applied; read-path consumers never see `id=None`. Same split for `EngineEventCreate` / `EngineEvent`
6. `Analysis.status` default set to `"pending"` (aligns with §3 async contract); state machine documented on the field
7. `ThresholdType = Literal["score_drop", "score_spike", "coverage_lapse"]` added; `WatchlistEntry.threshold_type` uses it
8. `engine_llm_usage` table schema added to migration 088
9. `Analysis.superseded_by_id` added for symmetric revision-chain lookup
10. `Analysis.archived_at` added; makes unique-constraint-with-status-filter observable
11. `ConfidenceLevel` semantics documented inline (four levels: high / medium / low / insufficient, with `insufficient` reserved for engine-degraded paths)
12. `Interpretation.from_cache` + `generated_at` cache-hit semantics documented inline

Document-level fixes (8):

13. Model ID pinned: default `claude-sonnet-4-6`, declared as constant in `app/engine/config.py`
14. Migration 088 explicit table list (seven tables): engine_analyses, engine_artifacts, engine_events, engine_watchlist, engine_prompts, engine_interpretation_cache, engine_llm_usage
15. Two named templates (`SHAPE_NO_COVERAGE_MEMO`, `SHAPE_API_UNAVAILABLE`); no third template permitted in v1
16. Async runtime mechanism specified: `asyncio.create_task` + status machine in DB + reaper task for orphaned `"pending"` rows
17. Router aggregation pattern specified: per-component files + aggregating `app/engine/router.py` + single import in `app/server.py`
18. P2 timeline realistically 5–6 days, not 4; total engine timeline updated to 13–18 days
19. Explicit v1 isolation from `app/report.py`, `app/integrity.py`, `app/divergence.py`, `app/discovery.py`
20. Degraded-analysis operator visibility: C5 delivers `confidence="insufficient"` or `model_id` starting with `"template:"` analyses with `[engine-degraded]` Slack tag

### v0.1 — April 22, 2026

Initial Step 0 package. See prior document.

---

---

<!-- §11 -->

## 11. Engineering Follow-ups (Standing)

Findings surfaced during engine-adjacent work that are not blockers for the Step 0 package but belong in the engineering backlog. Each entry cites the context in which it was discovered, so the history is recoverable.

### 11.1 USDC SII collector skipped the 2026-04-24 cycle

- **Discovered:** v0.2a fixture extraction, 2026-04-24.
- **Observed:** USDC `scores.computed_at` = 2026-04-22; other SII stablecoins computed 2026-04-24. `days_since_last_record = 2` for USDC vs 0 for peers.
- **Interpretation:** Collector run on 2026-04-24 completed for other SII entities but skipped USDC. Cause unknown — could be transient API failure, filter misfire, or silent collector path that didn't raise. No evidence this is a standing problem; first observed during fixture pinning.
- **Impact on engine:** None blocking. Motivated the addition of `days_since_last_record` + `coverage_window_days` to `EntityCoverage` (see §1) so that binary `live` can be inspected with staleness context. USDC shows up correctly as "live=False, 2 days stale" rather than just "live=False."
- **Follow-up:** Investigate worker cycle logs for 2026-04-22 → 2026-04-24 window on USDC path. If a single-cycle skip with no recurrence, log and close. If recurring or correlated with specific conditions, file a proper incident in the existing ops tracker.
- **Owner:** operator, out-of-band from engine build.
- **Severity:** low (isolated, not customer-facing).

### 11.2 Coverage extraction Query 5 is broken and retired

- **Discovered:** v0.2a fixture extraction, 2026-04-24.
- **Observed:** `docs/analytic_engine_coverage_extraction.sql` Query 5 (adjacent-index negative space) returned `covers_entity=false` for indexes that do cover the entity. Reproduced on `drift` and `jupiter-perpetual-exchange`.
- **Interpretation:** Likely root cause in the `covering` CTE — a mix of `UNION` semantics, a stray `LIMIT 1` clipping multi-row matches in the `psi_scores` branch, and inconsistent handling of `historical_protocol_data` as a logical "psi" index. Not debugged.
- **Impact on engine:** None. Query 5 output was ignored for fixture extraction; `adjacent_indexes_not_covering` in `tests/fixtures/canonical_coverage.py` was populated manually from Q1+Q2+Q3 results.
- **Follow-up:** P1 (Component 1, Coverage endpoint) rewrites this logic from scratch in Python against a single authoritative index registry. When P1 lands, delete Query 5 from the extraction SQL.
- **Owner:** P1 session.
- **Severity:** none (tool-only; no production consumer).

---

*Document ends. v0.2 incorporates all review findings from the v0.1 pass. v0.2a adds the migration-number correction and the standing follow-ups section. Ready for operator approval.*
