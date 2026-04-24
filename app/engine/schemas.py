"""
Basis Analytic Engine — canonical schemas.

These models are the contract for every engine endpoint, renderer, and
storage row. Breaking changes require a schema-amendment commit and
invalidate cached analyses (see docs/analytic_engine_step_0_v0.2.md §3).

Pre-S0 scaffold: this file lands with the v0.2 Step 0 package so that
fixtures in tests/fixtures/canonical_coverage.py can import the models
without waiting for S0 to run. S0 may add additional models (e.g.,
PromptRegistryEntry, LLMUsageRow) but must not modify the models
defined here without an amendment commit.
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

# EventWindow includes "baseline" for analyses with no event_date.
# Invariant (enforced by AnalysisCreate.model_validator):
#   event_date is None       ↔ only `baseline` populated
#   event_date is not None   ↔ only pre_event / event_window / post_event populated
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

    # Staleness fields (added post-extraction review).
    #
    # `live` is a binary derived from days_since_last_record (<= 2 = live).
    # On its own it can't distinguish "2 days stale" from "6 months stale,"
    # which matters for operator triage: a recently-stale collector is a
    # separate problem from a long-dormant one. The USDC case during v0.2a
    # fixture extraction surfaced this — USDC was `live=False` at 2 days
    # stale while other SII stablecoins were fresh that day (collector
    # skipped USDC). See Step 0 doc §11 for the standing follow-up.
    days_since_last_record: Optional[int] = None   # None if unique_days == 0
    coverage_window_days: Optional[int] = None     # latest_record - earliest_record in days

    data_source: str  # "generic_index_scores" | "historical_protocol_data" | "scores" | etc.
    available_endpoints: list[str] = Field(default_factory=list)

class RelatedEntity(BaseModel):
    """Entity adjacent to the queried one — suggestion only, not a declaration
    that Analysis should use it as a peer. Peer sets are explicit per
    docs/analytic_engine_step_0_v0.2.md §2."""
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
        description="REQUIRED per Step 0 doc §2. Empty list is permitted; it "
                    "triggers a methodology observation flagging the missing "
                    "peer set and bumps Interpretation.confidence down one level."
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
        description="REQUIRED. Empty list permitted; see Step 0 doc §2."
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
