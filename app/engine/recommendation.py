"""
Component 3: ArtifactRecommendation derivation.

Replaces the S2a stub. Maps coverage_quality + interpretation.confidence
into a (recommended, supports, blocked) triple per Step 0 §2.4.

V9.6 constitutional rules baked into the blocked list — incident_page
specifically cannot be produced when coverage is reconstructed
(backfilled) rather than live observed. No `force=true` path overrides
this; the render endpoint enforces.

Decision flow:
    1. Look up base recommendations by coverage_quality.
    2. If interpretation.confidence == "insufficient", restrict to
       internal_memo only — record-keeping, not analytical claims.
    3. Otherwise return the base.

Pure function: no DB, no I/O. Called from app.engine.analysis at
analysis-build time so the recommendation reflects the same coverage
+ confidence the analysis row will persist.
"""

from __future__ import annotations

from app.engine.schemas import (
    AnalysisType,
    ArtifactRecommendation,
    CoverageResponse,
    Interpretation,
    Signal,
)

# Coverage-quality → artifact decisions.
#
# `recommended`: artifacts the engine actively recommends; render endpoint
#                 lets these through unconditionally.
# `supports`:    not actively recommended but renderable with force=true.
#                 Empty in v1 — operator either uses recommended or hits
#                 a constitutional block.
# `blocked`:     V9.6-constitutional disallows; force=true cannot override.

_BASE_BY_COVERAGE_QUALITY: dict[str, dict] = {
    "full-live": {
        "recommended": [
            "incident_page",
            "retrospective_internal",
            "case_study",
            "internal_memo",
            "talking_points",
            "one_pager",
        ],
        "supports": [],
        "blocked": [],
        "blocked_reason": "",
    },
    "partial-live": {
        "recommended": [
            "retrospective_internal",
            "case_study",
            "internal_memo",
            "talking_points",
            "one_pager",
        ],
        "supports": [],
        "blocked": ["incident_page"],
        "blocked_reason": (
            "Live indexes are sparse and not sufficient to support an "
            "incident page (V9.6 evidence artifact requirements)."
        ),
    },
    "partial-reconstructable": {
        "recommended": ["retrospective_internal", "case_study", "internal_memo"],
        "supports": [],
        "blocked": ["incident_page", "talking_points", "one_pager"],
        "blocked_reason": (
            "Coverage is reconstructed (backfilled), not live-observed. "
            "Cannot support pinned-evidence or short-form public artifacts "
            "per V9.6 — these require live signal at the time of the event."
        ),
    },
    "sparse": {
        "recommended": ["internal_memo"],
        "supports": [],
        "blocked": [
            "incident_page",
            "retrospective_internal",
            "case_study",
            "talking_points",
            "one_pager",
        ],
        "blocked_reason": (
            "Coverage too thin for meaningful analytical artifacts. Only "
            "internal record-keeping is appropriate."
        ),
    },
    "none": {
        "recommended": [],
        "supports": [],
        "blocked": [
            "incident_page",
            "retrospective_internal",
            "case_study",
            "internal_memo",
            "talking_points",
            "one_pager",
        ],
        "blocked_reason": (
            "No coverage. No analysis artifacts can be produced for an "
            "entity Basis does not track."
        ),
    },
}


def derive_recommendation(
    coverage: CoverageResponse,
    signal: Signal,  # accepted for forward-compat; not yet used in v1
    interpretation: Interpretation,
) -> ArtifactRecommendation:
    """Return the ArtifactRecommendation for the given analysis inputs.

    `signal` is accepted but not consulted in v1 — the recommendation
    decision is derived from coverage_quality + interpretation.confidence.
    Future versions may use signal density (how many observations
    populated each window) to refine which artifacts are appropriate.
    """
    base = _BASE_BY_COVERAGE_QUALITY.get(coverage.coverage_quality)
    if base is None:
        # Defensive — every CoverageQuality literal is in the table above
        return ArtifactRecommendation(
            recommended="nothing",
            supports=[],
            reasoning=(
                f"Unknown coverage_quality {coverage.coverage_quality!r}; "
                "treating as no recommendation."
            ),
            blocked=[
                "incident_page", "retrospective_internal", "case_study",
                "internal_memo", "talking_points", "one_pager",
            ],
            blocked_reasons=["Unknown coverage_quality."],
        )

    recommended_list: list[str] = list(base["recommended"])
    blocked_list: list[str] = list(base["blocked"])
    blocked_reasons: list[str] = (
        [base["blocked_reason"]] if base.get("blocked_reason") else []
    )
    confidence_note = ""

    # Insufficient confidence floors us to record-keeping only.
    if interpretation.confidence == "insufficient":
        # Move every non-internal_memo artifact from recommended into blocked
        kept_recommended: list[str] = []
        added_blocked: list[str] = []
        for art in recommended_list:
            if art == "internal_memo":
                kept_recommended.append(art)
            else:
                added_blocked.append(art)
        recommended_list = kept_recommended
        for art in added_blocked:
            if art not in blocked_list:
                blocked_list.append(art)
        if added_blocked:
            blocked_reasons.append(
                "Confidence is insufficient — only internal_memo "
                "recommended for record-keeping."
            )
        confidence_note = " Confidence insufficient — restricted to internal_memo."

    # Pick the primary recommendation (first in recommended_list, or "nothing")
    primary: AnalysisType = (
        recommended_list[0] if recommended_list else "nothing"  # type: ignore[assignment]
    )

    reasoning = (
        f"Coverage quality: {coverage.coverage_quality}. "
        f"Confidence: {interpretation.confidence}.{confidence_note}"
    )

    return ArtifactRecommendation(
        recommended=primary,
        supports=recommended_list,  # full set caller can render unconditionally
        reasoning=reasoning,
        blocked=blocked_list,
        blocked_reasons=blocked_reasons,
    )
