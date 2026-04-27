"""
Component 3: internal_memo renderer.

Internal-only catch-all. Always allowed regardless of coverage quality
(unless coverage is "none" — in which case the recommendation logic
blocks all artifacts including this one). For minimal-coverage events
where the operator still wants to capture observations + follow-ups
on record.

Suggested_url is null — internal record-keeping, never published.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.engine.renderers._shared import (
    event_date_str,
    render_compact_observation_summary,
    render_coverage_gaps,
    render_follow_ups,
    render_methodology_observations,
    slugify,
)
from app.engine.schemas import Analysis


@dataclass
class RenderedArtifact:
    content_markdown: str
    suggested_path: Optional[str]
    suggested_url: Optional[str]


def render_internal_memo(analysis: Analysis) -> RenderedArtifact:
    coverage = analysis.coverage
    interp = analysis.interpretation
    signal = analysis.signal

    entity = analysis.entity
    event_str = event_date_str(analysis.event_date)
    entity_slug = slugify(entity)
    date_slug = (
        analysis.event_date.isoformat() if analysis.event_date else "no-date"
    )

    coverage_gaps_md = render_coverage_gaps(coverage.adjacent_indexes_not_covering)
    observations_md = render_compact_observation_summary(signal)
    follow_ups_md = render_follow_ups(analysis.follow_ups)
    methodology_md = render_methodology_observations(analysis.methodology_observations)

    body = f"""# Internal Memo: {entity} — {event_str}

*Internal observations and follow-ups. Not for publication.*

---

## What we know

{interp.event_summary}

## What we don't know

{interp.what_this_does_not_claim}

Coverage gaps:

{coverage_gaps_md}

## Observations

{observations_md}

## Follow-ups

{follow_ups_md}

## Methodology notes

{methodology_md}

---

*Confidence: {interp.confidence}. {interp.confidence_reasoning}*

*Generated {interp.generated_at.isoformat()} · Prompt version: {interp.prompt_version} · Model: {interp.model_id}*
"""

    suggested_path = (
        f"audits/internal/memos/{entity_slug}_{date_slug}_memo.md"
    )
    suggested_url = None

    return RenderedArtifact(
        content_markdown=body,
        suggested_path=suggested_path,
        suggested_url=suggested_url,
    )
