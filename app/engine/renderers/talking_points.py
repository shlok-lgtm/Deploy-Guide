"""
Component 3: talking_points renderer.

Internal briefing artifact. Bullet-style, ≤500 words. Used during press
inquiries, partner calls, governance discussions. Includes explicit
"What NOT to say" guardrails so the speaker doesn't drift into causation
claims that would violate V9.6.

Deterministic shaping only — no LLM. Pulls one-sentence summaries from
the LLM-generated prose via app.engine.renderers._shared.first_sentence.

Suggested_url is null — internal use only.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.engine.renderers._shared import (
    event_date_str,
    first_sentence,
    render_top_3_observation_stats,
    slugify,
)
from app.engine.schemas import Analysis


@dataclass
class RenderedArtifact:
    content_markdown: str
    suggested_path: Optional[str]
    suggested_url: Optional[str]


def render_talking_points(analysis: Analysis) -> RenderedArtifact:
    coverage = analysis.coverage
    interp = analysis.interpretation
    signal = analysis.signal

    entity = analysis.entity
    event_str = event_date_str(analysis.event_date)
    entity_slug = slugify(entity)
    date_slug = (
        analysis.event_date.isoformat() if analysis.event_date else "no-date"
    )

    pre_event_one = first_sentence(
        interp.pre_event_story,
        fallback="No pre-event signal recorded.",
    )
    event_one = first_sentence(
        interp.event_story,
        fallback="No event-window signal recorded.",
    )
    cross_peer_one = first_sentence(
        interp.cross_peer_reading,
        fallback="Single-entity analysis — no peer comparison.",
    )

    not_claim_one = first_sentence(
        interp.what_this_does_not_claim,
        fallback="This analysis describes signal patterns, not causation.",
    )
    confidence_one = first_sentence(
        interp.confidence_reasoning,
        fallback=f"Confidence rated {interp.confidence}.",
    )

    matched_count = len(coverage.matched_entities)

    stats_md = render_top_3_observation_stats(signal)

    body = f"""# Talking Points: {entity} — {event_str}

*Internal briefing. Use for press inquiries, partner calls, or governance discussions.*

---

## What happened

- {first_sentence(interp.event_summary, fallback=interp.event_summary)}

## What Basis observed

- Coverage: **{coverage.coverage_quality}** ({matched_count} index{'es' if matched_count != 1 else ''} tracked)
- Pre-event signal: {pre_event_one}
- Event window: {event_one}
- Cross-peer: {cross_peer_one}

## What to say if asked

- "{not_claim_one}"
- "Confidence is {interp.confidence} — {confidence_one}"
- "We tracked {matched_count} index{'es' if matched_count != 1 else ''} for this entity."

## What NOT to say

- We did not predict or anticipate the event.
- We do not claim causation.
- We did not influence any outcome.

## Stats to remember

{stats_md}

---

*Generated {interp.generated_at.isoformat()} · Prompt version: {interp.prompt_version}*
"""

    suggested_path = (
        f"audits/internal/talking_points/{entity_slug}_{date_slug}.md"
    )
    suggested_url = None

    return RenderedArtifact(
        content_markdown=body,
        suggested_path=suggested_path,
        suggested_url=suggested_url,
    )
