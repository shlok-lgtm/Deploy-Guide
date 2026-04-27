"""
Component 3: case_study renderer.

Public, longer-form analytical piece. Less time-pressured than
incident_page — can use historical data and reflect on the event with
distance. Suitable for partial-live coverage; not gated to full-live
because case studies are explicitly about offering analytical breadth
beyond the pinned-evidence requirements of an incident_page.

Suggested_url is the canonical public path under /case-studies/. C5
will publish.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.engine.renderers._shared import (
    event_date_str,
    render_follow_ups,
    slugify,
)
from app.engine.schemas import Analysis


@dataclass
class RenderedArtifact:
    content_markdown: str
    suggested_path: Optional[str]
    suggested_url: Optional[str]


def render_case_study(analysis: Analysis) -> RenderedArtifact:
    coverage = analysis.coverage
    interp = analysis.interpretation

    entity = analysis.entity
    event_str = event_date_str(analysis.event_date)
    entity_slug = slugify(entity)
    date_slug = (
        analysis.event_date.isoformat() if analysis.event_date else "no-date"
    )

    pre_event_prose = interp.pre_event_story or "_No pre-event signal available._"
    event_prose = interp.event_story or "_No event-window signal available._"
    post_event_prose = interp.post_event_story or "_No post-event signal available._"
    cross_peer_prose = (
        interp.cross_peer_reading
        or "Single-entity analysis — no peer comparison."
    )

    follow_ups_md = render_follow_ups(analysis.follow_ups)

    body = f"""# Case Study: {entity} — {event_str}

{interp.headline}

---

## Summary

{interp.event_summary}

## What Basis Indexes Showed

### Before

{pre_event_prose}

### During

{event_prose}

### After

{post_event_prose}

## Cross-peer Reading

{cross_peer_prose}

## Methodology

{coverage.coverage_summary}

Coverage quality: **{coverage.coverage_quality}**.

## Caveats

{interp.what_this_does_not_claim}

## Follow-ups

{follow_ups_md}

---

*Confidence: {interp.confidence} · Generated {interp.generated_at.isoformat()} · Prompt version: {interp.prompt_version}*
"""

    suggested_path = (
        f"audits/case_studies/{entity_slug}_{date_slug}.md"
    )
    suggested_url = f"/case-studies/{entity_slug}-{date_slug}"

    return RenderedArtifact(
        content_markdown=body,
        suggested_path=suggested_path,
        suggested_url=suggested_url,
    )
