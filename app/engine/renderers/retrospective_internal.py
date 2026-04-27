"""
Component 3: retrospective_internal renderer.

Internal audit document. Admin-only — never published. Produced when
coverage is `partial-reconstructable` (Drift-style: backfilled PSI, no
live data at the time of the event). Acknowledges reconstruction
explicitly so a reader is never led to believe Basis observed the
signal in real time.

Suggested_url is null — this artifact never publishes. C5 stores under
audits/internal/.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.engine.renderers._shared import (
    event_date_str,
    render_follow_ups,
    render_methodology_observations,
    render_observations_section,
    slugify,
)
from app.engine.schemas import Analysis


@dataclass
class RenderedArtifact:
    content_markdown: str
    suggested_path: Optional[str]
    suggested_url: Optional[str]


def render_retrospective_internal(analysis: Analysis) -> RenderedArtifact:
    coverage = analysis.coverage
    interp = analysis.interpretation
    signal = analysis.signal

    entity = analysis.entity
    event_str = event_date_str(analysis.event_date)
    entity_slug = slugify(entity)
    date_slug = (
        analysis.event_date.isoformat() if analysis.event_date else "no-date"
    )

    # Coverage block reasons — list, formatted as bullets if multiple
    if coverage.blocks_reasons:
        block_reasons_md = "\n".join(f"- {r}" for r in coverage.blocks_reasons)
    else:
        block_reasons_md = (
            "_(no specific block reasons recorded — see coverage_summary)_"
        )

    pre_event_section = render_observations_section(
        signal.pre_event, label="Pre-event observations"
    )
    event_section = render_observations_section(
        signal.event_window, label="Event window observations"
    )
    post_event_section = render_observations_section(
        signal.post_event, label="Post-event observations"
    )

    pre_event_prose = interp.pre_event_story or "Sparse coverage — see methodology."
    event_prose = interp.event_story or "Limited event-window data."
    post_event_prose = interp.post_event_story or ""

    follow_ups_md = render_follow_ups(analysis.follow_ups)
    methodology_md = render_methodology_observations(analysis.methodology_observations)

    body = f"""# Retrospective: {entity} — {event_str}

**Internal audit. Not published. Reconstructed from temporal indexes.**

---

## Why this is a retrospective, not an incident page

Coverage quality: **{coverage.coverage_quality}**.

{block_reasons_md}

This document analyzes signal that was reconstructed via temporal indexes (e.g., PSI backfill) rather than observed live. It is not a V9.6 evidence artifact and should not be published as such.

---

## Signal Reconstruction

### Pre-event (reconstructed)

{pre_event_prose}

{pre_event_section}

### Event window

{event_prose}

{event_section}

### Post-event

{post_event_prose}

{post_event_section}

---

## What this analysis cannot claim

{interp.what_this_does_not_claim}

This document is a retrospective; Basis did not observe this signal in real time.

---

## Methodology

{coverage.coverage_summary}

Confidence: **{interp.confidence}** ({interp.confidence_reasoning})

Generated: {interp.generated_at.isoformat()}
Inputs hash: `{analysis.inputs_hash}`
Prompt version: `{interp.prompt_version}`
Model: `{interp.model_id}`

---

## Methodology Observations

{methodology_md}

---

## Follow-ups

{follow_ups_md}
"""

    suggested_path = (
        f"audits/internal/{entity_slug}_retrospective_{date_slug}.md"
    )
    suggested_url = None

    return RenderedArtifact(
        content_markdown=body,
        suggested_path=suggested_path,
        suggested_url=suggested_url,
    )
