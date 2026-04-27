"""
Component 3: incident_page renderer.

V9.6 evidence artifact. Public. Pinned snapshot of what Basis observed —
"this analysis describes signal patterns, not causation."

Only renderable when the recommendation derivation allows incident_page
(coverage_quality == "full-live"). The render endpoint enforces; this
module assumes the gate has been passed.

Produces a markdown body suitable for either:
  - A standalone .md file (suggested_path is illustrative, used by C5)
  - Conversion into an IncidentPage.jsx via the existing front-end build

Suggested_url is the canonical public path; C5 will publish into the
React app.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.engine.renderers._shared import (
    camel_case,
    event_date_str,
    render_observations_table,
    slugify,
)
from app.engine.schemas import Analysis


@dataclass
class RenderedArtifact:
    content_markdown: str
    suggested_path: Optional[str]
    suggested_url: Optional[str]


def render_incident_page(analysis: Analysis) -> RenderedArtifact:
    coverage = analysis.coverage
    interp = analysis.interpretation
    signal = analysis.signal

    entity = analysis.entity
    event_str = event_date_str(analysis.event_date)
    entity_camel = camel_case(entity)
    entity_slug = slugify(entity)
    date_slug = (
        analysis.event_date.isoformat() if analysis.event_date else "no-date"
    )

    pre_event_table = render_observations_table(
        signal.pre_event, max_rows=10, include_peer_delta=False
    )
    event_table = render_observations_table(
        signal.event_window, max_rows=10, include_peer_delta=True
    )

    pre_event_prose = (
        interp.pre_event_story
        or "Pre-event signal not available — coverage too sparse for this window."
    )
    event_prose = (
        interp.event_story
        or "Event window observations not available."
    )
    cross_peer_prose = (
        interp.cross_peer_reading
        or "No peer set provided for this analysis."
    )

    inputs_hash_short = (
        analysis.inputs_hash[:24] + "…"
        if len(analysis.inputs_hash) > 24
        else analysis.inputs_hash
    )

    body = f"""# {entity} Incident — {event_str}

**Signal observed by Basis Protocol indexes**

*This page is a pinned evidence artifact. It describes the data Basis observed, not the cause of any event. See the [V9.6 Constitution Amendment](https://basisprotocol.xyz/v9.6) for context.*

---

## 01 — What Happened

{interp.event_summary}

---

## 02 — What Basis Tracked Before {event_str}

{pre_event_prose}

### Pre-event measures

{pre_event_table}

---

## 03 — Signal During the Event Window

{event_prose}

### Event window measures

{event_table}

---

## 04 — Cross-peer Reading

{cross_peer_prose}

---

## 05 — What This Does Not Claim

{interp.what_this_does_not_claim}

---

## 06 — Coverage and Methodology

Coverage quality: **{coverage.coverage_quality}**

{coverage.coverage_summary}

Confidence: **{interp.confidence}**. {interp.confidence_reasoning}

---

*Generated {interp.generated_at.isoformat()} · Analysis hash: {inputs_hash_short} · Prompt version: {interp.prompt_version} · Model: {interp.model_id}*
"""

    suggested_path = (
        f"frontend/src/pages/Incident{entity_camel}{date_slug.replace('-', '')}.jsx"
    )
    suggested_url = f"/incident/{entity_slug}-{date_slug}"

    return RenderedArtifact(
        content_markdown=body,
        suggested_path=suggested_path,
        suggested_url=suggested_url,
    )
