"""
Component 3: one_pager renderer.

Short executive summary, ≤300 words target. Public-eligible — for sharing
with external stakeholders (investors, partners, regulators) who need a
crisp version of the case_study without the full analytical depth.

Suggested_url is the canonical public path. C5 will publish into the
React app at /one-pagers/.

Word budget enforced via _shared.truncate_words on the longest prose
fields. The structural template is deliberately compact.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.engine.renderers._shared import (
    event_date_str,
    first_paragraph,
    first_sentence,
    slugify,
    truncate_words,
)
from app.engine.schemas import Analysis


@dataclass
class RenderedArtifact:
    content_markdown: str
    suggested_path: Optional[str]
    suggested_url: Optional[str]


def render_one_pager(analysis: Analysis) -> RenderedArtifact:
    coverage = analysis.coverage
    interp = analysis.interpretation

    entity = analysis.entity
    event_str = event_date_str(analysis.event_date)
    entity_slug = slugify(entity)
    date_slug = (
        analysis.event_date.isoformat() if analysis.event_date else "no-date"
    )

    matched_count = len(coverage.matched_entities)

    summary = truncate_words(interp.event_summary, max_words=80)
    pre_event_para = first_paragraph(interp.pre_event_story, fallback="")
    event_para = first_paragraph(interp.event_story, fallback="")
    not_claim_one = first_sentence(
        interp.what_this_does_not_claim,
        fallback="This analysis describes signal patterns, not causation.",
    )

    # Compose the "What Basis observed" section deterministically — only
    # include paragraphs that are non-empty so the artifact stays compact.
    observed_parts: list[str] = []
    if pre_event_para:
        observed_parts.append(truncate_words(pre_event_para, max_words=60))
    if event_para:
        observed_parts.append(truncate_words(event_para, max_words=60))
    observed_section = (
        "\n\n".join(observed_parts) if observed_parts
        else "_No detailed signal commentary available._"
    )

    body = f"""# {entity} — {event_str}

{interp.headline}

---

## What happened

{summary}

## What Basis observed

{observed_section}

## Methodology

Coverage: **{coverage.coverage_quality}** across {matched_count} index{'es' if matched_count != 1 else ''}.

## Limits

{not_claim_one}

---

*Confidence: {interp.confidence} · {interp.generated_at.isoformat()} · Prompt version: {interp.prompt_version}*
"""

    suggested_path = (
        f"audits/case_studies/one_pagers/{entity_slug}_{date_slug}_one_pager.md"
    )
    suggested_url = f"/one-pagers/{entity_slug}-{date_slug}"

    return RenderedArtifact(
        content_markdown=body,
        suggested_path=suggested_path,
        suggested_url=suggested_url,
    )
