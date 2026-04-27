"""
Component 3: Markdown rendering helpers shared across artifact renderers.

Pure functions that take Pydantic models from app.engine.schemas and return
markdown fragments (strings). No I/O. No template engine — plain f-strings
and join operations. Each renderer composes a final markdown document by
calling these helpers and slotting their output into the structural
template defined in the renderer module.

Design rules:
  - Every helper returns a string. Empty input → a placeholder string
    (e.g., "_None_" or "No observations.") rather than an empty string,
    so the surrounding template doesn't end up with awkward blank
    sections.
  - Numeric values get unit-aware formatting via format_metric_value().
  - Functions are deterministic and side-effect-free for testability.
"""

from __future__ import annotations

import math
from datetime import date
from typing import Iterable, Optional

from app.engine.schemas import (
    FollowUp,
    MethodologyObservation,
    Observation,
    Signal,
)


# ─────────────────────────────────────────────────────────────────
# Slug + entity-name helpers (used by suggested_path / suggested_url)
# ─────────────────────────────────────────────────────────────────

def slugify(text: str) -> str:
    """Lowercase, dash-separated, alphanumeric only. Used in URLs and
    file paths."""
    out: list[str] = []
    prev_dash = True
    for ch in text.lower():
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        elif not prev_dash:
            out.append("-")
            prev_dash = True
    return "".join(out).strip("-")


def camel_case(text: str) -> str:
    """drift → Drift; jupiter-perpetual-exchange → JupiterPerpetualExchange.
    Used for suggested JSX component filenames."""
    parts = []
    for chunk in text.replace("_", " ").replace("-", " ").split():
        if chunk:
            parts.append(chunk[:1].upper() + chunk[1:].lower())
    return "".join(parts) or "Entity"


def event_date_str(event_date: Optional[date]) -> str:
    """Human-readable event date for headlines. None → 'no event date'."""
    return event_date.isoformat() if event_date is not None else "no event date"


# ─────────────────────────────────────────────────────────────────
# Value formatting — unit-aware
# ─────────────────────────────────────────────────────────────────

def format_metric_value(value: Optional[float], unit: str) -> str:
    """Format a raw metric value according to its declared unit. Returns
    an empty placeholder ('_n/a_') for None/NaN inputs so the markdown
    table still renders cleanly."""
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "_n/a_"

    if unit == "usd":
        return _format_usd(value)
    if unit in ("pct", "percent"):
        return f"{value:.2f}%"
    if unit == "score_0_100":
        return f"{value:.1f}"
    if unit == "ratio_0_1":
        return f"{value:.3f}"
    if unit == "ratio":
        return f"{value:.3f}"
    if unit == "count":
        return f"{int(value)}" if float(value).is_integer() else f"{value:.0f}"
    if unit == "days":
        return f"{value:.0f} days"
    if unit == "boolean":
        return "true" if value else "false"
    # Unknown unit — print value as-is with reasonable precision
    if isinstance(value, float) and abs(value) >= 1000:
        return f"{value:,.2f}"
    return f"{value:.4g}" if isinstance(value, float) else str(value)


def _format_usd(value: float) -> str:
    av = abs(value)
    sign = "-" if value < 0 else ""
    if av >= 1e9:
        return f"{sign}${av / 1e9:.2f}B"
    if av >= 1e6:
        return f"{sign}${av / 1e6:.2f}M"
    if av >= 1e3:
        return f"{sign}${av / 1e3:.1f}K"
    return f"{sign}${av:.2f}"


# ─────────────────────────────────────────────────────────────────
# Observation tables
# ─────────────────────────────────────────────────────────────────

_ANOMALY_MARK = "🚨"


def _anomaly_cell(obs: Observation) -> str:
    if not obs.is_anomaly:
        return ""
    if obs.anomaly_z_score is None:
        return _ANOMALY_MARK
    return f"{_ANOMALY_MARK} z={obs.anomaly_z_score:+.2f}"


def _peer_delta_cell(obs: Observation) -> str:
    if obs.peer_divergence_magnitude is None:
        return ""
    return format_metric_value(obs.peer_divergence_magnitude, obs.unit)


def render_observations_table(
    observations: Iterable[Observation],
    *,
    max_rows: int = 10,
    include_peer_delta: bool = False,
    empty_placeholder: str = "_No observations._",
) -> str:
    """GitHub-flavored markdown table of observations. When `max_rows` is
    exceeded, the remainder is collapsed into a "+ N more" summary row.
    """
    rows = list(observations)
    if not rows:
        return empty_placeholder

    truncated = rows[:max_rows]
    remainder = len(rows) - len(truncated)

    if include_peer_delta:
        header = "| Index | Measure | Value | Anomaly | Peer Δ |"
        sep = "|---|---|---|---|---|"
    else:
        header = "| Index | Measure | Value | Anomaly |"
        sep = "|---|---|---|---|"

    lines = [header, sep]
    for o in truncated:
        cells = [
            o.index_id,
            o.measure,
            format_metric_value(o.metric_value, o.unit),
            _anomaly_cell(o),
        ]
        if include_peer_delta:
            cells.append(_peer_delta_cell(o))
        lines.append("| " + " | ".join(cells) + " |")

    if remainder > 0:
        more_cells = ["…", f"+ {remainder} more observations", "", ""]
        if include_peer_delta:
            more_cells.append("")
        lines.append("| " + " | ".join(more_cells) + " |")

    return "\n".join(lines)


def render_observations_section(
    observations: Iterable[Observation],
    *,
    label: str,
    max_rows: int = 15,
) -> str:
    """A bolded label + observations table. Used in retrospective renderers
    where each window gets its own subsection."""
    table = render_observations_table(observations, max_rows=max_rows)
    return f"**{label}**\n\n{table}"


# ─────────────────────────────────────────────────────────────────
# Follow-ups, methodology, coverage gaps
# ─────────────────────────────────────────────────────────────────

def render_follow_ups(
    follow_ups: Iterable[FollowUp],
    *,
    empty_placeholder: str = "_No follow-ups generated for this analysis._",
) -> str:
    items = list(follow_ups)
    if not items:
        return empty_placeholder
    lines = []
    for f in items:
        affected = ", ".join(f.affected_indexes) if f.affected_indexes else "none"
        lines.append(
            f"- **[{f.priority}]** ({f.follow_up_type}) {f.description} "
            f"(_affects: {affected}; effort: {f.estimated_effort}_)"
        )
    return "\n".join(lines)


def render_methodology_observations(
    observations: Iterable[MethodologyObservation],
    *,
    empty_placeholder: str = "_No methodology issues flagged._",
) -> str:
    items = list(observations)
    if not items:
        return empty_placeholder
    lines = []
    for m in items:
        affected = ", ".join(m.affected_indexes) if m.affected_indexes else "none"
        lines.append(
            f"- **[{m.severity}]** ({m.observation_type}) {m.finding} "
            f"(_indexes: {affected}; evidence: {m.evidence}_)"
        )
    return "\n".join(lines)


def render_coverage_gaps(adjacent_indexes_not_covering: Iterable[str]) -> str:
    items = list(adjacent_indexes_not_covering)
    if not items:
        return "- _No adjacent index gaps identified._"
    return "\n".join(f"- `{ix}` (no coverage)" for ix in items)


# ─────────────────────────────────────────────────────────────────
# Compact / summary helpers (used by short artifacts)
# ─────────────────────────────────────────────────────────────────

def _all_observations(signal: Signal) -> list[Observation]:
    return [
        *signal.baseline,
        *signal.pre_event,
        *signal.event_window,
        *signal.post_event,
    ]


def render_compact_observation_summary(signal: Signal) -> str:
    """Brief bullet-list summary of observations across all windows.
    Used by internal_memo. Reports counts per window and the most
    recent anomaly (if any)."""
    counts = {
        "baseline": len(signal.baseline),
        "pre_event": len(signal.pre_event),
        "event_window": len(signal.event_window),
        "post_event": len(signal.post_event),
    }
    populated = [(k, v) for k, v in counts.items() if v > 0]
    if not populated:
        return "_No observations across any window._"

    lines = [f"- {window}: {count} observation{'s' if count != 1 else ''}"
             for window, count in populated]

    # Surface the most recent anomaly, if any
    all_obs = _all_observations(signal)
    anomalies = [o for o in all_obs if o.is_anomaly and o.at_date is not None]
    if anomalies:
        latest_anomaly = max(anomalies, key=lambda o: o.at_date or date.min)
        lines.append(
            f"- Latest anomaly: `{latest_anomaly.measure}` "
            f"(z={latest_anomaly.anomaly_z_score:+.2f}) on "
            f"{latest_anomaly.at_date.isoformat()}"
        )
    return "\n".join(lines)


def render_top_3_observation_stats(signal: Signal) -> str:
    """Three most-notable observations across all windows, formatted as
    bullet stats. Used by talking_points. Notability ordered by:
      1. Largest absolute anomaly z-score (if any anomalies present)
      2. Largest absolute peer divergence (if any peer comparisons)
      3. Most-recent observation
    """
    all_obs = _all_observations(signal)
    if not all_obs:
        return "- _No observations to summarize._"

    seen: set[tuple[str, str]] = set()  # (index_id, measure) — dedup
    picks: list[Observation] = []

    # 1. Top anomalies
    anomalies = sorted(
        (o for o in all_obs if o.is_anomaly and o.anomaly_z_score is not None),
        key=lambda o: abs(o.anomaly_z_score or 0.0),
        reverse=True,
    )
    for o in anomalies:
        key = (o.index_id, o.measure)
        if key in seen:
            continue
        picks.append(o)
        seen.add(key)
        if len(picks) >= 3:
            break

    # 2. Top peer divergences
    if len(picks) < 3:
        divergences = sorted(
            (o for o in all_obs if o.peer_divergence_magnitude is not None),
            key=lambda o: abs(o.peer_divergence_magnitude or 0.0),
            reverse=True,
        )
        for o in divergences:
            key = (o.index_id, o.measure)
            if key in seen:
                continue
            picks.append(o)
            seen.add(key)
            if len(picks) >= 3:
                break

    # 3. Most recent observations as fallback
    if len(picks) < 3:
        recents = sorted(
            (o for o in all_obs if o.at_date is not None),
            key=lambda o: o.at_date or date.min,
            reverse=True,
        )
        for o in recents:
            key = (o.index_id, o.measure)
            if key in seen:
                continue
            picks.append(o)
            seen.add(key)
            if len(picks) >= 3:
                break

    lines = []
    for o in picks[:3]:
        prose = (
            f"- **{o.index_id}.{o.measure}**: "
            f"{format_metric_value(o.metric_value, o.unit)}"
        )
        if o.is_anomaly and o.anomaly_z_score is not None:
            prose += f" (z={o.anomaly_z_score:+.2f})"
        if o.peer_divergence_magnitude is not None:
            prose += (
                f" (peer Δ {format_metric_value(o.peer_divergence_magnitude, o.unit)})"
            )
        if o.at_date is not None:
            prose += f" on {o.at_date.isoformat()}"
        lines.append(prose)
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────
# Prose-shaping helpers (deterministic, no LLM)
# ─────────────────────────────────────────────────────────────────

def first_sentence(text: Optional[str], *, fallback: str = "") -> str:
    """Extract the first sentence from prose. Used by talking_points and
    one_pager to compress LLM-generated paragraphs into briefer forms."""
    if text is None or not text.strip():
        return fallback
    # Find the first sentence-terminating punctuation followed by space or end
    cleaned = text.strip()
    # Look for end of first sentence — prefer ". ", "! ", "? "
    for terminator in (". ", "! ", "? "):
        idx = cleaned.find(terminator)
        if idx > 0:
            return cleaned[: idx + 1]
    # Fall back to single-line trimming
    nl_idx = cleaned.find("\n")
    if nl_idx > 0:
        return cleaned[:nl_idx]
    return cleaned


def first_paragraph(text: Optional[str], *, fallback: str = "") -> str:
    """First paragraph (split on double-newline). Used by one_pager."""
    if text is None or not text.strip():
        return fallback
    paragraphs = text.strip().split("\n\n")
    return paragraphs[0].strip() if paragraphs else fallback


def truncate_words(text: Optional[str], max_words: int, *, suffix: str = "…") -> str:
    """Cap a string to max_words. Used to enforce length budgets on
    short-form artifacts."""
    if text is None or not text.strip():
        return ""
    words = text.split()
    if len(words) <= max_words:
        return text.strip()
    return " ".join(words[:max_words]) + suffix
