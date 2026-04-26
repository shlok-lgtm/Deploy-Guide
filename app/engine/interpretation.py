"""
Component 2c: LLM interpretation generation.

Replaces the S2a/S2b stub interpretation with real LLM-generated structured
output. End-to-end flow:

    coverage + signal
        ↓
    canonicalize → SHA-256 input_hash (includes prompt_version)
        ↓
    engine_interpretation_cache.get(input_hash)
        ├─ hit  → return cached Interpretation (from_cache=True, hit_count++)
        └─ miss
            ├─ cost_tracker.can_make_call() → False → SHAPE_API_UNAVAILABLE
            └─ True
                ├─ client.messages.parse() with output_format=Pydantic
                │   ├─ Pydantic-validated structured output, no parse logic
                │   ├─ stop_reason="refusal" → SHAPE_API_UNAVAILABLE (refusal)
                │   └─ APIError → SHAPE_API_UNAVAILABLE (api_error)
                └─ success → INSERT cache row, return Interpretation

Design notes:

- Sync, matches the rest of the engine pipeline. anthropic.Anthropic() (the
  sync client). build_stub_analysis stays sync; analyze_router unchanged.
- Model is pinned in MODEL_ID; bumping it requires a code change so every
  Analysis records exactly which model produced its interpretation.
- No prompt caching. Custom hash cache covers identical re-requests. The
  static prompt prefix is likely under Sonnet 4.6's 2048-token cache
  minimum and would only marginally help on partial-overlap calls.
- The Anthropic SDK auto-retries 429/5xx with exponential backoff
  (default max_retries=2). We don't add custom retry — failures bubble
  to the fallback path.
- Fallback responses are NEVER cached. The whole point is that a transient
  failure shouldn't poison the cache for future analyses with the same
  inputs. force_new=true on /analyze plus a fresh API call recovers.
- Prompt v1 is registered into engine_prompts lazily on first call (idempotent
  via ON CONFLICT DO NOTHING). No server.py startup hook needed.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import psycopg2.extras

# anthropic SDK and Pydantic — anthropic must be present in the runtime
# image. Tests that mock the SDK don't import it directly; integration
# tests skip cleanly without ANTHROPIC_API_KEY in env.
try:
    import anthropic
    _ANTHROPIC_AVAILABLE = True
except ImportError:
    _ANTHROPIC_AVAILABLE = False

from pydantic import BaseModel, Field
from typing import Literal

from app.database import execute, fetch_one, get_cursor
from app.engine import cost_tracker
from app.engine.schemas import (
    ConfidenceLevel,
    CoverageResponse,
    Interpretation,
    Signal,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# Constants pinned on every cache row
# ─────────────────────────────────────────────────────────────────

ACTIVE_PROMPT_VERSION = "v1"
MODEL_ID = "claude-sonnet-4-6"
MAX_TOKENS = 2000  # response budget for the structured JSON
TEMPERATURE = 0  # determinism: identical inputs → identical outputs

PROMPT_FILE = Path(__file__).parent / "prompts" / "interpretation_prompt_v1.md"
FALLBACK_MODEL_ID = "template:fallback"
FALLBACK_PROMPT_VERSION = "fallback"


# ─────────────────────────────────────────────────────────────────
# Pydantic schema enforced on the LLM response via messages.parse()
#
# Distinct from the canonical Interpretation model: this is just the
# CONTENT fields the model generates. Engine-side metadata (input_hash,
# model_id, generated_at, from_cache) is filled in afterward.
# ─────────────────────────────────────────────────────────────────

class LLMInterpretationOutput(BaseModel):
    event_summary: str
    pre_event_story: Optional[str] = None
    event_story: Optional[str] = None
    post_event_story: Optional[str] = None
    cross_peer_reading: Optional[str] = None
    what_this_does_not_claim: str
    headline: str
    confidence: Literal["high", "medium", "low", "insufficient"]
    confidence_reasoning: str


# ─────────────────────────────────────────────────────────────────
# Canonicalization for the input hash
#
# The hash must be stable across calls with identical semantic inputs.
# Sort everything that has natural ordering (peer_set, observations within
# a window). Float values are passed through json.dumps which has stable
# representation for finite floats.
# ─────────────────────────────────────────────────────────────────

def _sort_observations(observations: list) -> list[dict]:
    serialized = [o.model_dump(mode="json", exclude_none=False) for o in observations]
    return sorted(
        serialized,
        key=lambda o: (
            o.get("window") or "",
            o.get("index_id") or "",
            o.get("measure") or "",
            o.get("kind") or "",
            o.get("at_date") or "",
        ),
    )


def _canonicalize_signal(signal: Signal) -> dict:
    return {
        "baseline": _sort_observations(signal.baseline),
        "pre_event": _sort_observations(signal.pre_event),
        "event_window": _sort_observations(signal.event_window),
        "post_event": _sort_observations(signal.post_event),
    }


def compute_inputs_hash(
    *,
    entity: str,
    event_date: Optional[date],
    peer_set: list[str],
    coverage_snapshot_hash: str,
    signal: Signal,
    prompt_version: str = ACTIVE_PROMPT_VERSION,
) -> str:
    """SHA-256 over canonicalized inputs. Identical inputs (including
    prompt_version) produce identical hashes; bumping the prompt version
    invalidates all old cache entries by construction."""
    canonical = {
        "entity": entity,
        "event_date": event_date.isoformat() if event_date else None,
        "peer_set": sorted(peer_set),
        "coverage_snapshot_hash": coverage_snapshot_hash,
        "signal": _canonicalize_signal(signal),
        "prompt_version": prompt_version,
    }
    encoded = json.dumps(canonical, sort_keys=True).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


# ─────────────────────────────────────────────────────────────────
# Lazy prompt registration into engine_prompts
#
# Idempotent via ON CONFLICT DO NOTHING. We don't need a startup hook —
# the first analyze call registers the prompt; every subsequent call is
# a no-op INSERT. Module-level threading.Lock guards against the brief
# window where two concurrent first-calls might race; the DB-level
# constraint is the actual correctness backstop.
# ─────────────────────────────────────────────────────────────────

_prompt_registered = False
_prompt_lock = threading.Lock()


def _read_prompt_file() -> str:
    return PROMPT_FILE.read_text(encoding="utf-8")


def _ensure_prompt_registered() -> None:
    global _prompt_registered
    if _prompt_registered:
        return
    with _prompt_lock:
        if _prompt_registered:
            return
        try:
            content = _read_prompt_file()
        except FileNotFoundError:
            logger.error(
                "interpretation: prompt file missing at %s — fallback only",
                PROMPT_FILE,
            )
            return
        sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
        try:
            execute(
                """
                INSERT INTO engine_prompts (version, file_path, sha256, notes)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (version) DO NOTHING
                """,
                (
                    ACTIVE_PROMPT_VERSION,
                    str(PROMPT_FILE.relative_to(Path(__file__).parent.parent.parent)),
                    sha,
                    "Auto-registered on first interpretation call",
                ),
            )
            _prompt_registered = True
            logger.info(
                "interpretation: registered prompt %s (sha256=%s)",
                ACTIVE_PROMPT_VERSION, sha[:12],
            )
        except Exception as exc:  # pragma: no cover — DB unreachable path
            logger.exception(
                "interpretation: failed to register prompt %s: %s",
                ACTIVE_PROMPT_VERSION, exc,
            )


# ─────────────────────────────────────────────────────────────────
# Cache lookup + write
# ─────────────────────────────────────────────────────────────────

def _cache_get(input_hash: str) -> Optional[dict]:
    row = fetch_one(
        """
        SELECT interpretation_json, prompt_version, model_id, created_at
        FROM engine_interpretation_cache
        WHERE input_hash = %s
        """,
        (input_hash,),
    )
    if row is None:
        return None
    # Increment hit_count opportunistically; failure here is non-fatal
    try:
        execute(
            "UPDATE engine_interpretation_cache SET hit_count = hit_count + 1 WHERE input_hash = %s",
            (input_hash,),
        )
    except Exception as exc:  # pragma: no cover
        logger.warning("interpretation: hit_count update failed: %s", exc)
    return row


def _cache_put(
    input_hash: str,
    interpretation_payload: dict,
    *,
    input_tokens: int,
    output_tokens: int,
) -> None:
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO engine_interpretation_cache
              (input_hash, interpretation_json, prompt_version, model_id,
               token_input_count, token_output_count, hit_count)
            VALUES (%s, %s, %s, %s, %s, %s, 0)
            ON CONFLICT (input_hash) DO NOTHING
            """,
            (
                input_hash,
                psycopg2.extras.Json(interpretation_payload),
                ACTIVE_PROMPT_VERSION,
                MODEL_ID,
                input_tokens,
                output_tokens,
            ),
        )


# ─────────────────────────────────────────────────────────────────
# Fallback template — NOT cached
# ─────────────────────────────────────────────────────────────────

def _api_unavailable_template(
    *,
    inputs_hash: str,
    reason: str,
) -> Interpretation:
    return Interpretation(
        event_summary=f"Interpretation generation unavailable: {reason}.",
        pre_event_story=None,
        event_story=None,
        post_event_story=None,
        cross_peer_reading=None,
        what_this_does_not_claim=(
            "This is a fallback response generated because the LLM "
            "interpretation service was unavailable. No claims are made "
            "about the entity. Re-run analysis via force_new=true once the "
            "service is restored or the budget rolls over."
        ),
        headline="Interpretation unavailable — service degraded",
        confidence="insufficient",
        confidence_reasoning=(
            f"LLM service unavailable: {reason}. Signal data is still "
            "present on the analysis row; re-run interpretation when the "
            "service is restored."
        ),
        prompt_version=FALLBACK_PROMPT_VERSION,
        input_hash=inputs_hash,
        model_id=FALLBACK_MODEL_ID,
        generated_at=datetime.now(timezone.utc),
        from_cache=False,
    )


# ─────────────────────────────────────────────────────────────────
# Prompt rendering
#
# The prompt template uses single-brace {field} placeholders. The signal
# JSON we substitute in may contain literal curly braces (Pydantic dumps,
# nested objects). To avoid str.format misinterpretation we render with
# explicit string replacement on a fixed set of tokens.
# ─────────────────────────────────────────────────────────────────

def _signal_for_llm(signal: Signal) -> dict:
    """JSON-friendly view of a Signal for prompt embedding. Includes only
    populated lists — keeps the prompt focused."""
    out = {}
    for window in ("baseline", "pre_event", "event_window", "post_event"):
        observations = getattr(signal, window)
        if observations:
            out[window] = [o.model_dump(mode="json", exclude_none=True) for o in observations]
    return out


def _render_prompt(
    *,
    template: str,
    entity: str,
    event_date: Optional[date],
    peer_set: list[str],
    context: Optional[str],
    coverage: CoverageResponse,
    signal: Signal,
) -> str:
    fields = {
        "{entity}": entity,
        "{event_date}": event_date.isoformat() if event_date else "no event date specified",
        "{peer_set_json}": json.dumps(peer_set),
        "{context}": context or "no operator context",
        "{coverage_summary}": coverage.coverage_summary,
        "{coverage_quality}": coverage.coverage_quality,
        "{signal_json}": json.dumps(_signal_for_llm(signal), indent=2),
    }
    rendered = template
    for token, value in fields.items():
        rendered = rendered.replace(token, value)
    return rendered


# ─────────────────────────────────────────────────────────────────
# Anthropic API call — sync, structured output via messages.parse
# ─────────────────────────────────────────────────────────────────

def _call_anthropic(prompt: str) -> tuple[LLMInterpretationOutput, int, int]:
    """Returns (parsed_output, input_tokens, output_tokens). Raises
    anthropic.APIError on failure; the caller catches and falls back."""
    if not _ANTHROPIC_AVAILABLE:
        raise RuntimeError("anthropic package not installed")
    client = anthropic.Anthropic()
    response = client.messages.parse(
        model=MODEL_ID,
        max_tokens=MAX_TOKENS,
        temperature=TEMPERATURE,
        messages=[{"role": "user", "content": prompt}],
        output_format=LLMInterpretationOutput,
    )
    if response.stop_reason == "refusal":
        raise RuntimeError("model refusal")
    if response.parsed_output is None:
        raise RuntimeError(
            f"parse returned no parsed_output (stop_reason={response.stop_reason})"
        )
    in_tokens = getattr(response.usage, "input_tokens", 0) or 0
    out_tokens = getattr(response.usage, "output_tokens", 0) or 0
    return response.parsed_output, in_tokens, out_tokens


# ─────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────

def get_or_call_interpretation(
    *,
    entity: str,
    event_date: Optional[date],
    peer_set: list[str],
    coverage: CoverageResponse,
    signal: Signal,
    context: Optional[str] = None,
) -> Interpretation:
    """Return an Interpretation, hitting the cache, calling the API, or
    falling back to SHAPE_API_UNAVAILABLE — in that order.

    This function is sync. Callers in async handlers should already be
    inside a thread pool path (FastAPI runs sync route handlers there);
    if calling from explicit async code, wrap with asyncio.to_thread.
    """
    inputs_hash = compute_inputs_hash(
        entity=entity,
        event_date=event_date,
        peer_set=peer_set,
        coverage_snapshot_hash=coverage.data_snapshot_hash,
        signal=signal,
        prompt_version=ACTIVE_PROMPT_VERSION,
    )

    # 1. Cache hit — return immediately
    cached = _cache_get(inputs_hash)
    if cached is not None:
        payload = dict(cached["interpretation_json"])
        # Override metadata fields to reflect this serve, not the original
        payload["from_cache"] = True
        payload["input_hash"] = inputs_hash
        # Preserve original prompt_version + model_id from the cached row
        # — these are the producers of this interpretation regardless of
        # which call retrieves it
        payload.setdefault("prompt_version", cached["prompt_version"])
        payload.setdefault("model_id", cached["model_id"])
        return Interpretation.model_validate(payload)

    # 2. Budget gate — fall back if exhausted
    allowed, reason = cost_tracker.can_make_call()
    if not allowed:
        return _api_unavailable_template(
            inputs_hash=inputs_hash, reason=reason or "budget exhausted",
        )

    # 3. Ensure prompt v1 is registered before attempting cache write
    _ensure_prompt_registered()

    # 4. Call API; on failure return the fallback template (NOT cached)
    if not _ANTHROPIC_AVAILABLE:
        return _api_unavailable_template(
            inputs_hash=inputs_hash,
            reason="anthropic SDK not installed",
        )
    try:
        template = _read_prompt_file()
    except FileNotFoundError:
        return _api_unavailable_template(
            inputs_hash=inputs_hash,
            reason=f"prompt file missing at {PROMPT_FILE.name}",
        )
    prompt = _render_prompt(
        template=template,
        entity=entity,
        event_date=event_date,
        peer_set=peer_set,
        context=context,
        coverage=coverage,
        signal=signal,
    )
    try:
        parsed, in_tokens, out_tokens = _call_anthropic(prompt)
    except anthropic.APIError as exc:  # type: ignore[attr-defined]
        logger.error(
            "interpretation: Anthropic API failed (%s): %s",
            type(exc).__name__, exc,
        )
        return _api_unavailable_template(
            inputs_hash=inputs_hash,
            reason=f"API error: {type(exc).__name__}",
        )
    except Exception as exc:
        logger.exception("interpretation: unexpected error during API call")
        return _api_unavailable_template(
            inputs_hash=inputs_hash,
            reason=f"unexpected error: {type(exc).__name__}",
        )

    # 5. Build the Interpretation, persist to cache, return
    now = datetime.now(timezone.utc)
    interp = Interpretation(
        event_summary=parsed.event_summary,
        pre_event_story=parsed.pre_event_story,
        event_story=parsed.event_story,
        post_event_story=parsed.post_event_story,
        cross_peer_reading=parsed.cross_peer_reading,
        what_this_does_not_claim=parsed.what_this_does_not_claim,
        headline=parsed.headline,
        confidence=parsed.confidence,
        confidence_reasoning=parsed.confidence_reasoning,
        prompt_version=ACTIVE_PROMPT_VERSION,
        input_hash=inputs_hash,
        model_id=MODEL_ID,
        generated_at=now,
        from_cache=False,
    )

    try:
        _cache_put(
            inputs_hash,
            interp.model_dump(mode="json"),
            input_tokens=in_tokens,
            output_tokens=out_tokens,
        )
        cost_tracker.record_call(in_tokens, out_tokens)
    except Exception as exc:  # pragma: no cover
        logger.exception(
            "interpretation: cache write failed for input_hash=%s: %s",
            inputs_hash, exc,
        )

    return interp
