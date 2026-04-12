"""
Static Component Evidence Capture
====================================
Captures, hashes, and stores provenance evidence for every static component
value in the registry. For each source URL:

1. Fetch raw HTML/content snapshot
2. Compute SHA-256 content hash for change detection
3. Extract relevant section text (if source_section specified)
4. Store evidence record in static_evidence table
5. Optionally store artifacts to R2 (snapshot, screenshot, proof)

The content hash enables staleness detection: if the page changes between
captures, the system flags it for human review without auto-updating values.
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone

import httpx

from app.database import execute, fetch_one, fetch_all

logger = logging.getLogger(__name__)

# Rate limit between captures to avoid hammering any single domain
CAPTURE_DELAY_SECONDS = 2.0

# Max response body to capture (avoid OOM on huge pages)
MAX_SNAPSHOT_BYTES = 512 * 1024  # 512KB

# Evidence older than this is eligible for refresh
REFRESH_THRESHOLD_DAYS = 7

# Request timeout for fetching source pages
FETCH_TIMEOUT = 30


# ---------------------------------------------------------------------------
# Content fetching & hashing
# ---------------------------------------------------------------------------

def _fetch_page(url: str, range_limit: int = None) -> dict:
    """
    Fetch a URL and return response metadata + truncated body.

    For GitHub raw URLs, prefers the API endpoint for smaller responses.
    Uses Range header if range_limit is set to cap download size.
    """
    headers = {
        "User-Agent": "BasisProtocol-EvidenceCapture/1.0",
        "Accept": "text/html,application/xhtml+xml,application/json,*/*",
    }
    if range_limit:
        headers["Range"] = f"bytes=0-{range_limit}"

    # Prefer GitHub raw API for smaller responses
    api_url = _github_to_api_url(url)
    fetch_url = api_url or url

    try:
        with httpx.Client(timeout=FETCH_TIMEOUT, follow_redirects=True) as client:
            resp = client.get(fetch_url, headers=headers)
            resp.raise_for_status()

            body = resp.content[:MAX_SNAPSHOT_BYTES]
            content_type = resp.headers.get("content-type", "")

            return {
                "url": url,
                "fetch_url": fetch_url,
                "status_code": resp.status_code,
                "content_type": content_type,
                "body": body,
                "body_text": body.decode("utf-8", errors="replace"),
                "content_length": len(body),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            }
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return {
            "url": url,
            "fetch_url": fetch_url,
            "status_code": 0,
            "content_type": "",
            "body": b"",
            "body_text": "",
            "content_length": 0,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
        }


def _github_to_api_url(url: str) -> str | None:
    """
    Convert a GitHub web URL to its raw API equivalent for smaller responses.
    e.g. https://github.com/org/repo/blob/main/SECURITY.md
      -> https://api.github.com/repos/org/repo/contents/SECURITY.md?ref=main
    """
    if "github.com" not in url or "/blob/" not in url:
        return None
    try:
        # https://github.com/{owner}/{repo}/blob/{ref}/{path}
        parts = url.split("github.com/")[1]
        segments = parts.split("/")
        if len(segments) < 5 or segments[2] != "blob":
            return None
        owner, repo, _, ref = segments[0], segments[1], segments[2], segments[3]
        path = "/".join(segments[4:])
        return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={ref}"
    except (IndexError, ValueError):
        return None


def _compute_content_hash(body: bytes) -> str:
    """SHA-256 of raw content bytes."""
    return hashlib.sha256(body).hexdigest()


def _extract_section(body_text: str, section_hint: str) -> str:
    """
    Best-effort extraction of a relevant section from the page content.
    Looks for the section_hint text and returns surrounding context.
    """
    if not section_hint or not body_text:
        return ""

    hint_lower = section_hint.lower()
    text_lower = body_text.lower()
    idx = text_lower.find(hint_lower)
    if idx == -1:
        return ""

    # Return ~2000 chars around the match
    start = max(0, idx - 500)
    end = min(len(body_text), idx + 1500)
    return body_text[start:end].strip()


# ---------------------------------------------------------------------------
# Single component evidence capture
# ---------------------------------------------------------------------------

def capture_static_evidence(
    index_id: str,
    entity_slug: str,
    component_name: str,
    source_url: str,
    source_section: str | None,
    captured_value,
) -> dict:
    """
    Capture evidence for a single static component.

    Returns evidence metadata dict (without storing to DB — caller decides).
    """
    page = _fetch_page(source_url, range_limit=MAX_SNAPSHOT_BYTES)

    content_hash = _compute_content_hash(page["body"]) if page["body"] else ""
    extracted_text = _extract_section(page["body_text"], source_section) if source_section else ""

    return {
        "index_id": index_id,
        "entity_slug": entity_slug,
        "component_name": component_name,
        "source_url": source_url,
        "source_section": source_section,
        "captured_value": str(captured_value),
        "content_hash": content_hash,
        "extracted_text": extracted_text[:10000],  # cap DB column
        "captured_at": datetime.now(timezone.utc),
        "snapshot_size": page["content_length"],
        "fetch_error": page.get("error"),
        # R2 paths — populated later by artifact storage step
        "proof_r2_path": None,
        "screenshot_r2_path": None,
        "snapshot_r2_path": None,
    }


# ---------------------------------------------------------------------------
# Database persistence
# ---------------------------------------------------------------------------

def store_evidence(evidence: dict) -> int | None:
    """
    Insert an evidence record into the static_evidence table.
    Returns the inserted row ID, or None on failure.
    """
    try:
        row = fetch_one(
            """
            INSERT INTO static_evidence
                (index_id, entity_slug, component_name, source_url, source_section,
                 captured_value, content_hash, proof_r2_path, screenshot_r2_path,
                 snapshot_r2_path, extracted_text, captured_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                evidence["index_id"],
                evidence["entity_slug"],
                evidence["component_name"],
                evidence["source_url"],
                evidence["source_section"],
                evidence["captured_value"],
                evidence["content_hash"],
                evidence["proof_r2_path"],
                evidence["screenshot_r2_path"],
                evidence["snapshot_r2_path"],
                evidence["extracted_text"],
                evidence["captured_at"],
            ),
        )
        return row["id"] if row else None
    except Exception as e:
        logger.error(f"Failed to store evidence for {evidence['index_id']}/{evidence['entity_slug']}/{evidence['component_name']}: {e}")
        return None


def get_latest_evidence(index_id: str, entity_slug: str, component_name: str) -> dict | None:
    """Get the most recent evidence record for a specific component."""
    return fetch_one(
        """
        SELECT * FROM static_evidence
        WHERE index_id = %s AND entity_slug = %s AND component_name = %s
        ORDER BY captured_at DESC
        LIMIT 1
        """,
        (index_id, entity_slug, component_name),
    )


def get_evidence_for_entity(index_id: str, entity_slug: str) -> list[dict]:
    """Get all latest evidence records for an entity (one per component)."""
    rows = fetch_all(
        """
        SELECT DISTINCT ON (component_name) *
        FROM static_evidence
        WHERE index_id = %s AND entity_slug = %s
        ORDER BY component_name, captured_at DESC
        """,
        (index_id, entity_slug),
    )
    return rows or []


def get_stale_evidence() -> list[dict]:
    """Get all evidence records flagged as stale."""
    rows = fetch_all(
        """
        SELECT * FROM static_evidence
        WHERE stale_detected_at IS NOT NULL
        ORDER BY stale_detected_at DESC
        """,
    )
    return rows or []


def get_evidence_summary() -> dict:
    """Get summary statistics for the evidence table."""
    row = fetch_one(
        """
        SELECT
            COUNT(*) AS total_records,
            COUNT(DISTINCT index_id || '/' || entity_slug || '/' || component_name) AS unique_components,
            COUNT(*) FILTER (WHERE stale_detected_at IS NOT NULL) AS stale_count,
            MIN(captured_at) AS oldest_capture,
            MAX(captured_at) AS newest_capture
        FROM static_evidence
        """
    )
    return dict(row) if row else {}


# ---------------------------------------------------------------------------
# Staleness detection
# ---------------------------------------------------------------------------

def detect_staleness(
    index_id: str,
    entity_slug: str,
    component_name: str,
    source_url: str,
    source_section: str | None,
    stored_hash: str,
) -> dict:
    """
    Re-fetch a source URL and compare content hash against stored evidence.
    Returns dict with:
      - is_stale: bool (True if page content has changed)
      - new_hash: current content hash
      - old_hash: previously stored hash
    """
    page = _fetch_page(source_url, range_limit=MAX_SNAPSHOT_BYTES)
    new_hash = _compute_content_hash(page["body"]) if page["body"] else ""

    is_stale = bool(new_hash and stored_hash and new_hash != stored_hash)

    if is_stale:
        logger.warning(
            f"Static component {index_id}/{entity_slug}/{component_name} source has changed. "
            f"Old hash: {stored_hash[:16]}... New hash: {new_hash[:16]}... "
            f"Manual review required."
        )
        # Mark the existing evidence as stale
        try:
            execute(
                """
                UPDATE static_evidence
                SET stale_detected_at = NOW()
                WHERE index_id = %s AND entity_slug = %s AND component_name = %s
                  AND stale_detected_at IS NULL
                """,
                (index_id, entity_slug, component_name),
            )
        except Exception as e:
            logger.error(f"Failed to mark staleness: {e}")

    return {
        "is_stale": is_stale,
        "new_hash": new_hash,
        "old_hash": stored_hash,
        "fetch_error": page.get("error"),
    }


# ---------------------------------------------------------------------------
# Batch operations — called by worker
# ---------------------------------------------------------------------------

def run_static_evidence_collection():
    """
    Main entry point for the worker cycle. For each registered static component:
    1. Check if evidence exists and is fresh (<7 days old)
    2. If missing or stale, capture new evidence
    3. Store results and attest

    Rate-limited to ~2s between captures to avoid hammering domains.
    """
    from app.collectors.static_provenance_registry import iter_all_static_components

    captured = 0
    skipped = 0
    stale_detected = 0
    errors = 0

    for index_id, entity_slug, component_name, entry in iter_all_static_components():
        try:
            existing = get_latest_evidence(index_id, entity_slug, component_name)

            if existing:
                # Check age
                captured_at = existing.get("captured_at")
                if captured_at:
                    if hasattr(captured_at, "timestamp"):
                        age_days = (datetime.now(timezone.utc) - captured_at).days
                    else:
                        age_days = 999  # force refresh
                else:
                    age_days = 999

                if age_days < REFRESH_THRESHOLD_DAYS:
                    skipped += 1
                    continue

                # Existing evidence is old — check for staleness first
                if existing.get("content_hash"):
                    staleness = detect_staleness(
                        index_id, entity_slug, component_name,
                        entry["source_url"], entry.get("source_section"),
                        existing["content_hash"],
                    )
                    if staleness["is_stale"]:
                        stale_detected += 1
                    time.sleep(CAPTURE_DELAY_SECONDS)

            # Capture fresh evidence
            evidence = capture_static_evidence(
                index_id, entity_slug, component_name,
                entry["source_url"], entry.get("source_section"),
                entry["value"],
            )

            if evidence.get("fetch_error"):
                errors += 1
                logger.warning(
                    f"Evidence capture failed for {index_id}/{entity_slug}/{component_name}: "
                    f"{evidence['fetch_error']}"
                )
                # Still store the record with empty hash — marks the attempt
                if not evidence["content_hash"]:
                    evidence["content_hash"] = ""

            row_id = store_evidence(evidence)
            if row_id:
                captured += 1
            else:
                errors += 1

            time.sleep(CAPTURE_DELAY_SECONDS)

        except Exception as e:
            errors += 1
            logger.error(f"Evidence collection error for {index_id}/{entity_slug}/{component_name}: {e}")

    # Attest the batch
    try:
        from app.state_attestation import attest_state
        evidence_rows = fetch_all(
            """
            SELECT index_id, entity_slug, component_name, content_hash, captured_at
            FROM static_evidence
            WHERE captured_at > NOW() - INTERVAL '24 hours'
            ORDER BY captured_at DESC
            """
        )
        if evidence_rows:
            attest_state("static_evidence", [dict(r) for r in evidence_rows])
    except Exception as e:
        logger.debug(f"Static evidence attestation skipped: {e}")

    logger.info(
        f"Static evidence collection complete: "
        f"captured={captured} skipped={skipped} stale={stale_detected} errors={errors}"
    )

    return {
        "captured": captured,
        "skipped": skipped,
        "stale_detected": stale_detected,
        "errors": errors,
    }
