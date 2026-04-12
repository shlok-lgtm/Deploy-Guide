"""
Static Evidence Collector
==========================
Captures rendered screenshots and clean markdown extractions for static
source URLs using Firecrawl.  One API call per unique URL returns both a
PNG screenshot and markdown content — no headless browser required.

Key design decisions:
- Deduplication: components sharing the same source_url are captured once.
- Rate limiting: 2-second sleep between Firecrawl calls.
- Fallback: if Firecrawl fails for a URL, existing evidence is preserved.
- Staleness: weekly refresh detects content changes via markdown diffing.
- Storage: screenshots + markdown stored in Postgres (bytea / text).
  R2 path columns are populated when R2 upload infra is configured.
"""

import hashlib
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from app.database import execute, fetch_all, fetch_one
from app.services.firecrawl_client import capture_screenshot_and_markdown

logger = logging.getLogger(__name__)

# How long before evidence is considered stale
STALENESS_THRESHOLD_DAYS = 7

# Delay between Firecrawl calls (rate limit safety)
CAPTURE_DELAY_SECONDS = 2


def _content_hash(screenshot_bytes: bytes | None, markdown: str | None) -> str:
    """SHA-256 of combined screenshot + markdown evidence."""
    h = hashlib.sha256()
    if screenshot_bytes:
        h.update(screenshot_bytes)
    if markdown:
        h.update(markdown.encode("utf-8"))
    return h.hexdigest()


def _r2_path(index_id: str, entity_slug: str, component_slug: str, ts: str, filename: str) -> str:
    """Compute the canonical R2 object key for an evidence artifact."""
    return f"proofs/static/{index_id}/{entity_slug}/{component_slug}/{ts}/{filename}"


def capture_static_evidence(
    components: list[dict],
    rate_limit: float = CAPTURE_DELAY_SECONDS,
) -> dict:
    """
    Capture static evidence for a list of components.

    Each component dict must have:
        - source_url: str       (the page to capture)
        - entity_slug: str      (e.g. 'wormhole', 'usdc')
        - component_slug: str   (e.g. 'guardian_set', 'reserves')
        - index_id: str         (e.g. 'bri', 'sii')

    Returns summary: {"captured": int, "failed": int, "skipped": int, "deduplicated": int}
    """
    if not components:
        return {"captured": 0, "failed": 0, "skipped": 0, "deduplicated": 0}

    # --- Step 1: Group components by source_url for deduplication ---
    url_groups: dict[str, list[dict]] = defaultdict(list)
    for comp in components:
        url = comp.get("source_url", "").strip()
        if url:
            url_groups[url].append(comp)

    total_urls = len(url_groups)
    total_components = sum(len(g) for g in url_groups.values())
    deduplicated = total_components - total_urls

    logger.info(
        f"Static evidence: {total_components} components across "
        f"{total_urls} unique URLs ({deduplicated} deduplicated)"
    )

    captured = 0
    failed = 0
    skipped = 0

    # --- Step 2: Capture once per unique URL ---
    for i, (url, group) in enumerate(url_groups.items()):
        logger.info(f"  [{i+1}/{total_urls}] Capturing: {url[:80]}...")

        result = capture_screenshot_and_markdown(url)

        if not result["success"]:
            logger.warning(f"  [{i+1}/{total_urls}] FAILED: {url[:80]}")
            failed += len(group)
            # Rate limit even on failure
            if i < total_urls - 1:
                time.sleep(rate_limit)
            continue

        screenshot_bytes = result["screenshot_bytes"]
        markdown_content = result["markdown_content"]
        content_hash = _content_hash(screenshot_bytes, markdown_content)
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        # --- Step 3: Store for each component that shares this URL ---
        for comp in group:
            entity = comp["entity_slug"]
            component = comp["component_slug"]
            index_id = comp.get("index_id", "bri")

            screenshot_path = _r2_path(index_id, entity, component, ts, "screenshot.png")
            snapshot_path = _r2_path(index_id, entity, component, ts, "snapshot.md")

            try:
                _upsert_evidence(
                    source_url=url,
                    entity_slug=entity,
                    component_slug=component,
                    index_id=index_id,
                    screenshot_data=screenshot_bytes,
                    screenshot_r2_path=screenshot_path,
                    snapshot_content=markdown_content,
                    snapshot_r2_path=snapshot_path,
                    content_hash=content_hash,
                )
                captured += 1
            except Exception as e:
                logger.error(f"  Failed to store evidence for {entity}/{component}: {e}")
                failed += 1

        # Rate limit between URL captures
        if i < total_urls - 1:
            time.sleep(rate_limit)

    logger.info(
        f"Static evidence capture complete: "
        f"{captured} captured, {failed} failed, {skipped} skipped, "
        f"{deduplicated} deduplicated"
    )

    return {
        "captured": captured,
        "failed": failed,
        "skipped": skipped,
        "deduplicated": deduplicated,
    }


def _upsert_evidence(
    source_url: str,
    entity_slug: str,
    component_slug: str,
    index_id: str,
    screenshot_data: bytes | None,
    screenshot_r2_path: str,
    snapshot_content: str | None,
    snapshot_r2_path: str,
    content_hash: str,
) -> None:
    """Insert or update a static_evidence row, tracking content changes."""
    existing = fetch_one(
        """
        SELECT id, content_hash
        FROM static_evidence
        WHERE source_url = %s AND entity_slug = %s AND component_slug = %s
        """,
        (source_url, entity_slug, component_slug),
    )

    if existing:
        old_hash = existing.get("content_hash")
        execute(
            """
            UPDATE static_evidence
            SET screenshot_r2_path = %s,
                screenshot_data = %s,
                snapshot_r2_path = %s,
                snapshot_content = %s,
                content_hash = %s,
                previous_content_hash = %s,
                captured_at = NOW(),
                is_stale = FALSE,
                stale_since = NULL,
                capture_method = 'firecrawl'
            WHERE id = %s
            """,
            (
                screenshot_r2_path,
                screenshot_data,
                snapshot_r2_path,
                snapshot_content,
                content_hash,
                old_hash,
                existing["id"],
            ),
        )
        if old_hash and old_hash != content_hash:
            logger.info(
                f"  Content changed for {entity_slug}/{component_slug} "
                f"({old_hash[:12]}... -> {content_hash[:12]}...)"
            )
    else:
        execute(
            """
            INSERT INTO static_evidence
                (source_url, entity_slug, component_slug, index_id,
                 screenshot_r2_path, screenshot_data,
                 snapshot_r2_path, snapshot_content,
                 content_hash, capture_method)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'firecrawl')
            """,
            (
                source_url,
                entity_slug,
                component_slug,
                index_id,
                screenshot_r2_path,
                screenshot_data,
                snapshot_r2_path,
                snapshot_content,
                content_hash,
            ),
        )


# ---------------------------------------------------------------------------
# Staleness detection and refresh
# ---------------------------------------------------------------------------

def detect_stale_evidence() -> list[dict]:
    """
    Mark evidence rows as stale if captured_at is older than the threshold.
    Returns the list of stale rows.
    """
    threshold = datetime.now(timezone.utc) - timedelta(days=STALENESS_THRESHOLD_DAYS)

    execute(
        """
        UPDATE static_evidence
        SET is_stale = TRUE,
            stale_since = COALESCE(stale_since, NOW())
        WHERE captured_at < %s AND is_stale = FALSE
        """,
        (threshold,),
    )

    stale = fetch_all(
        """
        SELECT id, source_url, entity_slug, component_slug, index_id,
               captured_at, content_hash
        FROM static_evidence
        WHERE is_stale = TRUE
        ORDER BY captured_at ASC
        """
    )

    if stale:
        logger.info(f"Staleness scan: {len(stale)} evidence rows are stale")
    return stale


def refresh_stale_evidence() -> dict:
    """
    Re-capture evidence for all stale rows.
    Groups by URL to avoid redundant Firecrawl calls.
    Returns capture summary.
    """
    stale = detect_stale_evidence()
    if not stale:
        logger.info("No stale evidence to refresh")
        return {"captured": 0, "failed": 0, "skipped": 0, "deduplicated": 0}

    # Convert stale rows into component dicts for capture_static_evidence
    components = [
        {
            "source_url": row["source_url"],
            "entity_slug": row["entity_slug"],
            "component_slug": row["component_slug"],
            "index_id": row.get("index_id", "bri"),
        }
        for row in stale
    ]

    return capture_static_evidence(components)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def get_evidence_for_entity(entity_slug: str) -> list[dict]:
    """Get all evidence rows for an entity (for the witness page)."""
    rows = fetch_all(
        """
        SELECT id, source_url, entity_slug, component_slug, index_id,
               screenshot_r2_path, snapshot_r2_path, snapshot_content,
               content_hash, capture_method, captured_at,
               is_stale, stale_since, previous_content_hash
        FROM static_evidence
        WHERE entity_slug = %s
        ORDER BY component_slug
        """,
        (entity_slug,),
    )
    # Don't return raw screenshot bytes in list queries
    return rows


def get_screenshot(evidence_id: int) -> bytes | None:
    """Get raw screenshot PNG bytes for a specific evidence row."""
    row = fetch_one(
        "SELECT screenshot_data FROM static_evidence WHERE id = %s",
        (evidence_id,),
    )
    return row["screenshot_data"] if row else None


def get_evidence_summary() -> dict:
    """Summary stats for the static evidence table."""
    total = fetch_one("SELECT COUNT(*) as cnt FROM static_evidence")
    with_screenshots = fetch_one(
        "SELECT COUNT(*) as cnt FROM static_evidence WHERE screenshot_data IS NOT NULL"
    )
    stale_count = fetch_one(
        "SELECT COUNT(*) as cnt FROM static_evidence WHERE is_stale = TRUE"
    )
    unique_urls = fetch_one(
        "SELECT COUNT(DISTINCT source_url) as cnt FROM static_evidence"
    )
    entities = fetch_all(
        """
        SELECT entity_slug, COUNT(*) as component_count
        FROM static_evidence
        GROUP BY entity_slug
        ORDER BY entity_slug
        """
    )

    return {
        "total_evidence_rows": total["cnt"] if total else 0,
        "with_screenshots": with_screenshots["cnt"] if with_screenshots else 0,
        "stale_count": stale_count["cnt"] if stale_count else 0,
        "unique_source_urls": unique_urls["cnt"] if unique_urls else 0,
        "entities": {r["entity_slug"]: r["component_count"] for r in entities},
    }
