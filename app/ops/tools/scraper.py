"""
Scraper tool — uses Parallel Extract to scrape target content.
Reuses the existing Parallel.ai client configuration.
"""
import asyncio
import logging
from datetime import datetime
from app.database import fetch_one, execute
from app.services import parallel_client

logger = logging.getLogger(__name__)


async def scrape_target(target_id: int, url: str, source_type: str = "blog"):
    """
    Scrape a URL using Parallel Extract, store in ops_target_content.
    Returns the content record ID or None on failure.
    """
    # Check if already scraped
    existing = await asyncio.to_thread(fetch_one, "SELECT id FROM ops_target_content WHERE source_url = %s", (url,))
    if existing:
        logger.info(f"URL already scraped: {url}")
        return existing["id"]

    # Extract content via Parallel
    result = await parallel_client.extract(
        url,
        objective="Extract the full article content, title, publication date, and key themes discussed.",
        full_content=True,
    )

    if "error" in result:
        logger.error(f"Scrape failed for {url}: {result['error']}")
        return None

    # Parse result — Parallel returns list of page results
    pages = result.get("results", result.get("pages", [result]))
    if isinstance(pages, list) and len(pages) > 0:
        page = pages[0]
    else:
        page = result

    content = page.get("full_content") or page.get("content") or page.get("text", "")
    title = page.get("title", "")

    if not content:
        logger.warning(f"No content extracted from {url}")
        return None

    # Store in ops_target_content
    await asyncio.to_thread(
        execute,
        """INSERT INTO ops_target_content (target_id, source_url, source_type, title, content, scraped_at)
           VALUES (%s, %s, %s, %s, %s, %s)
           ON CONFLICT (source_url) DO NOTHING""",
        (target_id, url, source_type, title, content, datetime.utcnow()),
    )

    row = await asyncio.to_thread(fetch_one, "SELECT id FROM ops_target_content WHERE source_url = %s", (url,))
    return row["id"] if row else None


async def setup_monitors(webhook_base_url: str):
    """
    Register Parallel Monitor watches for Tier 1 target blog URLs.
    Returns list of monitor IDs created.
    """
    from app.database import fetch_all

    # Tier 1 target surfaces to monitor
    monitor_surfaces = [
        {"query": "karpatkey blog new posts on kpk.io", "target": "karpatkey", "frequency": "12h"},
        {"query": "Morpho blog new posts on morpho.org/blog", "target": "Morpho", "frequency": "12h"},
        {"query": "Steakhouse Financial new posts on steakhouse.financial", "target": "Steakhouse Financial", "frequency": "12h"},
        {"query": "Aave governance new stablecoin-related proposals on governance.aave.com", "target": "Aave governance", "frequency": "6h"},
        {"query": "Morpho forum new posts on forum.morpho.org", "target": "Morpho", "frequency": "6h"},
    ]

    webhook_url = f"{webhook_base_url}/api/ops/webhook/monitor"
    created = []

    for surface in monitor_surfaces:
        try:
            result = await parallel_client.monitor_create(
                query=surface["query"],
                frequency=surface["frequency"],
                webhook_url=webhook_url,
            )
            if "error" not in result:
                monitor_id = result.get("monitor_id") or result.get("id")
                created.append({
                    "target": surface["target"],
                    "monitor_id": monitor_id,
                    "query": surface["query"],
                })
                logger.info(f"Monitor created for {surface['target']}: {monitor_id}")
            else:
                logger.error(f"Monitor creation failed for {surface['target']}: {result['error']}")
        except Exception as e:
            logger.error(f"Monitor setup failed for {surface['target']}: {e}")

    return created
