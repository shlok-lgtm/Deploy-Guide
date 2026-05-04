"""
Twitter monitor — uses Parallel Search to find recent tweets from target handles.
Stores in ops_target_content with source_type='tweet', auto-triggers analysis.
"""
import asyncio
import logging
from datetime import datetime, timezone
from app.database import fetch_one, fetch_all, execute
from app.services import parallel_client

logger = logging.getLogger(__name__)

# Stablecoin keywords for relevance filtering
STABLECOIN_KEYWORDS = [
    "stablecoin", "usdc", "usdt", "dai", "frax", "pyusd", "fdusd", "tusd",
    "usdd", "usde", "usd1", "peg", "depeg", "attestation", "reserves",
    "collateral", "mint", "redeem", "backing", "sii", "basis protocol",
]


async def scan_target_tweets(target_id: int = None, max_per_handle: int = 10) -> dict:
    """
    Scan Twitter for recent tweets from target contacts' handles.
    If target_id provided, scan only that target. Otherwise scan all Tier 1+2.
    """
    if target_id:
        targets = await asyncio.to_thread(fetch_all,
            """SELECT t.id, t.name, t.tier FROM ops_targets t WHERE t.id = %s""",
            (target_id,),
        )
    else:
        targets = await asyncio.to_thread(fetch_all,
            """SELECT t.id, t.name, t.tier FROM ops_targets t
               WHERE t.tier <= 2 ORDER BY t.tier, t.name"""
        )

    if not targets:
        return {"scanned": 0, "new_tweets": 0, "message": "No targets to scan"}

    total_new = 0
    total_scanned = 0
    errors = []

    for target in targets:
        # Get Twitter handles from contacts
        contacts = await asyncio.to_thread(fetch_all,
            """SELECT twitter_handle FROM ops_target_contacts
               WHERE target_id = %s AND twitter_handle IS NOT NULL AND twitter_handle != ''""",
            (target["id"],),
        )
        if not contacts:
            continue

        for contact in contacts:
            handle = contact["twitter_handle"].lstrip("@")
            try:
                new = await _scan_handle(target["id"], handle, max_per_handle)
                total_new += new
                total_scanned += 1
            except Exception as e:
                errors.append({"handle": handle, "error": str(e)})
                logger.error(f"Twitter scan failed for @{handle}: {e}")

    return {
        "scanned": total_scanned,
        "new_tweets": total_new,
        "errors": errors[:5] if errors else [],
    }


async def _scan_handle(target_id: int, handle: str, max_results: int = 10) -> int:
    """
    Use Parallel Search to find recent tweets from a handle.
    Returns count of new tweets stored.
    """
    query = f"site:twitter.com OR site:x.com from:@{handle}"
    result = await parallel_client.search(query, num_results=max_results)

    if "error" in result:
        logger.warning(f"Parallel search failed for @{handle}: {result['error']}")
        return 0

    results_data = result.get("results", result.get("search_results", []))
    if not isinstance(results_data, list):
        return 0

    new_count = 0
    for item in results_data:
        if isinstance(item, dict):
            url = item.get("url") or item.get("link") or item.get("source_url", "")
            title = item.get("title", "")
            snippet = item.get("snippet") or item.get("excerpt") or item.get("description", "")
        elif isinstance(item, str):
            url = item
            title = ""
            snippet = ""
        else:
            continue

        if not url:
            continue

        # Filter to actual tweet URLs
        url_lower = url.lower()
        if not any(domain in url_lower for domain in ["twitter.com/", "x.com/"]):
            continue
        # Skip profile pages, lists, etc — want actual status URLs
        if "/status/" not in url_lower:
            continue

        # Skip if already stored
        existing = await asyncio.to_thread(fetch_one, "SELECT id FROM ops_target_content WHERE source_url = %s", (url,))
        if existing:
            continue

        content = snippet or title or f"Tweet from @{handle}"

        await asyncio.to_thread(execute,
            """INSERT INTO ops_target_content
               (target_id, source_url, source_type, title, content, scraped_at)
               VALUES (%s, %s, 'tweet', %s, %s, %s)
               ON CONFLICT (source_url) DO NOTHING""",
            (target_id, url, title or f"@{handle} tweet", content, datetime.now(timezone.utc)),
        )
        new_count += 1

    return new_count


async def scan_keyword_tweets(keywords: list = None, max_results: int = 20) -> dict:
    """
    Search Twitter for stablecoin-related tweets mentioning Basis-relevant topics.
    Useful for finding engagement opportunities beyond tracked handles.
    """
    if not keywords:
        keywords = STABLECOIN_KEYWORDS[:5]  # Top relevance keywords

    query_str = " OR ".join(keywords[:5])
    query = f"site:twitter.com OR site:x.com {query_str}"

    result = await parallel_client.search(query, num_results=max_results)
    if "error" in result:
        return {"error": result["error"]}

    results_data = result.get("results", result.get("search_results", []))
    tweets = []
    for item in results_data:
        if isinstance(item, dict):
            url = item.get("url") or item.get("link", "")
            if "twitter.com/" in url.lower() or "x.com/" in url.lower():
                tweets.append({
                    "url": url,
                    "title": item.get("title", ""),
                    "snippet": item.get("snippet") or item.get("excerpt", ""),
                })

    return {"keywords": keywords, "tweets": tweets, "count": len(tweets)}


def get_recent_tweets(limit: int = 30, target_id: int = None) -> list:
    """Get recent tweets from ops_target_content."""
    if target_id:
        return fetch_all(
            """SELECT c.*, t.name as target_name
               FROM ops_target_content c
               JOIN ops_targets t ON c.target_id = t.id
               WHERE c.source_type = 'tweet' AND c.target_id = %s
               ORDER BY c.scraped_at DESC LIMIT %s""",
            (target_id, limit),
        ) or []
    return fetch_all(
        """SELECT c.*, t.name as target_name
           FROM ops_target_content c
           JOIN ops_targets t ON c.target_id = t.id
           WHERE c.source_type = 'tweet'
           ORDER BY c.scraped_at DESC LIMIT %s""",
        (limit,),
    ) or []
