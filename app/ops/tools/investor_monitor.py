"""
Investor content monitor — tracks VC blogs, tweets, and portfolio announcements.
Uses Parallel Extract/Search for content discovery, Claude API for thesis alignment analysis.
Stores in ops_investor_content.
"""
import os
import json
import logging
import asyncio
import httpx
from datetime import datetime, timezone
from app.database import fetch_one, fetch_all, execute
from app.services import parallel_client

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# Known investor blog/content URLs
INVESTOR_SOURCES = {
    "Variant": {
        "blogs": ["https://variant.fund/writing"],
        "twitter_handles": ["@jessewldn"],
        "search_queries": ["Variant Fund crypto investment thesis DeFi"],
    },
    "Dragonfly": {
        "blogs": ["https://dragonfly.xyz/blog"],
        "twitter_handles": ["@hosaborni", "@tomhschmidt"],
        "search_queries": ["Dragonfly Capital DeFi infrastructure investment"],
    },
    "Coinbase Ventures": {
        "blogs": [],
        "twitter_handles": ["@coinaborni", "@CoinbaseDev", "@BuildOnBase"],
        "search_queries": ["Coinbase Ventures portfolio DeFi investment"],
    },
    "Village Global": {
        "blogs": [],
        "twitter_handles": ["@baborni"],
        "search_queries": ["Village Global fintech investment"],
    },
    "Polychain": {
        "blogs": [],
        "twitter_handles": ["@polyaborni"],
        "search_queries": ["Polychain Capital DeFi crypto investment"],
    },
    "Robot Ventures": {
        "blogs": [],
        "twitter_handles": ["@rleshner"],
        "search_queries": ["Robot Ventures Robert Leshner DeFi investment"],
    },
}


async def scan_investor_content(investor_id: int = None) -> dict:
    """
    Scan blogs and search for recent content from investors.
    If investor_id given, scan only that investor. Otherwise scan all with configured sources.
    """
    if investor_id:
        investor = await asyncio.to_thread(fetch_one, "SELECT id, name FROM ops_investors WHERE id = %s", (investor_id,))
        if not investor:
            return {"error": "Investor not found"}
        investors_to_scan = [investor]
    else:
        investors_to_scan = await asyncio.to_thread(
            fetch_all, "SELECT id, name FROM ops_investors WHERE tier <= 2 ORDER BY tier, name"
        ) or []

    total_new = 0
    total_scanned = 0
    errors = []

    for investor in investors_to_scan:
        name = investor["name"]
        sources = INVESTOR_SOURCES.get(name)
        if not sources:
            continue

        # Scan blogs via Parallel Extract
        for blog_url in sources.get("blogs", []):
            try:
                new = await _scan_investor_blog(investor["id"], blog_url)
                total_new += new
                total_scanned += 1
            except Exception as e:
                errors.append({"investor": name, "source": blog_url, "error": str(e)})
                logger.error(f"Investor blog scan failed for {name} ({blog_url}): {e}")

        # Search for recent content
        for query in sources.get("search_queries", []):
            try:
                new = await _search_investor_content(investor["id"], query)
                total_new += new
                total_scanned += 1
            except Exception as e:
                errors.append({"investor": name, "query": query, "error": str(e)})

        # Search for tweets from key handles
        for handle in sources.get("twitter_handles", []):
            try:
                new = await _scan_investor_tweets(investor["id"], handle.lstrip("@"))
                total_new += new
                total_scanned += 1
            except Exception as e:
                errors.append({"investor": name, "handle": handle, "error": str(e)})

    return {
        "scanned": total_scanned,
        "new_content": total_new,
        "errors": errors[:5] if errors else [],
    }


async def _scan_investor_blog(investor_id: int, blog_url: str) -> int:
    """Extract blog index page to find recent posts, then extract each."""
    result = await parallel_client.extract(
        blog_url,
        objective="Extract all article titles and URLs from this blog index page. Return each article's title and link.",
        full_content=True,
    )

    if "error" in result:
        logger.warning(f"Blog extract failed for {blog_url}: {result['error']}")
        return 0

    pages = result.get("results", result.get("pages", [result]))
    page = pages[0] if isinstance(pages, list) and pages else result
    content = page.get("full_content") or page.get("content") or page.get("text", "")

    if not content:
        return 0

    # Store the blog index as a content item
    existing = await asyncio.to_thread(fetch_one, "SELECT id FROM ops_investor_content WHERE source_url = %s", (blog_url,))
    if existing:
        # Update content if blog index page
        await asyncio.to_thread(
            execute,
            "UPDATE ops_investor_content SET content = %s, scraped_at = %s WHERE id = %s",
            (content[:20000], datetime.now(timezone.utc), existing["id"]),
        )
        return 0

    await asyncio.to_thread(
        execute,
        """INSERT INTO ops_investor_content
           (investor_id, source_url, source_type, title, content, scraped_at)
           VALUES (%s, %s, 'blog', %s, %s, %s)
           ON CONFLICT (source_url) DO NOTHING""",
        (investor_id, blog_url, f"Blog index", content[:20000], datetime.now(timezone.utc)),
    )
    return 1


async def _search_investor_content(investor_id: int, query: str) -> int:
    """Use Parallel Search to find recent investor content."""
    result = await parallel_client.search(query, num_results=10)

    if "error" in result:
        return 0

    results_data = result.get("results", result.get("search_results", []))
    new_count = 0

    for item in results_data if isinstance(results_data, list) else []:
        if not isinstance(item, dict):
            continue

        url = item.get("url") or item.get("link", "")
        title = item.get("title", "")
        snippet = item.get("snippet") or item.get("excerpt", "")

        if not url or not snippet:
            continue

        existing = await asyncio.to_thread(fetch_one, "SELECT id FROM ops_investor_content WHERE source_url = %s", (url,))
        if existing:
            continue

        source_type = "blog"
        url_lower = url.lower()
        if "twitter.com/" in url_lower or "x.com/" in url_lower:
            source_type = "tweet"
        elif "portfolio" in url_lower or "announcement" in url_lower:
            source_type = "portfolio_announcement"

        await asyncio.to_thread(
            execute,
            """INSERT INTO ops_investor_content
               (investor_id, source_url, source_type, title, content, scraped_at)
               VALUES (%s, %s, %s, %s, %s, %s)
               ON CONFLICT (source_url) DO NOTHING""",
            (investor_id, url, source_type, title, snippet, datetime.now(timezone.utc)),
        )
        new_count += 1

    return new_count


async def _scan_investor_tweets(investor_id: int, handle: str) -> int:
    """Search for recent tweets from an investor's key person."""
    query = f"site:twitter.com OR site:x.com from:@{handle}"
    result = await parallel_client.search(query, num_results=10)

    if "error" in result:
        return 0

    results_data = result.get("results", result.get("search_results", []))
    new_count = 0

    for item in results_data if isinstance(results_data, list) else []:
        if not isinstance(item, dict):
            continue

        url = item.get("url") or item.get("link", "")
        title = item.get("title", "")
        snippet = item.get("snippet") or item.get("excerpt", "")

        if not url:
            continue

        url_lower = url.lower()
        if not any(d in url_lower for d in ["twitter.com/", "x.com/"]):
            continue
        if "/status/" not in url_lower:
            continue

        existing = await asyncio.to_thread(fetch_one, "SELECT id FROM ops_investor_content WHERE source_url = %s", (url,))
        if existing:
            continue

        await asyncio.to_thread(
            execute,
            """INSERT INTO ops_investor_content
               (investor_id, source_url, source_type, title, content, scraped_at)
               VALUES (%s, %s, 'tweet', %s, %s, %s)
               ON CONFLICT (source_url) DO NOTHING""",
            (investor_id, url, title or f"@{handle} tweet", snippet or title, datetime.now(timezone.utc)),
        )
        new_count += 1

    return new_count


async def analyze_investor_content(content_id: int) -> dict:
    """
    Analyze investor content for thesis alignment with Basis.
    Updates the ops_investor_content record.
    """
    row = await asyncio.to_thread(
        fetch_one,
        """SELECT ic.*, i.name as investor_name, i.thesis_alignment, i.type as investor_type
           FROM ops_investor_content ic
           JOIN ops_investors i ON ic.investor_id = i.id
           WHERE ic.id = %s""",
        (content_id,),
    )
    if not row:
        return {"error": "Content not found"}

    if not ANTHROPIC_API_KEY:
        return {"error": "ANTHROPIC_API_KEY not set"}

    system_prompt = """You are an analyst for Basis Protocol, a stablecoin integrity scoring system.
You analyze investor/VC content to assess thesis alignment and identify outreach timing signals.
Respond with valid JSON only, no markdown wrapping.

Output JSON:
- thesis_extract: 1-2 sentence summary of the investment thesis expressed in this content
- alignment_score: 0.0-1.0, how aligned this investor's thesis is with Basis (stablecoin risk infrastructure, DeFi standards, protocol-level primitives)
- alignment_notes: why this content is relevant for Basis outreach
- outreach_angle: if alignment_score > 0.5, suggest a specific outreach angle based on this content
- timing_signal: true/false — does this content suggest good timing for outreach (new fund, thesis shift, portfolio gap, market event commentary)
- timing_notes: if timing_signal, explain why now is good timing"""

    user_prompt = f"""Analyze this content from investor {row['investor_name']}:

Known thesis alignment: {row.get('thesis_alignment', 'N/A')}
Investor type: {row.get('investor_type', 'N/A')}

Content:
Title: {row.get('title', 'Untitled')}
Source: {row.get('source_url', '')}
Type: {row.get('source_type', '')}

{row.get('content', '')[:6000]}"""

    async with httpx.AsyncClient(timeout=120) as client:
        try:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": CLAUDE_MODEL,
                    "max_tokens": 1500,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_prompt}],
                },
            )
            resp.raise_for_status()
            text = resp.json()["content"][0]["text"].strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]
            analysis = json.loads(text)
        except Exception as e:
            logger.error(f"Investor content analysis failed: {e}")
            return {"error": str(e)}

    await asyncio.to_thread(
        execute,
        """UPDATE ops_investor_content SET
           analyzed = TRUE,
           thesis_extract = %s,
           alignment_score = %s,
           alignment_notes = %s,
           outreach_angle = %s,
           timing_signal = %s,
           timing_notes = %s
           WHERE id = %s""",
        (
            analysis.get("thesis_extract"),
            analysis.get("alignment_score"),
            analysis.get("alignment_notes"),
            analysis.get("outreach_angle"),
            analysis.get("timing_signal", False),
            analysis.get("timing_notes"),
            content_id,
        ),
    )

    return analysis


async def get_investor_content(limit: int = 30, investor_id: int = None, analyzed_only: bool = False) -> list:
    """Get recent investor content."""
    conditions = []
    params = []

    if investor_id:
        conditions.append("ic.investor_id = %s")
        params.append(investor_id)
    if analyzed_only:
        conditions.append("ic.analyzed = TRUE")

    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    params.append(limit)

    return await asyncio.to_thread(
        fetch_all,
        f"""SELECT ic.*, i.name as investor_name
            FROM ops_investor_content ic
            JOIN ops_investors i ON ic.investor_id = i.id
            {where}
            ORDER BY ic.scraped_at DESC LIMIT %s""",
        params,
    ) or []


async def get_timing_signals(limit: int = 10) -> list:
    """Get investor content with timing signals — high-priority outreach opportunities."""
    return await asyncio.to_thread(
        fetch_all,
        """SELECT ic.*, i.name as investor_name, i.stage, i.tier
           FROM ops_investor_content ic
           JOIN ops_investors i ON ic.investor_id = i.id
           WHERE ic.timing_signal = TRUE AND ic.actioned = FALSE
           ORDER BY ic.alignment_score DESC NULLS LAST, ic.scraped_at DESC
           LIMIT %s""",
        (limit,),
    ) or []
