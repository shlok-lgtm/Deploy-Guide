"""
CoinGecko news monitor — fetches stablecoin-related news,
detects incidents, auto-drafts content angles.
"""
import asyncio
import os
import json
import logging
import httpx
from datetime import datetime, timezone
from app.database import fetch_one, fetch_all, execute

logger = logging.getLogger(__name__)

COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
COINGECKO_BASE = "https://pro-api.coingecko.com" if COINGECKO_API_KEY else "https://api.coingecko.com"

# Stablecoin symbols, issuers, generic terms, regulatory, and risk keywords
STABLECOIN_KEYWORDS = [
    # ── Stablecoin names / symbols ──
    "usdc", "usdt", "dai", "frax", "tusd", "busd", "gho", "lusd", "pyusd",
    "usde", "fdusd", "usr", "susd", "crvusd", "mkusd", "usdm", "euroe",
    "eurt", "sdai", "usds", "usdd", "usd1",
    # ── Issuer / protocol names ──
    "tether", "circle", "paxos", "makerdao", "maker", "sky", "frax finance",
    "ethena", "paypal", "mountain protocol", "resolv",
    # ── Generic stablecoin terms ──
    "stablecoin", "stable coin", "stablecoins", "depeg", "de-peg", "depegged",
    "reserves", "reserve", "attestation", "backing", "redemption", "redeem",
    "mint", "burn", "collateral", "peg",
    # ── Regulatory ──
    "mica", "genius act", "occ", "stablecoin regulation", "stablecoin legislation",
    # ── Risk events ──
    "exploit", "hack", "bank run", "liquidity crisis",
]


async def fetch_news(max_items: int = 50) -> list:
    """Fetch latest news from CoinGecko /api/v3/news endpoint."""
    headers = {}
    if COINGECKO_API_KEY:
        headers["x-cg-pro-api-key"] = COINGECKO_API_KEY

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(
                f"{COINGECKO_BASE}/api/v3/news",
                headers=headers,
            )
            resp.raise_for_status()
            data = resp.json()
            # CoinGecko returns { "data": [...] } or just a list
            items = data.get("data", data) if isinstance(data, dict) else data
            return items[:max_items] if isinstance(items, list) else []
        except Exception as e:
            logger.error(f"CoinGecko news fetch failed: {e}")
            return []


def _is_stablecoin_relevant(title: str, description: str) -> tuple:
    """Check if a news item is stablecoin-relevant. Returns (relevant, matched_keywords)."""
    text = (title + " " + description).lower()
    matches = [kw for kw in STABLECOIN_KEYWORDS if kw in text]
    return len(matches) > 0, matches


def _detect_incident(title: str, description: str) -> bool:
    """Check if a news item describes a stablecoin incident."""
    text = (title + " " + description).lower()
    incident_keywords = [
        "depeg", "depegged", "de-peg", "crash", "collapse", "exploit", "hack",
        "vulnerability", "freeze", "frozen", "blacklist", "sanctions",
        "investigation", "sec ", "lawsuit", "insolvency", "bank run",
        "withdrawal halt", "pause", "liquidity crisis", "rug pull",
    ]
    return any(kw in text for kw in incident_keywords)


async def scan_news() -> dict:
    """
    Fetch CoinGecko news, filter for stablecoin relevance,
    detect incidents, store new items in ops_coingecko_news.
    Returns summary of findings.
    """
    items = await fetch_news()
    if not items:
        return {"status": "no_news", "new_items": 0}

    new_count = 0
    relevant_count = 0
    incidents = []

    for item in items:
        news_id = str(item.get("id", item.get("news_id", "")))
        title = item.get("title", "")
        description = item.get("description", item.get("content", ""))[:2000]
        url = item.get("url", "")
        source = item.get("author", item.get("source", ""))
        thumb = item.get("thumb_2x", item.get("thumb", ""))
        published = item.get("updated_at", item.get("created_at"))

        if not news_id or not title:
            continue

        # Skip if already stored
        existing = await asyncio.to_thread(fetch_one, "SELECT id FROM ops_coingecko_news WHERE news_id = %s", (news_id,))
        if existing:
            continue

        relevant, keywords = _is_stablecoin_relevant(title, description)
        is_incident = _detect_incident(title, description) if relevant else False

        await asyncio.to_thread(
            execute,
            """INSERT INTO ops_coingecko_news
               (news_id, title, description, url, thumb, source, published_at,
                stablecoin_relevant, relevant_symbols, incident_detected)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (news_id) DO NOTHING""",
            (
                news_id, title, description[:2000], url, thumb, source,
                published, relevant, keywords or None,
                is_incident,
            ),
        )
        new_count += 1

        if relevant:
            relevant_count += 1
        if is_incident:
            incidents.append({"title": title, "url": url, "keywords": keywords})

    return {
        "status": "ok",
        "fetched": len(items),
        "new_items": new_count,
        "stablecoin_relevant": relevant_count,
        "incidents": incidents,
    }


def get_recent_news(limit: int = 30, relevant_only: bool = True) -> list:
    """Get recent CoinGecko news items."""
    if relevant_only:
        return fetch_all(
            "SELECT * FROM ops_coingecko_news WHERE stablecoin_relevant = TRUE ORDER BY fetched_at DESC LIMIT %s",
            (limit,),
        ) or []
    return fetch_all(
        "SELECT * FROM ops_coingecko_news ORDER BY fetched_at DESC LIMIT %s",
        (limit,),
    ) or []


def get_incidents(days: int = 7) -> list:
    """Get detected incidents from recent news."""
    return fetch_all(
        """SELECT * FROM ops_coingecko_news
           WHERE incident_detected = TRUE
             AND fetched_at > NOW() - INTERVAL '%s days'
           ORDER BY fetched_at DESC""",
        (days,),
    ) or []
