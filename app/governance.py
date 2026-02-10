"""
Basis Protocol — Governance Intelligence
============================================
Crawls DeFi governance forums (Discourse API), extracts stablecoin mentions
and risk metric discussions. Runs in the same process as SII, same database.

Collapsed from standalone governance-intel project into single module.

Forums: Aave, MakerDAO, Compound, Morpho, Frax
"""

import re
import time
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Generator
from dataclasses import dataclass, asdict
from collections import defaultdict

import httpx

from app.database import get_conn

logger = logging.getLogger(__name__)


# =============================================================================
# Forum Config (was sources.yaml)
# =============================================================================

FORUMS = {
    "aave": {
        "name": "Aave Governance",
        "base_url": "https://governance.aave.com",
        "categories": [
            {"id": 4, "slug": "governance"},
            {"id": 7, "slug": "risk"},
            {"id": 6, "slug": "other"},
        ],
    },
    "makerdao": {
        "name": "MakerDAO/Sky Forum",
        "base_url": "https://forum.makerdao.com",
        "categories": [
            {"id": 89, "slug": "general-discussion"},
            {"id": 84, "slug": "spark-prime"},
            {"id": 103, "slug": "grove-prime"},
            {"id": 104, "slug": "keel-prime"},
            {"id": 94, "slug": "ecosystem-proposals"},
        ],
    },
    "compound": {
        "name": "Compound Forum",
        "base_url": "https://www.comp.xyz",
        "categories": [
            {"id": 5, "slug": "governance"},
            {"id": 6, "slug": "proposals"},
            {"id": 9, "slug": "markets"},
        ],
    },
    "morpho": {
        "name": "Morpho Forum",
        "base_url": "https://forum.morpho.org",
        "categories": [
            {"id": 26, "slug": "governance"},
            {"id": 23, "slug": "morpho-blue"},
            {"id": 17, "slug": "vaults"},
        ],
    },
    "frax": {
        "name": "Frax Governance",
        "base_url": "https://gov.frax.finance",
        "categories": [
            {"id": 5, "slug": "governance"},
            {"id": 7, "slug": "fip"},
        ],
    },
}

# Stablecoin variants for entity extraction
STABLECOIN_VARIANTS = {
    "USDC": ["USDC", "usdc", "USD Coin", "Circle USD"],
    "USDT": ["USDT", "usdt", "Tether", "tether"],
    "DAI":  ["DAI", "dai", "Dai"],
    "FRAX": ["FRAX", "frax", "Frax"],
    "FDUSD": ["FDUSD", "fdusd", "First Digital USD"],
    "PYUSD": ["PYUSD", "pyusd", "PayPal USD"],
    "TUSD": ["TUSD", "tusd", "TrueUSD"],
    "LUSD": ["LUSD", "lusd", "Liquity USD"],
    "crvUSD": ["crvUSD", "crvusd", "Curve USD"],
    "GHO":  ["GHO", "gho"],
    "USDD": ["USDD", "usdd"],
    "USDe": ["USDe", "usde", "Ethena"],
    "sUSD": ["sUSD", "susd"],
}

# Metric keywords → SII categories
METRIC_KEYWORDS = {
    "reserve_collateral": [
        "reserve", "reserves", "collateral", "backing", "attestation",
        "audit", "treasury", "T-bill", "commercial paper", "custodian",
        "overcollateralized",
    ],
    "peg_stability": [
        "peg", "depeg", "deviation", "stability", "price stability",
        "dollar peg",
    ],
    "liquidity": [
        "liquidity", "TVL", "pool", "depth", "volume", "slippage",
        "order book", "DEX", "CEX", "Curve", "Uniswap",
    ],
    "redemption": [
        "redemption", "redeem", "mint", "burn", "withdrawal", "queue",
    ],
    "distribution": [
        "holder", "concentration", "whale", "distribution", "Gini",
    ],
    "smart_contract": [
        "contract", "exploit", "vulnerability", "upgrade",
        "multisig", "timelock", "pause", "blacklist",
    ],
    "oracle": [
        "oracle", "Chainlink", "Pyth", "price feed", "manipulation",
    ],
    "governance": [
        "governance", "DAO", "vote", "proposal", "admin",
    ],
    "regulatory": [
        "MiCA", "regulation", "compliance", "license", "NYDFS",
        "SEC", "sanction", "GENIUS Act",
    ],
}

RATE_LIMIT_DELAY = 2.0  # seconds between requests


# =============================================================================
# Database Schema (governance tables in SII database)
# =============================================================================

GOV_MIGRATION = """
-- Governance Intelligence tables (added to SII database)

CREATE TABLE IF NOT EXISTS gov_documents (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    source_id VARCHAR(255),
    url TEXT,
    title TEXT,
    body TEXT NOT NULL,
    author VARCHAR(255),
    published_at TIMESTAMP,
    scraped_at TIMESTAMP DEFAULT NOW(),
    document_type VARCHAR(50),
    category VARCHAR(100),
    extra_data JSONB,
    UNIQUE(source, source_id)
);

CREATE TABLE IF NOT EXISTS gov_stablecoin_mentions (
    id SERIAL PRIMARY KEY,
    document_id INTEGER REFERENCES gov_documents(id),
    stablecoin VARCHAR(50) NOT NULL,
    context TEXT,
    sentiment VARCHAR(20),
    mention_type VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS gov_metric_mentions (
    id SERIAL PRIMARY KEY,
    document_id INTEGER REFERENCES gov_documents(id),
    metric_name VARCHAR(100) NOT NULL,
    metric_category VARCHAR(50),
    raw_text TEXT,
    importance_signal VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS gov_analysis_tags (
    id SERIAL PRIMARY KEY,
    document_id INTEGER REFERENCES gov_documents(id),
    tag_type VARCHAR(50),
    tag_value TEXT,
    confidence NUMERIC,
    created_by VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS gov_crawl_logs (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    documents_found INTEGER DEFAULT 0,
    documents_new INTEGER DEFAULT 0,
    errors INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'running'
);

CREATE INDEX IF NOT EXISTS idx_gov_docs_source ON gov_documents(source);
CREATE INDEX IF NOT EXISTS idx_gov_docs_published ON gov_documents(published_at);
CREATE INDEX IF NOT EXISTS idx_gov_mentions_coin ON gov_stablecoin_mentions(stablecoin);
CREATE INDEX IF NOT EXISTS idx_gov_metrics_cat ON gov_metric_mentions(metric_category);
"""


def apply_gov_migration():
    """Create governance tables if they don't exist."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(GOV_MIGRATION)
        logger.info("Governance tables ready")
    except Exception as e:
        logger.error(f"Gov migration failed: {e}")


# =============================================================================
# Discourse Crawler (collapsed from crawlers/discourse.py)
# =============================================================================

@dataclass
class ForumPost:
    source: str
    source_id: str
    url: str
    title: str
    body: str
    author: str
    published_at: Optional[str]
    document_type: str
    category: str
    reply_count: int = 0
    views: int = 0
    tags: list = None


def _strip_html(html: str) -> str:
    """Strip HTML tags from text."""
    text = re.sub(r'<[^>]+>', ' ', html)
    return re.sub(r'\s+', ' ', text).strip()


def _classify_doc_type(title: str, category: str, tags: list) -> str:
    """Classify document type from signals."""
    t = title.lower()
    c = category.lower()
    tl = [x.lower() for x in (tags or [])]

    proposal_signals = ['proposal', 'arfc', 'arc', 'temp check',
                        'temperature check', 'rfc', 'fip', 'mip']
    if any(s in t for s in proposal_signals) or 'proposal' in c or \
       any(s in tl for s in ['proposal', 'arc', 'arfc']):
        return 'proposal'

    risk_signals = ['risk', 'parameter', 'ltv', 'collateral', 'liquidation']
    if any(s in t for s in risk_signals) or 'risk' in c:
        return 'risk_discussion'

    return 'discussion'


def _is_stablecoin_relevant(title: str, body: str) -> bool:
    """Check if post mentions stablecoins."""
    text = f"{title} {body}".lower()
    for variants in STABLECOIN_VARIANTS.values():
        for v in variants:
            if v.lower() in text:
                return True
    general = ['stablecoin', 'stable coin', 'peg', 'depeg',
               'collateral', 'reserve', 'attestation']
    return any(kw in text for kw in general)


def crawl_forum(forum_key: str, since_days: int = 30) -> Generator[ForumPost, None, None]:
    """
    Crawl a single Discourse forum, yielding stablecoin-relevant posts.
    Uses synchronous httpx to keep it simple.
    """
    forum = FORUMS.get(forum_key)
    if not forum:
        logger.error(f"Unknown forum: {forum_key}")
        return

    base_url = forum["base_url"].rstrip("/")
    since_date = datetime.now(timezone.utc) - timedelta(days=since_days)

    for cat in forum["categories"]:
        cat_id = cat["id"]
        cat_slug = cat["slug"]
        page = 0
        topics_crawled = 0

        while topics_crawled < 200:
            logger.info(f"Crawling {forum_key}/{cat_slug} page {page}")

            try:
                r = httpx.get(
                    f"{base_url}/c/{cat_id}.json",
                    params={"page": page},
                    timeout=30,
                    headers={"Accept": "application/json",
                             "User-Agent": "BasisProtocol/1.0"},
                )
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                logger.error(f"Failed to get {forum_key}/{cat_slug} page {page}: {e}")
                break

            time.sleep(RATE_LIMIT_DELAY)

            topics = data.get("topic_list", {}).get("topics", [])
            if not topics:
                break

            old_count = 0
            for topic in topics:
                topic_id = topic.get("id")
                last_posted = topic.get("last_posted_at") or topic.get("created_at", "")

                # Skip old topics
                if last_posted:
                    try:
                        from dateutil.parser import parse as parse_date
                        lp = parse_date(last_posted)
                        if lp < since_date:
                            old_count += 1
                            continue
                    except Exception:
                        pass

                # Fetch full topic
                try:
                    tr = httpx.get(
                        f"{base_url}/t/{topic_id}.json",
                        timeout=30,
                        headers={"Accept": "application/json",
                                 "User-Agent": "BasisProtocol/1.0"},
                    )
                    tr.raise_for_status()
                    td = tr.json()
                except Exception as e:
                    logger.error(f"Failed to get topic {topic_id}: {e}")
                    time.sleep(RATE_LIMIT_DELAY)
                    continue

                time.sleep(RATE_LIMIT_DELAY)

                posts = td.get("post_stream", {}).get("posts", [])
                if not posts:
                    continue

                # Combine all posts
                all_content = []
                for p in posts:
                    author = p.get("username", "unknown")
                    cooked = p.get("cooked", "")
                    plain = _strip_html(cooked)
                    all_content.append(f"[{author}]: {plain}")

                full_body = "\n\n---\n\n".join(all_content)
                title = topic.get("title", "")

                # Filter for stablecoin relevance
                if not _is_stablecoin_relevant(title, full_body):
                    continue

                tags_list = td.get("tags", [])
                doc_type = _classify_doc_type(title, cat_slug, tags_list)

                slug = topic.get("slug", str(topic_id))

                yield ForumPost(
                    source=forum_key,
                    source_id=str(topic_id),
                    url=f"{base_url}/t/{slug}/{topic_id}",
                    title=title,
                    body=full_body,
                    author=posts[0].get("username", "unknown"),
                    published_at=topic.get("created_at"),
                    document_type=doc_type,
                    category=cat_slug,
                    reply_count=topic.get("reply_count", 0),
                    views=topic.get("views", 0),
                    tags=tags_list,
                )

                topics_crawled += 1

            # If most topics on this page are old, stop
            if old_count > len(topics) * 0.8:
                break

            page += 1


# =============================================================================
# Entity Extraction (collapsed from extractors/entities.py)
# =============================================================================

def _build_patterns():
    """Build compiled regex patterns for extraction."""
    coin_patterns = {}
    for coin, variants in STABLECOIN_VARIANTS.items():
        pattern = r'\b(' + '|'.join(re.escape(v) for v in variants) + r')\b'
        coin_patterns[coin] = re.compile(pattern, re.IGNORECASE)

    metric_patterns = {}
    for category, keywords in METRIC_KEYWORDS.items():
        parts = [re.escape(kw).replace(r'\ ', r'[\s-]?') for kw in keywords]
        combined = r'\b(' + '|'.join(parts) + r')\b'
        metric_patterns[category] = re.compile(combined, re.IGNORECASE)

    return coin_patterns, metric_patterns


COIN_PATTERNS, METRIC_PATTERNS = _build_patterns()


def extract_stablecoins(text: str, context_window: int = 200) -> list[dict]:
    """Extract stablecoin mentions with surrounding context."""
    mentions = []
    seen_positions = set()

    for coin, pattern in COIN_PATTERNS.items():
        for match in pattern.finditer(text):
            pos = match.start()
            if pos in seen_positions:
                continue
            seen_positions.add(pos)

            start = max(0, pos - context_window)
            end = min(len(text), match.end() + context_window)
            context = text[start:end]

            mentions.append({
                "stablecoin": coin,
                "context": context[:500],
                "sentiment": _guess_sentiment(context),
            })

    return mentions


def extract_metrics(text: str, context_window: int = 150) -> list[dict]:
    """Extract risk metric mentions."""
    mentions = []

    for category, pattern in METRIC_PATTERNS.items():
        for match in pattern.finditer(text):
            start = max(0, match.start() - context_window)
            end = min(len(text), match.end() + context_window)
            context = text[start:end]

            mentions.append({
                "metric_name": match.group().lower(),
                "metric_category": category,
                "raw_text": context[:500],
                "importance_signal": _assess_importance(context),
            })

    return mentions


def _guess_sentiment(context: str) -> str:
    """Quick sentiment from context keywords."""
    c = context.lower()
    neg = ['risk', 'concern', 'problem', 'issue', 'fail', 'depeg', 'hack',
           'exploit', 'vulnerability', 'worry', 'dangerous', 'opaque']
    pos = ['safe', 'trust', 'reliable', 'transparent', 'strong', 'growing',
           'healthy', 'compliant', 'robust']
    n = sum(1 for w in neg if w in c)
    p = sum(1 for w in pos if w in c)
    if n > p + 1:
        return 'negative'
    if p > n + 1:
        return 'positive'
    if n > 0 and n > p:
        return 'concerned'
    return 'neutral'


def _assess_importance(context: str) -> str:
    """Assess importance of a metric mention."""
    c = context.lower()
    critical = ['critical', 'crucial', 'essential', 'must have', 'key factor',
                'primary concern', 'main risk', 'biggest issue', 'most important',
                'we need', 'should require', 'mandatory']
    dismiss = ["not important", "doesn't matter", "irrelevant", "ignore",
               "not a concern", "minimal risk", "negligible"]
    if any(p in c for p in critical):
        return 'critical'
    if any(p in c for p in dismiss):
        return 'dismissed'
    return 'mentioned'


# =============================================================================
# Storage
# =============================================================================

def store_post(post: ForumPost) -> Optional[int]:
    """Store a forum post and its extractions. Returns doc ID or None if exists."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM gov_documents WHERE source = %s AND source_id = %s",
                    (post.source, post.source_id),
                )
                row = cur.fetchone()
                if row:
                    return None

                cur.execute("""
                    INSERT INTO gov_documents
                        (source, source_id, url, title, body, author,
                         published_at, document_type, category, extra_data)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    post.source, post.source_id, post.url, post.title,
                    post.body, post.author, post.published_at,
                    post.document_type, post.category,
                    json.dumps({
                        "reply_count": post.reply_count,
                        "views": post.views,
                        "tags": post.tags or [],
                    }),
                ))
                doc_id = cur.fetchone()[0]

                coin_mentions = extract_stablecoins(f"{post.title} {post.body}")
                for m in coin_mentions:
                    cur.execute("""
                        INSERT INTO gov_stablecoin_mentions
                            (document_id, stablecoin, context, sentiment)
                        VALUES (%s, %s, %s, %s)
                    """, (doc_id, m["stablecoin"], m["context"], m["sentiment"]))

                metric_mentions = extract_metrics(f"{post.title} {post.body}")
                for m in metric_mentions:
                    cur.execute("""
                        INSERT INTO gov_metric_mentions
                            (document_id, metric_name, metric_category,
                             raw_text, importance_signal)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (doc_id, m["metric_name"], m["metric_category"],
                          m["raw_text"], m["importance_signal"]))

            return doc_id
    except Exception as e:
        logger.error(f"Failed to store post {post.source_id}: {e}")
        return None


# =============================================================================
# Synthesis Queries (collapsed from analysis/synthesis.py)
# =============================================================================

def get_hot_debates(days: int = 7, limit: int = 10) -> list[dict]:
    """Find hot governance debates with stablecoin mentions."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        d.id, d.title, d.source, d.url, d.published_at,
                        d.document_type,
                        COALESCE((d.extra_data->>'reply_count')::int, 0) as reply_count,
                        COALESCE((d.extra_data->>'views')::int, 0) as views,
                        array_agg(DISTINCT sm.stablecoin)
                            FILTER (WHERE sm.stablecoin IS NOT NULL) as stablecoins,
                        array_agg(DISTINCT sm.sentiment)
                            FILTER (WHERE sm.sentiment IS NOT NULL) as sentiments,
                        array_agg(DISTINCT mm.metric_category)
                            FILTER (WHERE mm.metric_category IS NOT NULL) as metrics
                    FROM gov_documents d
                    LEFT JOIN gov_stablecoin_mentions sm ON d.id = sm.document_id
                    LEFT JOIN gov_metric_mentions mm ON d.id = mm.document_id
                    WHERE d.published_at > NOW() - INTERVAL '%s days'
                    GROUP BY d.id
                    HAVING COUNT(sm.id) > 0
                    ORDER BY
                        COALESCE((d.extra_data->>'reply_count')::int, 0) DESC,
                        COUNT(sm.id) DESC
                    LIMIT %s
                """, (days, limit))

                cols = [c.name for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Hot debates query failed: {e}")
        return []


def get_sentiment_trends(days: int = 14) -> list[dict]:
    """Get stablecoin sentiment trends over time."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        sm.stablecoin,
                        sm.sentiment,
                        COUNT(*) as count,
                        DATE_TRUNC('day', d.published_at)::date as day
                    FROM gov_stablecoin_mentions sm
                    JOIN gov_documents d ON sm.document_id = d.id
                    WHERE d.published_at > NOW() - INTERVAL '%s days'
                    GROUP BY sm.stablecoin, sm.sentiment,
                             DATE_TRUNC('day', d.published_at)
                    ORDER BY day DESC
                """, (days,))

                cols = [c.name for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Sentiment trends query failed: {e}")
        return []


def get_metric_attention(days: int = 7) -> list[dict]:
    """What SII-relevant metrics are being discussed?"""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        mm.metric_category,
                        mm.metric_name,
                        mm.importance_signal,
                        COUNT(*) as mention_count,
                        array_agg(DISTINCT d.source) as sources
                    FROM gov_metric_mentions mm
                    JOIN gov_documents d ON mm.document_id = d.id
                    WHERE d.published_at > NOW() - INTERVAL '%s days'
                    GROUP BY mm.metric_category, mm.metric_name,
                             mm.importance_signal
                    ORDER BY mention_count DESC
                    LIMIT 20
                """, (days,))

                cols = [c.name for c in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Metric attention query failed: {e}")
        return []


def get_stats() -> dict:
    """Get governance intelligence stats."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM gov_documents")
                total_docs = cur.fetchone()[0]

                cur.execute("""
                    SELECT source, COUNT(*) FROM gov_documents
                    GROUP BY source ORDER BY COUNT(*) DESC
                """)
                by_source = {r[0]: r[1] for r in cur.fetchall()}

                cur.execute("SELECT COUNT(*) FROM gov_stablecoin_mentions")
                total_mentions = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM gov_metric_mentions")
                total_metrics = cur.fetchone()[0]

                cur.execute("""
                    SELECT stablecoin, COUNT(*) FROM gov_stablecoin_mentions
                    GROUP BY stablecoin ORDER BY COUNT(*) DESC LIMIT 10
                """)
                top_coins = {r[0]: r[1] for r in cur.fetchall()}

                return {
                    "total_documents": total_docs,
                    "documents_by_source": by_source,
                    "total_stablecoin_mentions": total_mentions,
                    "total_metric_mentions": total_metrics,
                    "top_stablecoins": top_coins,
                }
    except Exception as e:
        logger.error(f"Stats query failed: {e}")
        return {}


# =============================================================================
# Worker: Runs crawl as background task
# =============================================================================

def run_crawl(forums: list[str] = None, since_days: int = 30):
    """
    Run governance crawl. Called by worker on schedule.

    Args:
        forums: List of forum keys, or None for all.
        since_days: How far back to look.
    """
    forum_keys = forums or list(FORUMS.keys())
    total_new = 0

    for forum_key in forum_keys:
        logger.info(f"Crawling {forum_key}...")
        new_count = 0
        error_count = 0

        try:
            for post in crawl_forum(forum_key, since_days=since_days):
                doc_id = store_post(post)
                if doc_id:
                    new_count += 1
                    if new_count % 10 == 0:
                        logger.info(f"  {forum_key}: {new_count} new documents")
        except Exception as e:
            logger.error(f"Crawl error for {forum_key}: {e}")
            error_count += 1

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO gov_crawl_logs
                            (source, completed_at, documents_new, errors, status)
                        VALUES (%s, NOW(), %s, %s, %s)
                    """, (forum_key, new_count, error_count,
                          'completed' if error_count == 0 else 'completed_with_errors'))
        except Exception:
            pass

        total_new += new_count
        logger.info(f"Completed {forum_key}: {new_count} new documents")

    logger.info(f"Governance crawl complete: {total_new} new documents total")
    return total_new


# =============================================================================
# API Routes
# =============================================================================

def register_gov_routes(app):
    """Register governance intelligence API routes."""
    from fastapi import Query as FQuery

    @app.get("/api/governance/stats")
    async def gov_stats():
        """Governance intelligence overview."""
        return get_stats()

    @app.get("/api/governance/debates")
    async def gov_debates(days: int = FQuery(default=7, ge=1, le=90)):
        """Hot governance debates involving stablecoins."""
        debates = get_hot_debates(days=days)
        return {"debates": debates, "count": len(debates)}

    @app.get("/api/governance/sentiment")
    async def gov_sentiment(days: int = FQuery(default=14, ge=1, le=90)):
        """Stablecoin sentiment trends from governance forums."""
        trends = get_sentiment_trends(days=days)
        # Aggregate by coin
        by_coin = defaultdict(lambda: {"positive": 0, "negative": 0,
                                        "neutral": 0, "concerned": 0, "total": 0})
        for t in trends:
            coin = t["stablecoin"]
            sent = t.get("sentiment", "neutral")
            cnt = t.get("count", 0)
            if sent in by_coin[coin]:
                by_coin[coin][sent] += cnt
            by_coin[coin]["total"] += cnt

        return {
            "by_coin": dict(by_coin),
            "daily_trends": trends,
        }

    @app.get("/api/governance/metrics")
    async def gov_metrics(days: int = FQuery(default=7, ge=1, le=90)):
        """What risk metrics are governance contributors discussing?"""
        return {"metrics": get_metric_attention(days=days)}

    @app.post("/api/governance/crawl")
    async def gov_trigger_crawl(forums: list[str] = None, days: int = 30):
        """Manually trigger a governance crawl (runs synchronously)."""
        import threading
        def _crawl():
            run_crawl(forums=forums, since_days=days)
        t = threading.Thread(target=_crawl, daemon=True)
        t.start()
        return {"status": "crawl_started", "forums": forums or list(FORUMS.keys())}
