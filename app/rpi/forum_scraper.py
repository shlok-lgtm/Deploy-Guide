"""
RPI Governance Forum Scraper
==============================
Scrapes Discourse-based governance forums for risk vendor mentions,
budget amounts, and incident reports. Reuses the Discourse crawler
from app/governance.py but stores results in the RPI-specific
governance_forum_posts table for lens component automation.

Automates:
- vendor_diversity (risk_organization lens) — count distinct active vendors
- spend_ratio refinement (base component) — extract budget amounts
"""

import re
import logging
import time
from datetime import datetime, timezone, timedelta

import httpx

from app.database import execute, fetch_one, fetch_all

logger = logging.getLogger(__name__)

RATE_LIMIT_DELAY = 2.0

# Known risk vendors — matched case-insensitively in post text
RISK_VENDORS = [
    "gauntlet", "chaos labs", "chaos-labs", "llamarisk", "llama risk",
    "warden finance", "wardenfinance", "openzeppelin", "open zeppelin",
    "trail of bits", "trailofbits", "certora", "nethermind",
    "sigma prime", "sigmaprime", "consensys diligence", "quantstamp",
    "halborn", "ottersec", "otter sec", "neodyme", "spearbit",
    "immunefi", "code4rena", "code 4rena", "sherlock", "cantina",
    "ba labs", "phoenix labs", "steakhouse", "block analitica",
    "blockanalitica", "aave companies", "bgd labs",
]

# Incident keywords
INCIDENT_KEYWORDS = [
    "incident", "exploit", "loss", "vulnerability", "post-mortem",
    "postmortem", "bad debt", "liquidation error", "erroneous liquidation",
    "hack", "breach", "emergency", "compensation", "shortfall",
    "oracle failure", "oracle misconfiguration",
]

# Budget extraction patterns
BUDGET_REGEX = [
    r'\$[\d,]+(?:\.\d+)?(?:\s*[MmKk](?:illion|illion)?)?',
    r'[\d,]+(?:\.\d+)?\s*(?:USDC|USDT|DAI|USD)\b',
    r'[\d,]+(?:\.\d+)?\s*(?:million|Million|M)\s*(?:USD|USDC|dollars)?',
]

# Forum configurations — extends app/governance.py FORUMS with RPI-specific protocols
# Maps protocol_slug to Discourse forum details
RPI_FORUMS = {
    "aave": {
        "base_url": "https://governance.aave.com",
        "categories": [4, 7, 6],  # governance, risk, other
    },
    "sky": {
        "base_url": "https://forum.makerdao.com",
        "categories": [89, 84, 103, 104, 94],
    },
    "compound-finance": {
        "base_url": "https://www.comp.xyz",
        "categories": [5, 6, 9],
    },
    "morpho": {
        "base_url": "https://forum.morpho.org",
        "categories": [26, 23, 17],
    },
    "uniswap": {
        "base_url": "https://gov.uniswap.org",
        "categories": [6, 7, 8],  # governance, temperature-check, proposal-discussion
    },
    "curve-finance": {
        "base_url": "https://gov.curve.fi",
        "categories": [4, 5],  # governance, proposals
    },
    "lido": {
        "base_url": "https://research.lido.fi",
        "categories": [5, 6, 7],  # governance, research, node-operators
    },
    "eigenlayer": {
        "base_url": "https://forum.eigenlayer.xyz",
        "categories": [4, 5],
    },
}


def _extract_vendors(text: str) -> list[str]:
    """Extract mentioned risk vendors from text (case-insensitive)."""
    text_lower = text.lower()
    found = []
    for vendor in RISK_VENDORS:
        if vendor.lower() in text_lower:
            # Normalize vendor name
            normalized = vendor.replace("-", " ").replace("  ", " ").title()
            if normalized not in found:
                found.append(normalized)
    return found


def _has_incident_mention(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in INCIDENT_KEYWORDS)


def _has_budget_mention(text: str) -> bool:
    text_lower = text.lower()
    budget_context = ["budget", "compensation", "payment", "funding",
                      "grant", "renewal", "stream", "allocation"]
    return any(kw in text_lower for kw in budget_context)


def _extract_budget_amount(text: str) -> float | None:
    """Extract the largest dollar amount from budget-related text."""
    amounts = []
    for pattern in BUDGET_REGEX:
        for match in re.finditer(pattern, text):
            raw = match.group()
            num_str = re.sub(r'[^\d.,]', '', raw.split()[0] if ' ' in raw else raw)
            num_str = num_str.replace(',', '')
            try:
                val = float(num_str)
                if 'M' in raw or 'million' in raw.lower():
                    val *= 1_000_000
                elif 'K' in raw or 'k' in raw:
                    val *= 1_000
                if 1_000 <= val <= 100_000_000:
                    amounts.append(val)
            except ValueError:
                continue
    return max(amounts) if amounts else None


def _strip_html(html: str) -> str:
    text = re.sub(r'<[^>]+>', ' ', html)
    return re.sub(r'\s+', ' ', text).strip()


def _fetch_discourse_topics(base_url: str, category_id: int,
                            since_days: int = 90, max_pages: int = 5) -> list[dict]:
    """Fetch topic list from a Discourse category."""
    topics = []
    since_date = datetime.now(timezone.utc) - timedelta(days=since_days)

    for page in range(max_pages):
        time.sleep(RATE_LIMIT_DELAY)
        url = f"{base_url}/c/{category_id}/l/latest.json?page={page}"
        try:
            resp = httpx.get(url, timeout=15, follow_redirects=True)
            if resp.status_code != 200:
                break
            data = resp.json()
            topic_list = data.get("topic_list", {}).get("topics", [])
            if not topic_list:
                break

            for t in topic_list:
                created = t.get("created_at", "")
                if created and created > since_date.isoformat():
                    topics.append(t)
                elif created:
                    return topics  # past our window, stop
        except Exception as e:
            logger.debug(f"Discourse fetch failed {url}: {e}")
            break

    return topics


def _fetch_topic_posts(base_url: str, topic_id: int) -> list[dict]:
    """Fetch the first post of a topic (the OP)."""
    time.sleep(RATE_LIMIT_DELAY)
    try:
        resp = httpx.get(f"{base_url}/t/{topic_id}.json", timeout=15, follow_redirects=True)
        if resp.status_code == 200:
            data = resp.json()
            posts = data.get("post_stream", {}).get("posts", [])
            return posts[:1]  # just the OP
    except Exception as e:
        logger.debug(f"Topic fetch failed {base_url}/t/{topic_id}: {e}")
    return []


def scrape_forum(protocol_slug: str, config: dict = None,
                 since_days: int = 90) -> int:
    """Scrape a single protocol's governance forum. Returns count of posts stored."""
    if config is None:
        config = RPI_FORUMS.get(protocol_slug)
    if not config:
        return 0

    base_url = config["base_url"].rstrip("/")
    categories = config.get("categories", [])
    stored = 0

    for cat_id in categories:
        topics = _fetch_discourse_topics(base_url, cat_id, since_days)
        logger.info(f"RPI forum: {protocol_slug} cat={cat_id} — {len(topics)} topics")

        for topic in topics:
            topic_id = topic.get("id")
            title = topic.get("title", "")

            # Fetch OP to get body text
            posts = _fetch_topic_posts(base_url, topic_id)
            if not posts:
                continue
            op = posts[0]
            body_raw = op.get("cooked", "") or op.get("raw", "")
            body_text = _strip_html(body_raw)
            body_excerpt = body_text[:500]

            full_text = f"{title} {body_text}"
            vendors = _extract_vendors(full_text)
            has_incident = _has_incident_mention(full_text)
            has_budget = _has_budget_mention(full_text)
            budget_amount = _extract_budget_amount(full_text) if has_budget else None

            post_id = str(op.get("id", topic_id))
            posted_at = op.get("created_at") or topic.get("created_at")

            try:
                execute("""
                    INSERT INTO governance_forum_posts
                        (protocol_slug, forum_url, post_id, topic_id, title,
                         body_excerpt, author, category,
                         mentions_risk_vendor, mentioned_vendors,
                         mentions_incident, mentions_budget,
                         extracted_budget_amount, posted_at)
                    VALUES (%s, %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s,
                            %s, %s)
                    ON CONFLICT (protocol_slug, post_id) DO UPDATE SET
                        mentions_risk_vendor = EXCLUDED.mentions_risk_vendor,
                        mentioned_vendors = EXCLUDED.mentioned_vendors,
                        mentions_incident = EXCLUDED.mentions_incident,
                        mentions_budget = EXCLUDED.mentions_budget,
                        extracted_budget_amount = EXCLUDED.extracted_budget_amount
                """, (
                    protocol_slug, base_url, post_id, str(topic_id), title,
                    body_excerpt, op.get("username", ""), str(cat_id),
                    bool(vendors), vendors if vendors else None,
                    has_incident, has_budget,
                    budget_amount, posted_at,
                ))
                stored += 1
            except Exception as e:
                logger.debug(f"Failed to store forum post {post_id}: {e}")

    return stored


def scrape_all_forums(since_days: int = 90) -> dict[str, int]:
    """Scrape all configured governance forums. Returns dict of slug -> post count."""
    # Also load from rpi_protocol_config for expanded protocols
    db_configs = {}
    try:
        rows = fetch_all("""
            SELECT protocol_slug, governance_forum_url
            FROM rpi_protocol_config
            WHERE governance_forum_url IS NOT NULL AND enabled = TRUE
        """)
        for r in rows:
            if r["protocol_slug"] not in RPI_FORUMS:
                db_configs[r["protocol_slug"]] = {
                    "base_url": r["governance_forum_url"],
                    "categories": [4, 5, 6],  # sensible defaults
                }
    except Exception:
        pass

    all_configs = {**RPI_FORUMS, **db_configs}
    results = {}
    for slug, config in all_configs.items():
        try:
            count = scrape_forum(slug, config, since_days)
            results[slug] = count
            logger.info(f"RPI forum scraper: {slug} — {count} posts stored")
        except Exception as e:
            logger.warning(f"Forum scrape failed for {slug}: {e}")
            results[slug] = 0

    return results


def update_vendor_diversity_lens():
    """Update the vendor_diversity lens component from forum data.

    Counts distinct risk vendors mentioned in recent forum posts
    and governance proposals for each protocol.
    """
    from app.index_definitions.rpi_v2 import RPI_TARGET_PROTOCOLS

    updated = 0
    for slug in RPI_TARGET_PROTOCOLS:
        # Collect vendors from forum posts (last 180 days)
        forum_vendors = set()
        rows = fetch_all("""
            SELECT mentioned_vendors
            FROM governance_forum_posts
            WHERE protocol_slug = %s
              AND mentions_risk_vendor = TRUE
              AND posted_at >= NOW() - INTERVAL '180 days'
        """, (slug,))
        for r in rows:
            if r.get("mentioned_vendors"):
                vendors = r["mentioned_vendors"]
                if isinstance(vendors, list):
                    forum_vendors.update(v.lower() for v in vendors)

        # Also check governance_proposals for vendor keywords
        prop_rows = fetch_all("""
            SELECT risk_keywords
            FROM governance_proposals
            WHERE protocol_slug = %s
              AND is_risk_related = TRUE
              AND created_at >= NOW() - INTERVAL '180 days'
        """, (slug,))
        vendor_keywords_in_proposals = {"gauntlet", "chaos", "llamarisk", "openzeppelin",
                                        "warden", "certora", "nethermind", "trail of bits"}
        for r in prop_rows:
            kws = r.get("risk_keywords", []) or []
            for kw in kws:
                if kw.lower() in vendor_keywords_in_proposals:
                    forum_vendors.add(kw.lower())

        vendor_count = len(forum_vendors)
        if vendor_count > 0:
            # Normalize: 0→0, 1→30, 2→60, 3+→80
            if vendor_count == 1:
                score = 30.0
            elif vendor_count == 2:
                score = 60.0
            else:
                score = 80.0

            execute("""
                INSERT INTO rpi_components
                    (protocol_slug, component_id, component_type, lens_id,
                     raw_value, normalized_score, source_type, data_source,
                     metadata, collected_at)
                VALUES (%s, 'vendor_diversity', 'lens', 'risk_organization',
                        %s, %s, 'automated', 'governance_forums',
                        %s, NOW())
            """, (slug, vendor_count, score,
                  f'{{"vendors": {list(forum_vendors)}}}'.replace("'", '"')))
            updated += 1
            logger.info(f"RPI vendor_diversity: {slug} = {vendor_count} vendors {list(forum_vendors)}")

    return updated
