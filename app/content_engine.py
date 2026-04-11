"""
Basis Protocol — Content Engine
=================================
Connects governance crawler intel + SII live scores + 60-day content arc
to generate posting-ready content.

Input: What's hot in DeFi governance forums
Output: Drafted threads/posts with real SII data as evidence

Usage:
    python -m app.content_engine                    # Daily digest
    python -m app.content_engine --signal "USDT attestation debate on Aave"
    python -m app.content_engine --arc-day 44       # Scheduled arc content
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from dataclasses import dataclass, field, asdict

import httpx

logger = logging.getLogger(__name__)

# SII API (the app we just built — can be localhost or Replit URL)
SII_API_BASE = os.environ.get("SII_API_BASE", "http://localhost:5000")

# Claude API for draft generation
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

# Component name mapping: governance vocabulary → SII component IDs
GOVERNANCE_TO_SII = {
    "reserve_composition": ["reserve_to_supply_ratio", "cash_equivalents_pct"],
    "peg_stability": ["peg_current_deviation", "peg_7d_stddev", "peg_30d_stability"],
    "attestation": ["attestation_freshness", "attestation_frequency", "auditor_quality"],
    "liquidity": ["volume_24h", "volume_mcap_ratio", "curve_3pool_health"],
    "redemption": ["daily_turnover_ratio", "mcap_change_24h"],
    "collateral": ["reserve_to_supply_ratio", "cash_equivalents_pct"],
    "governance": ["governance_model", "team_transparency"],
    "smart_contract": ["contract_verified", "primary_chain_security"],
    "concentration": ["volume_concentration"],
    "regulatory": ["regulatory_status", "jurisdiction_clarity"],
    "oracle": [],  # Not yet implemented in collectors
    "depeg": ["peg_current_deviation", "depeg_events_30d", "max_drawdown_30d"],
    "ltv": ["lending_tvl", "lending_apy"],
    "risk": ["peg_7d_stddev", "cross_exchange_variance", "stress_performance"],
}

# Stablecoin name normalization
COIN_ALIASES = {
    "usdc": "usdc", "usd coin": "usdc", "circle": "usdc",
    "usdt": "usdt", "tether": "usdt",
    "dai": "dai", "makerdao": "dai", "maker": "dai", "sky": "dai",
    "frax": "frax",
    "pyusd": "pyusd", "paypal": "pyusd", "paxos": "pyusd",
    "fdusd": "fdusd", "first digital": "fdusd",
    "tusd": "tusd", "trueusd": "tusd",
    "usdd": "usdd", "tron": "usdd",
    "usde": "usde", "ethena": "usde",
}


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class GovernanceSignal:
    """A hot debate or trend from governance forums."""
    title: str
    source: str  # e.g. "aave", "makerdao"
    url: str
    published_at: str
    stablecoins: list  # coins discussed
    metrics: list  # SII-relevant metrics mentioned
    sentiment: str  # overall sentiment
    frustrations: list  # data gaps or pain points
    reply_count: int = 0
    key_quote: str = ""


@dataclass
class ContentOpportunity:
    """A content opportunity combining signal + data + arc."""
    signal: GovernanceSignal
    sii_data: dict  # relevant SII scores
    arc_match: Optional[dict] = None  # matching 60-day arc entry
    relevance_score: float = 0.0
    draft: str = ""


# =============================================================================
# 60-Day Content Arc (from project docs)
# =============================================================================

CONTENT_ARC = [
    {"day": 60, "phase": "The Ancients", "title": "The Opening Frame", "theme": "Risk measurement is civilization's hidden operating system", "keywords": ["probability", "pascal", "uncertainty"], "sii_connection": None},
    {"day": 58, "phase": "The Ancients", "title": "Quetelet's Ghost", "theme": "The invention of the average as a real thing", "keywords": ["average", "statistical", "index"], "sii_connection": None},
    {"day": 56, "phase": "The Ancients", "title": "Lloyd's Coffee House", "theme": "How strangers learned to trust each other's risk assessments", "keywords": ["trust", "insurance", "standardized"], "sii_connection": "attestation"},
    {"day": 54, "phase": "The Ancients", "title": "Ticker Tape Revolution", "theme": "Data infrastructure precedes index infrastructure", "keywords": ["data", "infrastructure", "real-time"], "sii_connection": "on-chain data"},
    {"day": 52, "phase": "The Ancients", "title": "Charles Dow's Insight", "theme": "The first modern market index as compression function", "keywords": ["compression", "legibility", "index"], "sii_connection": "TVL comparison"},
    {"day": 50, "phase": "The Ancients", "title": "Moody's Letter Grades", "theme": "The invention of credit ratings", "keywords": ["rating", "grade", "regulation"], "sii_connection": "grade scale"},
    {"day": 48, "phase": "The Ancients", "title": "The Basel Revelation", "theme": "Crisis creates demand for standardization", "keywords": ["crisis", "solvency", "capital ratio"], "sii_connection": "PSI concept"},
    {"day": 46, "phase": "The Ancients", "title": "Birth of Fear", "theme": "The VIX as volatility made indexable", "keywords": ["volatility", "vix", "fear"], "sii_connection": "CVI concept"},
    {"day": 44, "phase": "The Patterns", "title": "The Lifecycle", "theme": "Six-stage pattern of index emergence", "keywords": ["lifecycle", "standardization", "infrastructure"], "sii_connection": "current stage"},
    {"day": 42, "phase": "The Patterns", "title": "Regulation Creates the Gap", "theme": "Disclosure without interpretation as key trigger", "keywords": ["disclosure", "regulation", "methodology gap"], "sii_connection": "MiCA gap"},
    {"day": 40, "phase": "The Patterns", "title": "First Mover Advantage", "theme": "Legibility beats accuracy", "keywords": ["first mover", "standard", "network effects"], "sii_connection": "SII as first"},
    {"day": 38, "phase": "The Patterns", "title": "Trust Crisis Catalyst", "theme": "Crises legitimize new indices", "keywords": ["libor", "trust", "crisis", "reform"], "sii_connection": "Terra/FTX"},
    {"day": 36, "phase": "The Patterns", "title": "Performativity Problem", "theme": "Indices reshape markets they measure", "keywords": ["performativity", "TVL", "optimization"], "sii_connection": "TVL vs SII"},
    {"day": 34, "phase": "The Patterns", "title": "Power Follows Infrastructure", "theme": "Control over indices is control over markets", "keywords": ["MSCI", "power", "governance"], "sii_connection": "methodology governance"},
    {"day": 32, "phase": "The Patterns", "title": "The ESG Parallel", "theme": "Crypto is where ESG was in 2005", "keywords": ["ESG", "competing methodologies", "regulatory"], "sii_connection": "market timing"},
    {"day": 30, "phase": "The Patterns", "title": "The 24-36 Month Clock", "theme": "Regulatory timelines create urgency", "keywords": ["MiCA", "GENIUS Act", "deadline"], "sii_connection": "regulatory timeline"},
    {"day": 28, "phase": "The Parallels", "title": "Crypto's Cognitive Overload", "theme": "The complexity threshold has been crossed", "keywords": ["complexity", "illegible", "TVL"], "sii_connection": "102 components"},
    {"day": 26, "phase": "The Parallels", "title": "The Stablecoin Moment", "theme": "Why stablecoins are the right wedge", "keywords": ["stablecoin", "backbone", "collateral"], "sii_connection": "SII scores"},
    {"day": 24, "phase": "The Parallels", "title": "What March 2023 Revealed", "theme": "USDC depeg as trust crisis", "keywords": ["SVB", "USDC", "depeg", "banking"], "sii_connection": "backtest SVB"},
    {"day": 22, "phase": "The Parallels", "title": "Terra/Luna in Retrospect", "theme": "The $60B early warning that wasn't", "keywords": ["Terra", "Luna", "UST", "death spiral"], "sii_connection": "backtest Terra"},
    {"day": 20, "phase": "The Parallels", "title": "FTX Custody Problem", "theme": "When reserves aren't what they seem", "keywords": ["FTX", "reserves", "proof", "solvency"], "sii_connection": "attestation gap"},
    {"day": 18, "phase": "The Parallels", "title": "Missing Index Landscape", "theme": "Cataloging what crypto doesn't have", "keywords": ["missing", "index", "gap"], "sii_connection": "full surface list"},
    {"day": 16, "phase": "The Parallels", "title": "Regulatory Window", "theme": "The specific opportunity of 2024-2026", "keywords": ["MiCA", "MAS", "GENIUS", "window"], "sii_connection": "MiCA backtest"},
    {"day": 14, "phase": "The Gap", "title": "Why Incumbents Can't", "theme": "Structural conflicts prevent existing players", "keywords": ["S&P", "Gauntlet", "Chaos Labs", "conflict"], "sii_connection": "positioning"},
    {"day": 12, "phase": "The Gap", "title": "What a Real Index Requires", "theme": "The 102-component problem", "keywords": ["102", "component", "category"], "sii_connection": "component inventory"},
    {"day": 10, "phase": "The Gap", "title": "The Normalization Challenge", "theme": "Why raw data isn't enough", "keywords": ["normalization", "cadence", "format"], "sii_connection": "data pipeline"},
    {"day": 8, "phase": "The Gap", "title": "Credibility Bootstrapping", "theme": "Why this is hard to start", "keywords": ["credibility", "chicken-egg", "wedge"], "sii_connection": "GTM strategy"},
    {"day": 6, "phase": "The Inevitable", "title": "The Basis Thesis", "theme": "What we're building and why", "keywords": ["Basis", "risk surfaces", "SII"], "sii_connection": "full product"},
    {"day": 4, "phase": "The Inevitable", "title": "SII Architecture", "theme": "How it actually works", "keywords": ["formula", "weights", "components"], "sii_connection": "live demo"},
    {"day": 2, "phase": "The Inevitable", "title": "The Backtest", "theme": "We saw it coming", "keywords": ["backtest", "MiCA", "early warning"], "sii_connection": "106-day detection"},
    {"day": 1, "phase": "The Inevitable", "title": "The Standard Becomes Inevitable", "theme": "The closing frame", "keywords": ["standard", "inevitable", "Basis"], "sii_connection": "launch"},
]

INTERSTITIAL_TWEETS = [
    "Every major index emerges in the gap between disclosure mandates and methodology prescription.",
    "Indices don't passively measure markets. They actively constitute them.",
    "The first index is never the most accurate. It's the most legible.",
    "TVL is to DeFi what the Dow was to stocks in 1896: first, legible, and wrong in known ways.",
    "Stablecoins are $170B+. There's no standardized way to compare their risk profiles.",
    "Proof of reserves ≠ proof of solvency. A snapshot ≠ continuity. Attestation ≠ measurement.",
    "Whoever sets the methodology controls the outcome. That's not cynicism. It's 130 years of evidence.",
    "Disclosure doesn't equal meaning.",
]


# =============================================================================
# Governance Queries — delegates to app.governance (same DB)
# =============================================================================

def get_hot_debates(days: int = 7, limit: int = 10) -> list[dict]:
    from app.governance import get_hot_debates as _get
    return _get(days=days, limit=limit)

def get_sentiment_trends(days: int = 14) -> list[dict]:
    from app.governance import get_sentiment_trends as _get
    return _get(days=days)

def get_metric_attention(days: int = 7) -> list[dict]:
    from app.governance import get_metric_attention as _get
    return _get(days=days)


# =============================================================================
# SII Data Fetch
# =============================================================================

async def get_sii_scores() -> dict:
    """Fetch current SII scores from our own API."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{SII_API_BASE}/api/scores")
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        logger.error(f"SII API fetch failed: {e}")
    return {}


async def get_sii_detail(coin: str) -> dict:
    """Fetch detailed SII breakdown for one coin."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{SII_API_BASE}/api/scores/{coin}")
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        logger.error(f"SII detail fetch for {coin} failed: {e}")
    return {}


async def get_sii_comparison(coins: list[str]) -> dict:
    """Fetch side-by-side comparison."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{SII_API_BASE}/api/compare", params={"coins": ",".join(coins)})
            if r.status_code == 200:
                return r.json()
    except Exception as e:
        logger.error(f"SII comparison fetch failed: {e}")
    return {}


# =============================================================================
# Signal Detection & Matching
# =============================================================================

def detect_signals(days: int = 7) -> list[GovernanceSignal]:
    """Detect content-worthy signals from governance data."""
    debates = get_hot_debates(days=days)
    signals = []

    for d in debates:
        coins = [c.lower() for c in (d.get("stablecoins") or []) if c]
        metrics = [m for m in (d.get("metrics") or []) if m]
        sentiments = d.get("sentiments") or []
        frustrations = [f for f in (d.get("frustrations") or []) + (d.get("data_gaps") or []) if f]

        # Determine overall sentiment
        neg_count = sentiments.count("negative") + sentiments.count("concerned")
        pos_count = sentiments.count("positive")
        overall = "negative" if neg_count > pos_count else ("positive" if pos_count > neg_count else "neutral")

        signals.append(GovernanceSignal(
            title=d["title"],
            source=d.get("source", "unknown"),
            url=d.get("url", ""),
            published_at=str(d.get("published_at", "")),
            stablecoins=coins,
            metrics=metrics,
            sentiment=overall,
            frustrations=frustrations,
            reply_count=d.get("reply_count", 0),
        ))

    return signals


def match_arc_theme(signal: GovernanceSignal) -> Optional[dict]:
    """Find the best matching 60-day arc entry for a signal."""
    best_match = None
    best_score = 0

    signal_text = f"{signal.title} {' '.join(signal.metrics)} {' '.join(signal.frustrations)}".lower()

    for entry in CONTENT_ARC:
        score = 0
        for kw in entry["keywords"]:
            if kw.lower() in signal_text:
                score += 1

        # Boost if stablecoin-specific arc entries match the coins being discussed
        if entry.get("sii_connection"):
            conn = entry["sii_connection"].lower()
            for coin in signal.stablecoins:
                if coin in conn:
                    score += 2

        # Boost for metric category matches
        for metric in signal.metrics:
            metric_lower = metric.lower()
            for kw in entry["keywords"]:
                if kw.lower() in metric_lower or metric_lower in kw.lower():
                    score += 1

        if score > best_score:
            best_score = score
            best_match = entry

    return best_match if best_score >= 1 else None


async def build_opportunities(days: int = 7) -> list[ContentOpportunity]:
    """Build content opportunities from signals + SII data + arc."""
    signals = detect_signals(days=days)

    if not signals:
        logger.info("No governance signals found. Using arc-only content.")
        return []

    # Fetch SII scores once
    sii_all = await get_sii_scores()
    scores_by_id = {}
    for s in sii_all.get("stablecoins", []):
        sid = s.get("stablecoin_id", s.get("id", "")).lower()
        scores_by_id[sid] = s

    opportunities = []

    for signal in signals:
        # Get SII data for discussed coins
        sii_data = {}
        for coin in signal.stablecoins:
            normalized = COIN_ALIASES.get(coin.lower(), coin.lower())
            if normalized in scores_by_id:
                sii_data[normalized] = scores_by_id[normalized]
            else:
                # Try fetching detail
                detail = await get_sii_detail(normalized)
                if detail:
                    sii_data[normalized] = detail

        # Match to arc
        arc_match = match_arc_theme(signal)

        # Score relevance
        relevance = 0.0
        relevance += min(signal.reply_count / 20, 3.0)  # Activity (cap at 3)
        relevance += len(signal.stablecoins) * 0.5  # Coins discussed
        relevance += len(signal.metrics) * 0.5  # Metrics discussed
        relevance += len(signal.frustrations) * 1.0  # Frustrations = gold
        if signal.sentiment in ("negative", "concerned"):
            relevance += 1.0  # Negative sentiment = higher urgency
        if arc_match:
            relevance += 2.0  # Arc alignment bonus
        if sii_data:
            relevance += 1.5  # We have data to back it up

        opportunities.append(ContentOpportunity(
            signal=signal,
            sii_data=sii_data,
            arc_match=arc_match,
            relevance_score=round(relevance, 1),
        ))

    # Sort by relevance
    opportunities.sort(key=lambda x: x.relevance_score, reverse=True)
    return opportunities


# =============================================================================
# Draft Generation (Claude API)
# =============================================================================

SYSTEM_PROMPT = """You are a content strategist for Basis Protocol, which builds standardized risk surfaces for on-chain finance. The founder (Shlok) has 20+ years normalizing fragmented data into enterprise APIs.

Voice guidelines:
- Analytical, not promotional. Show the data, let it speak.
- Historical depth — reference how indices emerged in TradFi when relevant.
- Specific numbers, not vague claims. "USDT attestation score: 45/100" not "USDT has problems."
- Never attack issuers directly. Present comparative data neutrally.
- Thread format: Hook → Context → Data → Insight → Implication
- Tone: Thoughtful practitioner, not crypto influencer. No emojis. No hype.
- End threads with a question or observation, not a call to action.

Key phrases to weave in naturally (not forced):
- "Disclosure doesn't equal meaning"
- "Standardized risk surfaces"
- "The coordination tax"
- "Measure once, apply many times"

DO NOT:
- Shill Basis directly (let the data create the pull)
- Make price predictions
- Attack specific issuers
- Use buzzwords like "revolutionary" or "game-changing"
- Include hashtags
"""


async def generate_draft(opportunity: ContentOpportunity) -> str:
    """Generate a thread draft using Claude API."""
    if not ANTHROPIC_API_KEY:
        return _generate_draft_template(opportunity)

    # Build the prompt
    sii_context = ""
    for coin, data in opportunity.sii_data.items():
        score = data.get("sii_score", data.get("score", "?"))
        sii_context += f"\n- {coin.upper()}: SII {score}"
        cats = data.get("categories", data.get("category_scores", {}))
        if cats:
            for cat_name, cat_val in cats.items():
                if isinstance(cat_val, dict):
                    sii_context += f"\n  {cat_name}: {cat_val.get('score', '?')}"
                else:
                    sii_context += f"\n  {cat_name}: {cat_val}"

    arc_context = ""
    if opportunity.arc_match:
        arc_context = f"""
Relevant content arc theme (Day {opportunity.arc_match['day']}): "{opportunity.arc_match['title']}"
Theme: {opportunity.arc_match['theme']}
Phase: {opportunity.arc_match['phase']}
"""

    user_prompt = f"""Generate a Twitter/X thread (4-7 tweets) based on this governance debate signal.

GOVERNANCE SIGNAL:
- Title: {opportunity.signal.title}
- Source: {opportunity.signal.source} governance forum
- Stablecoins discussed: {', '.join(opportunity.signal.stablecoins)}
- Metrics discussed: {', '.join(opportunity.signal.metrics)}
- Sentiment: {opportunity.signal.sentiment}
- Frustrations expressed: {'; '.join(opportunity.signal.frustrations) if opportunity.signal.frustrations else 'none explicitly'}
- Reply count: {opportunity.signal.reply_count}

SII DATA (use these real numbers):
{sii_context or "No SII data available — use general framing"}

{arc_context}

Write a thread that:
1. Opens with the governance debate (what practitioners are actually discussing)
2. Shows why standardized measurement matters in this context
3. Uses the real SII data as evidence (specific numbers)
4. Connects to the broader pattern of how risk indices emerge
5. Ends with an insight, not a sales pitch

Format each tweet as "1/ ...", "2/ ...", etc. Keep each under 280 chars.
"""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 1500,
                    "system": SYSTEM_PROMPT,
                    "messages": [{"role": "user", "content": user_prompt}],
                },
            )
            if r.status_code == 200:
                data = r.json()
                return data["content"][0]["text"]
            else:
                logger.error(f"Claude API error: {r.status_code} {r.text[:200]}")
    except Exception as e:
        logger.error(f"Draft generation failed: {e}")

    return _generate_draft_template(opportunity)


def _generate_draft_template(opp: ContentOpportunity) -> str:
    """Fallback template when Claude API isn't available."""
    coins = ", ".join(c.upper() for c in opp.signal.stablecoins)
    metrics = ", ".join(opp.signal.metrics) if opp.signal.metrics else "risk assessment"

    lines = [
        f"1/ {opp.signal.source.title()} governance is debating {metrics} for {coins}.",
        f"",
        f"The conversation has {opp.signal.reply_count} replies. Here's what the data shows 🧵",
        f"",
    ]

    tweet_num = 2
    for coin, data in opp.sii_data.items():
        score = data.get("sii_score", data.get("score", "?"))
        lines.append(f"{tweet_num}/ {coin.upper()}: SII Score {score}")
        tweet_num += 1

    if len(opp.sii_data) >= 2:
        scores = [(c, d.get("sii_score", d.get("score", 0))) for c, d in opp.sii_data.items()]
        scores.sort(key=lambda x: x[1], reverse=True)
        gap = scores[0][1] - scores[-1][1]
        if gap > 5:
            lines.append(f"")
            lines.append(f"{tweet_num}/ The {gap:.0f}-point gap between {scores[0][0].upper()} and {scores[-1][0].upper()} reflects what the governance debate is feeling but can't yet quantify.")
            tweet_num += 1

    if opp.signal.frustrations:
        lines.append(f"")
        lines.append(f"{tweet_num}/ The frustration: \"{opp.signal.frustrations[0]}\"")
        lines.append(f"")
        lines.append(f"Disclosure doesn't equal meaning. Standardized measurement does.")

    if opp.arc_match:
        lines.append(f"")
        lines.append(f"[Arc theme: Day {opp.arc_match['day']} — \"{opp.arc_match['title']}\"]")

    return "\n".join(lines)


# =============================================================================
# Digest Output
# =============================================================================

async def generate_digest(days: int = 7, top_n: int = 3) -> str:
    """Generate a daily content digest."""
    opportunities = await build_opportunities(days=days)

    lines = [
        f"# Basis Content Digest — {datetime.now().strftime('%Y-%m-%d')}",
        f"",
        f"Governance signals from the last {days} days, matched with SII data.",
        f"",
        f"---",
        f"",
    ]

    if not opportunities:
        # No signals — suggest arc content
        today_offset = (datetime.now() - datetime(2025, 6, 1)).days  # Adjust launch date
        arc_today = None
        for entry in CONTENT_ARC:
            if entry["day"] <= 60:  # Just pick next unposted
                arc_today = entry
                break

        lines.append("## No Hot Governance Signals Today")
        lines.append("")
        lines.append("**Suggested: Post scheduled arc content**")
        lines.append("")
        if arc_today:
            lines.append(f"### Day {arc_today['day']}: {arc_today['title']}")
            lines.append(f"*Phase: {arc_today['phase']}*")
            lines.append(f"Theme: {arc_today['theme']}")
        lines.append("")
        lines.append("**Or use an interstitial tweet:**")
        import random
        lines.append(f"> {random.choice(INTERSTITIAL_TWEETS)}")
        return "\n".join(lines)

    # Top opportunities
    for i, opp in enumerate(opportunities[:top_n]):
        lines.append(f"## 🎯 Signal #{i+1} (Relevance: {opp.relevance_score})")
        lines.append(f"")
        lines.append(f"**Source:** {opp.signal.source} — [{opp.signal.title}]({opp.signal.url})")
        lines.append(f"**Coins:** {', '.join(c.upper() for c in opp.signal.stablecoins)}")
        lines.append(f"**Metrics:** {', '.join(opp.signal.metrics) if opp.signal.metrics else 'general'}")
        lines.append(f"**Sentiment:** {opp.signal.sentiment}")
        lines.append(f"**Replies:** {opp.signal.reply_count}")
        if opp.signal.frustrations:
            lines.append(f"**Frustrations:** {'; '.join(opp.signal.frustrations)}")
        lines.append(f"")

        # SII data
        if opp.sii_data:
            lines.append("**SII Data:**")
            for coin, data in opp.sii_data.items():
                score = data.get("sii_score", data.get("score", "?"))
                lines.append(f"- {coin.upper()}: {score}")
            lines.append("")

        # Arc match
        if opp.arc_match:
            lines.append(f"**Arc Match:** Day {opp.arc_match['day']} — \"{opp.arc_match['title']}\" ({opp.arc_match['phase']})")
            lines.append(f"")

        # Draft
        if opp.draft:
            lines.append("**Draft Thread:**")
            lines.append("```")
            lines.append(opp.draft)
            lines.append("```")
        lines.append("")
        lines.append("---")
        lines.append("")

    # Sentiment overview
    trends = get_sentiment_trends(days=14)
    if trends:
        lines.append("## Sentiment Trends (14d)")
        coin_sentiments = {}
        for t in trends:
            coin = t["stablecoin"]
            if coin not in coin_sentiments:
                coin_sentiments[coin] = {"positive": 0, "negative": 0, "neutral": 0, "concerned": 0}
            sent = t.get("sentiment", "neutral")
            if sent in coin_sentiments[coin]:
                coin_sentiments[coin][sent] += t.get("count", 0)

        for coin, sents in sorted(coin_sentiments.items()):
            neg = sents.get("negative", 0) + sents.get("concerned", 0)
            pos = sents.get("positive", 0)
            total = sum(sents.values())
            if total > 0:
                lines.append(f"- **{coin}**: {total} mentions ({neg} negative, {pos} positive)")
        lines.append("")

    # Metric attention
    metrics = get_metric_attention(days=7)
    if metrics:
        lines.append("## Hot Metrics This Week")
        for m in metrics[:5]:
            lines.append(f"- {m['metric_name']} ({m['mention_count']} mentions) — {', '.join(m.get('sources', []))}")
        lines.append("")

    return "\n".join(lines)


# =============================================================================
# API Endpoint (added to server)
# =============================================================================

def register_content_routes(app):
    """Register content engine routes on the FastAPI app."""
    from fastapi import Query as FQuery

    @app.get("/api/content/digest")
    async def content_digest(days: int = FQuery(default=7, ge=1, le=30)):
        """Generate content digest from governance signals + SII data."""
        digest = await generate_digest(days=days)
        opportunities = await build_opportunities(days=days)
        return {
            "digest_markdown": digest,
            "opportunity_count": len(opportunities),
            "top_opportunities": [
                {
                    "title": o.signal.title,
                    "source": o.signal.source,
                    "stablecoins": o.signal.stablecoins,
                    "metrics": o.signal.metrics,
                    "sentiment": o.signal.sentiment,
                    "relevance": o.relevance_score,
                    "arc_match": o.arc_match["title"] if o.arc_match else None,
                    "sii_coins": list(o.sii_data.keys()),
                }
                for o in opportunities[:5]
            ],
            "generated_at": datetime.now().isoformat(),
        }

    @app.post("/api/content/draft")
    async def content_draft(signal: dict):
        """Generate a thread draft from a manual signal.
        
        POST body: {"signal": "USDT attestation debate on Aave, 15 replies, negative sentiment"}
        """
        text = signal.get("signal", "")
        if not text:
            return {"error": "Provide a 'signal' field"}

        # Parse the manual signal into a GovernanceSignal
        detected_coins = []
        for alias, canonical in COIN_ALIASES.items():
            if alias in text.lower():
                if canonical not in detected_coins:
                    detected_coins.append(canonical)

        detected_metrics = []
        for metric_key in GOVERNANCE_TO_SII:
            if metric_key.replace("_", " ") in text.lower() or metric_key in text.lower():
                detected_metrics.append(metric_key)

        manual_signal = GovernanceSignal(
            title=text[:120],
            source="manual",
            url="",
            published_at=datetime.now().isoformat(),
            stablecoins=detected_coins or ["usdt", "usdc"],
            metrics=detected_metrics or ["risk"],
            sentiment="negative" if any(w in text.lower() for w in ["concern", "negative", "risk", "problem", "issue", "debate"]) else "neutral",
            frustrations=[],
        )

        # Get SII data
        sii_data = {}
        for coin in manual_signal.stablecoins:
            detail = await get_sii_detail(coin)
            if detail:
                sii_data[coin] = detail

        arc_match = match_arc_theme(manual_signal)

        opp = ContentOpportunity(
            signal=manual_signal,
            sii_data=sii_data,
            arc_match=arc_match,
            relevance_score=5.0,
        )

        draft = await generate_draft(opp)

        return {
            "signal": text,
            "detected_coins": detected_coins,
            "detected_metrics": detected_metrics,
            "arc_match": arc_match["title"] if arc_match else None,
            "draft": draft,
            "sii_data": {k: {"score": v.get("sii_score", v.get("score"))} for k, v in sii_data.items()},
        }

    @app.get("/api/content/arc/{day}")
    async def content_arc_day(day: int):
        """Get arc content for a specific day."""
        entry = next((e for e in CONTENT_ARC if e["day"] == day), None)
        if not entry:
            return {"error": f"No arc entry for day {day}", "available_days": [e["day"] for e in CONTENT_ARC]}

        # If the arc entry has an SII connection, fetch relevant data
        sii_data = {}
        if entry.get("sii_connection"):
            scores = await get_sii_scores()
            for s in scores.get("stablecoins", [])[:4]:
                sid = s.get("stablecoin_id", s.get("id", ""))
                sii_data[sid] = s

        return {
            "day": entry["day"],
            "phase": entry["phase"],
            "title": entry["title"],
            "theme": entry["theme"],
            "sii_connection": entry.get("sii_connection"),
            "sii_data": sii_data,
            "suggested_interstitial": INTERSTITIAL_TWEETS[day % len(INTERSTITIAL_TWEETS)],
        }


# =============================================================================
# CLI
# =============================================================================

async def main_cli():
    import argparse
    parser = argparse.ArgumentParser(description="Basis Content Engine")
    parser.add_argument("command", choices=["digest", "signals", "arc", "draft"], default="digest", nargs="?")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--day", type=int, help="Arc day number")
    parser.add_argument("--signal", type=str, help="Manual signal text")
    parser.add_argument("--generate-drafts", action="store_true", help="Generate Claude drafts (requires ANTHROPIC_API_KEY)")
    args = parser.parse_args()

    if args.command == "signals":
        signals = detect_signals(days=args.days)
        if not signals:
            print("No governance signals found.")
            return
        for i, s in enumerate(signals):
            print(f"\n--- Signal {i+1} ---")
            print(f"Title: {s.title}")
            print(f"Source: {s.source}")
            print(f"Coins: {', '.join(s.stablecoins)}")
            print(f"Metrics: {', '.join(s.metrics)}")
            print(f"Sentiment: {s.sentiment}")
            print(f"Replies: {s.reply_count}")

    elif args.command == "arc":
        day = args.day or 44
        entry = next((e for e in CONTENT_ARC if e["day"] == day), None)
        if entry:
            print(f"\nDay {entry['day']}: {entry['title']}")
            print(f"Phase: {entry['phase']}")
            print(f"Theme: {entry['theme']}")
            print(f"SII Connection: {entry.get('sii_connection', 'none')}")
        else:
            print(f"No arc entry for day {day}")
            print(f"Available: {[e['day'] for e in CONTENT_ARC]}")

    elif args.command == "draft":
        text = args.signal or "USDT attestation debate on Aave governance"
        print(f"Generating draft for: {text}")

        detected_coins = [COIN_ALIASES[a] for a in COIN_ALIASES if a in text.lower()]
        detected_coins = list(set(detected_coins)) or ["usdt", "usdc"]

        signal = GovernanceSignal(
            title=text, source="manual", url="", published_at=datetime.now().isoformat(),
            stablecoins=detected_coins, metrics=["risk"], sentiment="neutral", frustrations=[],
        )

        sii_data = {}
        for coin in signal.stablecoins:
            detail = await get_sii_detail(coin)
            if detail:
                sii_data[coin] = detail

        opp = ContentOpportunity(signal=signal, sii_data=sii_data, arc_match=match_arc_theme(signal))

        if args.generate_drafts:
            draft = await generate_draft(opp)
        else:
            draft = _generate_draft_template(opp)

        print(f"\n{draft}")

    else:  # digest
        digest = await generate_digest(days=args.days)

        if args.generate_drafts:
            opps = await build_opportunities(days=args.days)
            for opp in opps[:3]:
                opp.draft = await generate_draft(opp)
            digest = await generate_digest(days=args.days)

        print(digest)


if __name__ == "__main__":
    asyncio.run(main_cli())
