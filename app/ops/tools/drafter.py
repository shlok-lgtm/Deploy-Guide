"""
Drafter tool — Claude API-powered personalized outreach generation.
Generates DMs, emails, and governance forum posts using target worldview context.
"""
import asyncio
import os
import json
import logging
import httpx
from app.database import fetch_one, fetch_all, execute

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL = "claude-sonnet-4-20250514"


async def _call_claude(system_prompt: str, user_prompt: str, max_tokens: int = 3000) -> str:
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set, skipping draft")
        return ""
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
                    "max_tokens": max_tokens,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_prompt}],
                },
            )
            resp.raise_for_status()
            return resp.json()["content"][0]["text"]
        except Exception as e:
            logger.error(f"Claude API call failed: {e}")
            return ""


def _load_worldview(target_name: str) -> str:
    safe_name = target_name.lower().replace(" ", "_").replace("/", "_")
    worldview_dir = os.path.join(os.path.dirname(__file__), "..", "worldviews")
    try:
        for fname in os.listdir(worldview_dir):
            if fname.endswith(".md") and safe_name in fname.lower():
                with open(os.path.join(worldview_dir, fname)) as f:
                    return f.read()
    except FileNotFoundError:
        pass
    return ""


def _get_target_context(target_id: int) -> dict:
    """Load target + contacts + recent engagement for drafting context."""
    target = fetch_one("SELECT * FROM ops_targets WHERE id = %s", (target_id,))
    if not target:
        return {}
    contacts = fetch_all(
        "SELECT * FROM ops_target_contacts WHERE target_id = %s", (target_id,)
    )
    recent_engagement = fetch_all(
        "SELECT * FROM ops_target_engagement_log WHERE target_id = %s ORDER BY created_at DESC LIMIT 5",
        (target_id,),
    )
    recent_content = fetch_all(
        "SELECT title, source_type, bridge_text, content_summary FROM ops_target_content WHERE target_id = %s AND analyzed = TRUE ORDER BY scraped_at DESC LIMIT 5",
        (target_id,),
    )
    return {
        "target": target,
        "contacts": contacts,
        "recent_engagement": recent_engagement,
        "recent_content": recent_content,
        "worldview_file": _load_worldview(target["name"]),
    }


def _get_live_sii_data() -> str:
    """Pull current SII scores for use in drafts."""
    rows = fetch_all(
        "SELECT stablecoin_id, overall_score, grade FROM scores ORDER BY overall_score DESC"
    )
    if not rows:
        return "No SII data available."
    lines = ["Current SII Rankings:"]
    for r in rows:
        lines.append(f"  {r['stablecoin_id']:8s} {r['overall_score']:5.1f} ({r['grade']})")
    return "\n".join(lines)


async def draft_dm(target_id: int, trigger_context: str) -> dict:
    """
    Generate a personalized DM draft for a target.
    trigger_context: what prompted the outreach (e.g., "published blog post about X",
    "liked our forum post", "Resolv exposure detected").
    """
    ctx = await asyncio.to_thread(_get_target_context, target_id)
    if not ctx.get("target"):
        return {"error": "Target not found"}

    t = ctx["target"]
    contacts = ctx["contacts"]
    contact_names = ", ".join(c["name"] for c in contacts) if contacts else "unknown contact"

    system_prompt = """You are drafting a DM on behalf of the founder of Basis Protocol, a stablecoin integrity scoring system.

Rules:
- Write in the founder's voice: direct, knowledgeable, not salesy
- Reference their specific work, not generic flattery
- Frame Basis as relevant to THEIR worldview, not ours
- Never use "risk rating" — use "score", "index", "quality signal"
- Keep it under 280 characters for Twitter DMs, under 500 for email
- Include a specific, low-commitment ask (look at something, react to something)
- Do not mention fundraising or investment

Output JSON:
{
  "twitter_dm": "short DM for Twitter (under 280 chars)",
  "email_subject": "email subject line",
  "email_body": "fuller email version (3-5 sentences)",
  "rationale": "why this framing works for this target"
}"""

    user_prompt = f"""Draft outreach to {t['name']}.

TARGET CONTEXT:
Name: {t['name']}
Type: {t['type']}
Worldview: {t.get('worldview_summary', 'N/A')}
Gap: {t.get('gap', 'N/A')}
Positioning: {t.get('positioning', 'N/A')}
Landmine: {t.get('landmine', 'N/A')}
First wedge: {t.get('first_wedge', 'N/A')}
Contact(s): {contact_names}

{ctx['worldview_file']}

TRIGGER: {trigger_context}

RECENT ENGAGEMENT:
{json.dumps([{"action": e["action_type"], "channel": e.get("channel"), "content": e.get("content")} for e in ctx.get("recent_engagement", [])], indent=2) if ctx.get("recent_engagement") else "No prior engagement"}

THEIR RECENT CONTENT:
{json.dumps([{"title": c["title"], "type": c["source_type"], "bridge": c.get("bridge_text")} for c in ctx.get("recent_content", [])], indent=2) if ctx.get("recent_content") else "No scraped content"}"""

    response = await _call_claude(system_prompt, user_prompt)
    if not response:
        return {"error": "Claude API failed"}

    try:
        text = response.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0]
        draft = json.loads(text)
    except json.JSONDecodeError:
        return {"error": "Failed to parse draft", "raw": response}

    return {"target_id": target_id, "target_name": t["name"], "trigger": trigger_context, "draft": draft}


async def draft_forum_post(
    forum: str,
    topic: str,
    target_id: int = None,
    include_sii_data: bool = True,
) -> dict:
    """
    Generate a governance-grade forum post.
    forum: 'aave', 'morpho', 'cow', 'ens', 'lido'
    topic: what the post is about
    """
    ctx = (await asyncio.to_thread(_get_target_context, target_id)) if target_id else {}
    sii_data = (await asyncio.to_thread(_get_live_sii_data)) if include_sii_data else ""

    forum_conventions = {
        "aave": "Aave governance forum (governance.aave.com). Posts follow ARC/ARFC/AIP structure. Be substantive, reference risk data, complement existing risk providers (Gauntlet, Sentora). Audience: delegates, risk teams, core contributors.",
        "morpho": "Morpho forum (forum.morpho.org). Curators are the audience. Focus on vault allocation quality, pre-allocation checks. Tone: quantitative, practical.",
        "cow": "CoW DAO forum (forum.cow.fi). Treasury management audience. Focus on stablecoin allocation in treasury strategies.",
        "ens": "ENS governance forum (discuss.ens.domains). Endowment management context. Steakhouse and karpatkey are active here.",
        "lido": "Lido research forum (research.lido.fi). EarnUSD vault strategy context. Focus on stablecoin quality for yield strategies.",
    }

    system_prompt = f"""You are drafting a governance forum post for the {forum_conventions.get(forum, forum + ' governance forum')}.

The post is by the founder of Basis Protocol — a stablecoin integrity scoring system (SII) that provides continuous, versioned quality scores for stablecoins.

Rules:
- Be substantive and technical, not promotional
- Lead with analysis and insight, not product pitch
- Reference specific data (SII scores, methodology)
- Frame as contributing to the community's existing discussion
- Use the forum's conventions and terminology
- Never use "risk rating" — use "score", "index", "quality signal"
- Include relevant SII data naturally, not as an advertisement
- End with a question or invitation for feedback, not a CTA

Output JSON:
{{
  "title": "forum post title",
  "body": "full forum post body in markdown",
  "tldr": "2-sentence summary",
  "tags": ["relevant", "tags"]
}}"""

    target_context = ""
    if ctx.get("target"):
        t = ctx["target"]
        target_context = f"""
TARGET CONTEXT (this post is relevant to engagement with {t['name']}):
Worldview: {t.get('worldview_summary', 'N/A')}
Gap: {t.get('gap', 'N/A')}
"""

    user_prompt = f"""Write a forum post for {forum} governance forum.

TOPIC: {topic}
{target_context}
{f'LIVE SII DATA:{chr(10)}{sii_data}' if sii_data else ''}

{ctx.get('worldview_file', '')}"""

    response = await _call_claude(system_prompt, user_prompt, max_tokens=4000)
    if not response:
        return {"error": "Claude API failed"}

    try:
        text = response.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0]
        draft = json.loads(text)
    except json.JSONDecodeError:
        return {"error": "Failed to parse draft", "raw": response}

    return {"forum": forum, "topic": topic, "target_id": target_id, "draft": draft}
