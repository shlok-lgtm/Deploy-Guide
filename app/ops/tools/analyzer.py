"""
Analyzer tool — Claude API for worldview extraction, bridge finding, comment drafting.
"""
import asyncio
import os
import logging
import json
import httpx
from app.database import fetch_one, execute

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
CLAUDE_MODEL = "claude-sonnet-4-20250514"


async def _call_claude(system_prompt: str, user_prompt: str) -> str:
    """Call Claude API and return the text response."""
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set, skipping analysis")
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
                    "max_tokens": 2000,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": user_prompt}],
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return data["content"][0]["text"]
        except Exception as e:
            logger.error(f"Claude API call failed: {e}")
            return ""


def _load_worldview(target_name: str) -> str:
    """Load worldview file for a target if it exists."""
    import os
    safe_name = target_name.lower().replace(" ", "_").replace("/", "_")
    worldview_dir = os.path.join(os.path.dirname(__file__), "..", "worldviews")
    for fname in os.listdir(worldview_dir):
        if fname.endswith(".md") and safe_name in fname.lower():
            with open(os.path.join(worldview_dir, fname)) as f:
                return f.read()
    return ""


async def analyze_content(content_id: int):
    """
    Analyze a piece of scraped content:
    1. Extract worldview signals
    2. Find bridge to stablecoin quality scoring
    3. Draft a comment if bridge exists
    4. Score relevance

    Updates the ops_target_content record in place.
    """
    row = await asyncio.to_thread(
        fetch_one,
        """SELECT c.*, t.name as target_name, t.worldview_summary, t.gap, t.positioning
           FROM ops_target_content c
           LEFT JOIN ops_targets t ON c.target_id = t.id
           WHERE c.id = %s""",
        (content_id,),
    )
    if not row:
        logger.warning(f"Content {content_id} not found")
        return None

    # Load worldview context
    worldview_file = _load_worldview(row["target_name"]) if row["target_name"] else ""
    target_context = f"""
Target: {row.get('target_name', 'Unknown')}
Worldview: {row.get('worldview_summary', 'N/A')}
Gap: {row.get('gap', 'N/A')}
Positioning: {row.get('positioning', 'N/A')}
{worldview_file}
""".strip()

    system_prompt = """You are an analyst for Basis Protocol, a stablecoin integrity scoring system (SII).
Your job is to analyze content published by targets and find where stablecoin quality scoring
fits into their worldview. You must respond with valid JSON only, no markdown wrapping.

Output JSON with these fields:
- content_summary: 2-3 sentence summary of the content
- worldview_extract: what thesis/worldview does this content express (1-2 sentences)
- bridge_found: true/false — does this content create an opening for stablecoin quality scoring?
- bridge_text: if bridge_found, explain the bridge (how SII connects to their content)
- draft_comment: if bridge_found, draft a substantive comment (not promotional — adds value, asks good questions, or extends their argument using stablecoin quality as a lens). 2-4 sentences.
- comment_type: one of 'clarification', 'extension', 'stress_test', 'translation'
- engagement_action: one of 'comment', 'dm_trigger', 'artifact_trigger', 'skip'
- relevance_score: 0.0-1.0 how relevant this is for Basis engagement"""

    user_prompt = f"""Analyze this content from {row.get('target_name', 'a target')}:

TARGET CONTEXT:
{target_context}

CONTENT:
Title: {row.get('title', 'Untitled')}
Source: {row.get('source_url', '')}
Type: {row.get('source_type', '')}

{row.get('content', '')[:8000]}"""

    response = await _call_claude(system_prompt, user_prompt)
    if not response:
        return None

    # Parse JSON response
    try:
        # Handle potential markdown wrapping
        text = response.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0]
        analysis = json.loads(text)
    except json.JSONDecodeError:
        logger.error(f"Failed to parse analysis JSON for content {content_id}")
        return None

    # Update the content record
    await asyncio.to_thread(
        execute,
        """UPDATE ops_target_content SET
           analyzed = TRUE,
           content_summary = %s,
           worldview_extract = %s,
           bridge_found = %s,
           bridge_text = %s,
           draft_comment = %s,
           comment_type = %s,
           engagement_action = %s,
           relevance_score = %s
           WHERE id = %s""",
        (
            analysis.get("content_summary"),
            analysis.get("worldview_extract"),
            analysis.get("bridge_found", False),
            analysis.get("bridge_text"),
            analysis.get("draft_comment"),
            analysis.get("comment_type"),
            analysis.get("engagement_action"),
            analysis.get("relevance_score"),
            content_id,
        ),
    )

    return analysis
