"""
Publisher — Social Renderer
==============================
Formats assessments for X/Twitter, Telegram, and Farcaster.
Disabled by default until social_enabled is True in config.
"""

import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def format_alert_text(assessment: dict) -> str:
    """Format an assessment as a plain-text alert for social channels."""
    addr = assessment.get("wallet_address", "")
    short_addr = f"{addr[:6]}...{addr[-4:]}" if len(addr) >= 10 else addr
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    trigger = assessment.get("trigger_type", "unknown")
    severity = assessment.get("severity", "silent").upper()
    score = assessment.get("wallet_risk_score")
    prev_score = assessment.get("wallet_risk_score_prev")
    hhi = assessment.get("concentration_hhi")
    prev_hhi = assessment.get("concentration_hhi_prev")

    lines = [f"BASIS {severity} · {now}", ""]
    lines.append(f"Wallet {short_addr}")

    # Trigger-specific detail
    detail = assessment.get("trigger_detail") or {}
    if trigger == "large_movement":
        direction = detail.get("direction", "?")
        movement = detail.get("movement_usd", 0)
        lines.append(f"Movement: ${movement:,.0f} ({direction})")
    elif trigger == "depeg":
        coin = detail.get("stablecoin_id", "?").upper()
        deviation = detail.get("deviation_pct", 0)
        lines.append(f"{coin} depeg: {deviation:.2f}% deviation")
    elif trigger == "concentration_shift":
        symbol = detail.get("symbol", "?").upper()
        prev_pct = detail.get("prev_pct", 0)
        curr_pct = detail.get("current_pct", 0)
        lines.append(f"{symbol} concentration: {prev_pct:.0f}% -> {curr_pct:.0f}%")
    elif trigger == "score_change":
        coin = detail.get("stablecoin_id", "?").upper()
        delta = detail.get("delta", 0)
        lines.append(f"{coin} SII moved {delta:+.1f} pts")

    lines.append("")

    if score is not None:
        score_str = f"Wallet risk: {score:.1f}"
        if prev_score is not None:
            score_str = f"Wallet risk: {prev_score:.1f} -> {score:.1f}"
        lines.append(score_str)

    if hhi is not None and prev_hhi is not None:
        lines.append(f"HHI: {prev_hhi:.2f} -> {hhi:.2f}")

    # Link to assessment page
    event_id = assessment.get("id", "")
    if event_id:
        lines.append("")
        lines.append(f"basis.protocol/assessment/{event_id}")

    return "\n".join(lines)


async def post_alert(assessment: dict) -> None:
    """
    Post assessment alert to social channels.
    Currently a no-op — social_enabled must be True in config.
    """
    text = format_alert_text(assessment)
    logger.info(f"Social alert formatted (not posted — social_enabled=False):\n{text}")

    # TODO: Post to X/Twitter via API v2
    # TODO: Post to Telegram bot channel
    # TODO: Post to Farcaster via Hub API
