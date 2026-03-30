"""
Verification Agent — Watcher
==============================
Monitors the wallet risk graph and SII scores for material state changes.
Generates assessment events when trigger conditions are met.
"""

import logging
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one
from app.agent.config import AGENT_CONFIG
from app.agent.assessor import generate_assessment, _get_sii_scores
from app.agent.classifier import classify_severity
from app.agent.store import store_assessment

logger = logging.getLogger(__name__)


def _get_previous_assessment(wallet_address: str) -> dict | None:
    """Get the most recent assessment for a wallet (used as baseline)."""
    row = fetch_one("""
        SELECT wallet_risk_score, wallet_risk_grade, concentration_hhi,
               coverage_ratio, total_stablecoin_value, holdings_snapshot
        FROM assessment_events
        WHERE wallet_address = %s
        ORDER BY created_at DESC LIMIT 1
    """, (wallet_address,))
    return dict(row) if row else None


def _broadcasts_today() -> int:
    """Count how many broadcasts have been sent today."""
    row = fetch_one("""
        SELECT COUNT(*) AS cnt FROM assessment_events
        WHERE broadcast = TRUE
        AND created_at > CURRENT_DATE
    """)
    return row["cnt"] if row else 0


def _process_assessment(wallet_address: str, trigger_type: str,
                        trigger_detail: dict, sii_scores: dict,
                        config: dict) -> dict | None:
    """Generate, classify, and store a single assessment."""
    previous = _get_previous_assessment(wallet_address)

    assessment = generate_assessment(
        wallet_address=wallet_address,
        trigger_type=trigger_type,
        trigger_detail=trigger_detail,
        previous_assessment=previous,
        sii_scores=sii_scores,
    )
    if assessment is None:
        return None

    severity, broadcast = classify_severity(assessment, previous, config)
    assessment["severity"] = severity
    assessment["broadcast"] = broadcast

    # Enforce daily broadcast cap
    if broadcast and _broadcasts_today() >= config["max_broadcasts_per_day"]:
        logger.warning("Daily broadcast cap reached — downgrading to non-broadcast")
        assessment["broadcast"] = False

    event_id = store_assessment(assessment)
    if event_id:
        assessment["id"] = event_id
    return assessment


def detect_score_changes(config: dict, sii_scores: dict) -> list[dict]:
    """
    Detect SII score changes > threshold in the last 24h.
    Returns list of trigger dicts with affected stablecoin info.
    """
    threshold = config["score_change_threshold_pts"]
    rows = fetch_all("""
        SELECT s.stablecoin_id, s.overall_score,
               h.overall_score AS prev_score
        FROM scores s
        JOIN score_history h
            ON h.stablecoin = s.stablecoin_id
            AND h.score_date = CURRENT_DATE - 1
        WHERE ABS(s.overall_score - h.overall_score) > %s
    """, (threshold,))

    triggers = []
    for r in rows:
        delta = round(r["overall_score"] - r["prev_score"], 2)
        triggers.append({
            "stablecoin_id": r["stablecoin_id"],
            "current_score": r["overall_score"],
            "previous_score": r["prev_score"],
            "delta": delta,
        })
        logger.info(
            f"Score change trigger: {r['stablecoin_id']} "
            f"moved {delta:+.1f} pts"
        )
    return triggers


def detect_large_movements(config: dict) -> list[dict]:
    """
    Detect wallets with value changes > $1M since last assessment.
    Compares current wallet_holdings totals against previous assessment snapshots.
    """
    threshold = config["movement_threshold_usd"]

    # Get wallets with current holdings
    rows = fetch_all("""
        WITH current_totals AS (
            SELECT wallet_address, SUM(value_usd) AS current_value
            FROM wallet_graph.wallet_holdings
            WHERE indexed_at = (
                SELECT MAX(indexed_at) FROM wallet_graph.wallet_holdings
            )
            GROUP BY wallet_address
        ),
        prev_totals AS (
            SELECT DISTINCT ON (wallet_address)
                wallet_address, total_stablecoin_value AS prev_value
            FROM assessment_events
            ORDER BY wallet_address, created_at DESC
        )
        SELECT c.wallet_address, c.current_value,
               COALESCE(p.prev_value, 0) AS prev_value,
               ABS(c.current_value - COALESCE(p.prev_value, 0)) AS movement
        FROM current_totals c
        LEFT JOIN prev_totals p ON p.wallet_address = c.wallet_address
        WHERE ABS(c.current_value - COALESCE(p.prev_value, 0)) > %s
        ORDER BY movement DESC
        LIMIT 100
    """, (threshold,))

    triggers = []
    for r in rows:
        direction = "in" if r["current_value"] > r["prev_value"] else "out"
        triggers.append({
            "wallet_address": r["wallet_address"],
            "movement_usd": round(r["movement"], 2),
            "direction": direction,
            "current_value": round(r["current_value"], 2),
            "prev_value": round(r["prev_value"], 2),
        })
    if triggers:
        logger.info(f"Large movement triggers: {len(triggers)} wallets")
    return triggers


def detect_concentration_shifts(config: dict) -> list[dict]:
    """
    Detect wallets where a single asset jumped from <20% to >40% of the wallet.
    Only for wallets with total value > $500K.
    """
    from_pct = config["concentration_shift_from_pct"]
    to_pct = config["concentration_shift_to_pct"]
    min_value = config["concentration_min_wallet_value"]

    rows = fetch_all("""
        WITH current_holdings AS (
            SELECT wh.wallet_address, wh.symbol, wh.pct_of_wallet, wh.value_usd
            FROM wallet_graph.wallet_holdings wh
            JOIN wallet_graph.wallets w ON w.address = wh.wallet_address
            WHERE w.total_stablecoin_value > %s
            AND wh.indexed_at = (
                SELECT MAX(indexed_at) FROM wallet_graph.wallet_holdings
            )
            AND wh.pct_of_wallet > %s
        )
        SELECT ch.wallet_address, ch.symbol, ch.pct_of_wallet
        FROM current_holdings ch
    """, (min_value, to_pct))

    triggers = []
    for r in rows:
        # Check if this was previously below from_pct via the previous assessment
        prev = _get_previous_assessment(r["wallet_address"])
        if prev and prev.get("holdings_snapshot"):
            prev_holdings = prev["holdings_snapshot"]
            if isinstance(prev_holdings, list):
                for ph in prev_holdings:
                    if isinstance(ph, dict) and ph.get("symbol", "").upper() == r["symbol"].upper():
                        if ph.get("pct_of_wallet", 0) < from_pct:
                            triggers.append({
                                "wallet_address": r["wallet_address"],
                                "symbol": r["symbol"],
                                "prev_pct": ph["pct_of_wallet"],
                                "current_pct": r["pct_of_wallet"],
                            })
                        break
    if triggers:
        logger.info(f"Concentration shift triggers: {len(triggers)} wallets")
    return triggers


def detect_depeg_events(config: dict, sii_scores: dict) -> list[dict]:
    """
    Check current prices for deviation > 1% from $1 peg.
    Uses the most recent price data from historical_prices table.
    """
    threshold_pct = config["depeg_threshold_pct"]

    rows = fetch_all("""
        SELECT DISTINCT ON (coingecko_id)
            coingecko_id, price, timestamp
        FROM historical_prices
        ORDER BY coingecko_id, timestamp DESC
    """)

    triggers = []
    for r in rows:
        price = r.get("price")
        if price is None:
            continue
        deviation = abs(price - 1.0) * 100
        if deviation >= threshold_pct:
            triggers.append({
                "stablecoin_id": r["coingecko_id"],
                "price": price,
                "deviation_pct": round(deviation, 3),
                "recorded_at": r["timestamp"].isoformat() if r.get("timestamp") else None,
            })
            logger.warning(
                f"Depeg trigger: {r['coingecko_id']} at ${price:.4f} "
                f"({deviation:.2f}% deviation)"
            )
    return triggers


def _get_top_wallets(limit: int = 100) -> list[str]:
    """Get top wallets by total stablecoin value."""
    rows = fetch_all("""
        SELECT address FROM wallet_graph.wallets
        WHERE total_stablecoin_value > 0
        ORDER BY total_stablecoin_value DESC
        LIMIT %s
    """, (limit,))
    return [r["address"] for r in rows]


def _get_wallets_holding(stablecoin_id: str) -> list[str]:
    """Get wallets with material exposure to a specific stablecoin."""
    rows = fetch_all("""
        SELECT DISTINCT wallet_address
        FROM wallet_graph.wallet_holdings
        WHERE LOWER(symbol) = LOWER(%s)
        AND value_usd > 10000
        AND indexed_at = (
            SELECT MAX(indexed_at) FROM wallet_graph.wallet_holdings
        )
    """, (stablecoin_id,))
    return [r["wallet_address"] for r in rows]


def run_agent_cycle():
    """
    Main agent entry point. Called after each SII scoring cycle.

    Checks all trigger conditions and generates assessment events.
    """
    config = AGENT_CONFIG
    logger.info("=== Verification agent cycle starting ===")

    try:
        sii_scores = _get_sii_scores()
    except Exception as e:
        logger.error(f"Failed to fetch SII scores: {e}")
        return

    assessments = []
    total_processed = 0

    # 1. Detect SII score changes
    try:
        score_triggers = detect_score_changes(config, sii_scores)
        for trigger in score_triggers:
            if total_processed >= config["max_assessments_per_cycle"]:
                break
            # Re-assess all wallets holding the affected asset
            wallets = _get_wallets_holding(trigger["stablecoin_id"])
            for addr in wallets[:50]:  # cap per-trigger
                if total_processed >= config["max_assessments_per_cycle"]:
                    break
                result = _process_assessment(
                    addr, "score_change", trigger, sii_scores, config
                )
                if result:
                    assessments.append(result)
                    total_processed += 1
    except Exception as e:
        logger.error(f"Score change detection failed: {e}")

    # 2. Detect large movements
    try:
        movement_triggers = detect_large_movements(config)
        for trigger in movement_triggers:
            if total_processed >= config["max_assessments_per_cycle"]:
                break
            result = _process_assessment(
                trigger["wallet_address"], "large_movement",
                trigger, sii_scores, config
            )
            if result:
                assessments.append(result)
                total_processed += 1
    except Exception as e:
        logger.error(f"Large movement detection failed: {e}")

    # 3. Detect concentration shifts
    try:
        conc_triggers = detect_concentration_shifts(config)
        for trigger in conc_triggers:
            if total_processed >= config["max_assessments_per_cycle"]:
                break
            result = _process_assessment(
                trigger["wallet_address"], "concentration_shift",
                trigger, sii_scores, config
            )
            if result:
                assessments.append(result)
                total_processed += 1
    except Exception as e:
        logger.error(f"Concentration shift detection failed: {e}")

    # 4. Detect depeg events
    try:
        depeg_triggers = detect_depeg_events(config, sii_scores)
        for trigger in depeg_triggers:
            if total_processed >= config["max_assessments_per_cycle"]:
                break
            # Re-assess all wallets holding the affected stablecoin
            wallets = _get_wallets_holding(trigger["stablecoin_id"])
            for addr in wallets[:100]:  # higher cap for depeg (critical)
                if total_processed >= config["max_assessments_per_cycle"]:
                    break
                detail = {**trigger, "trigger_source": "depeg"}
                result = _process_assessment(
                    addr, "depeg", detail, sii_scores, config
                )
                if result:
                    assessments.append(result)
                    total_processed += 1
    except Exception as e:
        logger.error(f"Depeg detection failed: {e}")

    # 5. Daily cycle: assess top wallets
    daily_ran = None
    try:
        now_utc = datetime.now(timezone.utc)
        # Check if daily cycle has already run today
        daily_ran = fetch_one("""
            SELECT 1 FROM assessment_events
            WHERE trigger_type = 'daily_cycle'
            AND created_at > CURRENT_DATE
            LIMIT 1
        """)
        if not daily_ran:
            logger.info("Running daily cycle assessment...")
            top_wallets = _get_top_wallets(100)
            for addr in top_wallets:
                if total_processed >= config["max_assessments_per_cycle"]:
                    break
                result = _process_assessment(
                    addr, "daily_cycle",
                    {"cycle_date": now_utc.strftime("%Y-%m-%d")},
                    sii_scores, config,
                )
                if result:
                    assessments.append(result)
                    total_processed += 1
    except Exception as e:
        logger.error(f"Daily cycle failed: {e}")

    # 6. Generate daily pulse (if daily_cycle ran)
    if not daily_ran:
        try:
            from app.publisher.pulse_renderer import generate_daily_pulse
            pulse = generate_daily_pulse()
            if pulse:
                logger.info("Daily pulse generated successfully")
        except Exception as e:
            logger.error(f"Pulse generation failed: {e}")

    # Heartbeat: ensure events freshness is tracked even when no triggers fire
    if total_processed == 0:
        try:
            from app.agent.store import store_assessment
            heartbeat = {
                "wallet_address": "0x0000000000000000000000000000000000000000",
                "trigger_type": "heartbeat",
                "severity": "silent",
                "broadcast": False,
                "trigger_detail": {"cycle_assessments": 0},
            }
            store_assessment(heartbeat)
        except Exception as e:
            logger.debug(f"Heartbeat store failed: {e}")

    # Summary
    severities = {}
    for a in assessments:
        sev = a.get("severity", "unknown")
        severities[sev] = severities.get(sev, 0) + 1

    broadcasts = sum(1 for a in assessments if a.get("broadcast"))

    logger.info(
        f"=== Agent cycle complete: {total_processed} assessments | "
        f"severities={severities} | broadcasts={broadcasts} ==="
    )

    return {
        "assessments": total_processed,
        "severities": severities,
        "broadcasts": broadcasts,
    }
