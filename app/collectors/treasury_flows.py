"""
Treasury Flow Collector
========================
Detects behavioral events from labeled treasury wallets.
Same pattern as flows.py but operating on wallet addresses
instead of token contracts.

Data sources:
  - Etherscan V2 tokentx (already integrated via edges.py)
  - wallet_graph.wallet_risk_scores (existing time series)
  - wallet_graph.wallet_holdings (existing snapshots)

Event types detected:
  1. twap_conversion    — repeated swaps of similar size over time
  2. rebalance          — moving assets between DeFi protocols
  3. concentration_drift — HHI changing significantly over 7/30d
  4. quality_shift      — SII-weighted exposure changing
  5. large_transfer     — single transfers > $1M

Runs: daily, after wallet scoring cycle completes.
API budget: 1 Etherscan call per registered treasury per cycle.
"""

import os
import math
import asyncio
import logging
import statistics
from datetime import datetime, timezone, timedelta

import httpx

from app.database import fetch_all, fetch_one, get_conn

logger = logging.getLogger(__name__)

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
RATE_LIMIT_DELAY = 0.15
LARGE_TRANSFER_THRESHOLD = 1_000_000  # $1M

# Known DEX/aggregator settlement contracts
SETTLEMENT_CONTRACTS = {
    "0x9008d19f58aabd9ed0d60971565aa8510560ab41": "CoWSwap",
    "0x1111111254eeb25477b68fb85ed929f73a960582": "1inch",
    "0x3fc91a3afd70395cd496c647d5a6cc9d4b2b7fad": "Uniswap Universal Router",
    "0xdef171fe48cf0115b1d80b88dc8eab59176fee57": "Paraswap",
    "0xe592427a0aece92de3edee1f18e0157c05861564": "Uniswap V3 Router",
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "Uniswap V2 Router",
}

# Severity escalation thresholds
SEVERITY_ESCALATION = {
    "large_transfer": {"warning": 10_000_000},         # > $10M
    "concentration_drift": {"critical": 8000},           # HHI > 8000
    "quality_shift": {"critical": 70},                   # risk score < 70
}


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------

def get_registered_treasuries() -> list[dict]:
    """Fetch all monitoring-enabled treasury wallets."""
    return fetch_all("""
        SELECT address, chain, entity_name, entity_type, label_source,
               wallet_purpose, related_addresses, notes
        FROM wallet_graph.treasury_registry
        WHERE monitoring_enabled = TRUE
        ORDER BY entity_name
    """) or []


def _get_known_labels() -> dict:
    """Build address→label lookup from etherscan KNOWN_HOLDERS + treasury registry."""
    labels = {}
    try:
        from app.collectors.etherscan import KNOWN_HOLDERS
        for addr, label, cat in KNOWN_HOLDERS:
            labels[addr.lower()] = {"label": label, "type": cat}
    except Exception:
        pass
    # Overlay treasury registry
    try:
        treasuries = get_registered_treasuries()
        for t in treasuries:
            labels[t["address"].lower()] = {"label": t["entity_name"], "type": t["entity_type"]}
    except Exception:
        pass
    return labels


# ---------------------------------------------------------------------------
# Token price helper
# ---------------------------------------------------------------------------

def _get_token_price_usd(contract_address: str, decimals: int = 18) -> float:
    """Get approximate USD price for a token from SII scores or default to 1.0 for stables."""
    try:
        row = fetch_one("""
            SELECT s.current_price FROM scores s
            JOIN stablecoins st ON st.id = s.stablecoin_id
            WHERE LOWER(st.contract) = LOWER(%s)
        """, (contract_address,))
        if row and row.get("current_price"):
            return float(row["current_price"])
    except Exception:
        pass
    # Check if known stablecoin by contract
    try:
        row = fetch_one(
            "SELECT symbol FROM stablecoins WHERE LOWER(contract) = LOWER(%s)",
            (contract_address,)
        )
        if row:
            return 1.0  # Stablecoin, assume ~$1
    except Exception:
        pass
    return 0  # Unknown token, can't price


def _transfer_value_usd(tx: dict) -> float:
    """Estimate USD value of a token transfer."""
    try:
        decimals = int(tx.get("tokenDecimal", 18))
        raw_value = int(tx.get("value", 0))
        amount = raw_value / (10 ** decimals)
        contract = (tx.get("contractAddress") or "").lower()
        price = _get_token_price_usd(contract, decimals)
        return amount * price
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Etherscan fetch (reuses edges.py pattern)
# ---------------------------------------------------------------------------

async def _fetch_transfers(
    client: httpx.AsyncClient,
    wallet_address: str,
    api_key: str,
    pages: int = 3,
) -> list[dict]:
    """Fetch recent ERC-20 transfers for a wallet (up to 300 most recent)."""
    all_transfers = []
    for page in range(1, pages + 1):
        try:
            resp = await client.get(ETHERSCAN_V2_BASE, params={
                "chainid": 1,
                "module": "account",
                "action": "tokentx",
                "address": wallet_address,
                "page": page,
                "offset": 100,
                "sort": "desc",
                "apikey": api_key,
            }, timeout=15.0)
            data = resp.json()
            txs = data.get("result", [])
            if not isinstance(txs, list):
                break
            all_transfers.extend(txs)
            if len(txs) < 100:
                break
            await asyncio.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            logger.warning(f"Treasury transfer fetch error for {wallet_address}: {e}")
            break
    return all_transfers


# ---------------------------------------------------------------------------
# Event Detector 1: Large Single Transfer
# ---------------------------------------------------------------------------

def detect_large_transfers(
    transfers: list[dict],
    wallet_address: str,
    labels: dict,
) -> list[dict]:
    """Detect single transfers > $1M involving a treasury wallet."""
    events = []
    wallet_lower = wallet_address.lower()

    for tx in transfers:
        usd_value = _transfer_value_usd(tx)
        if usd_value < LARGE_TRANSFER_THRESHOLD:
            continue

        from_addr = (tx.get("from") or "").lower()
        to_addr = (tx.get("to") or "").lower()
        direction = "outflow" if from_addr == wallet_lower else "inflow"
        counterparty = to_addr if direction == "outflow" else from_addr
        cp_info = labels.get(counterparty, {})

        severity = "info"
        if usd_value >= SEVERITY_ESCALATION["large_transfer"]["warning"]:
            severity = "warning"

        symbol = tx.get("tokenSymbol") or tx.get("tokenName") or "UNKNOWN"

        events.append({
            "event_type": "large_transfer",
            "direction": direction,
            "token": symbol,
            "token_contract": tx.get("contractAddress", ""),
            "value_usd": round(usd_value, 2),
            "counterparty": counterparty,
            "counterparty_label": cp_info.get("label"),
            "counterparty_type": cp_info.get("type"),
            "tx_hash": tx.get("hash", ""),
            "block_number": int(tx.get("blockNumber", 0)),
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "confidence": "high",
            "severity": severity,
            "stablecoins_involved": [symbol] if _get_token_price_usd(tx.get("contractAddress", "")) > 0 else [],
        })

    return events


# ---------------------------------------------------------------------------
# Event Detector 2: TWAP Conversion Pattern
# ---------------------------------------------------------------------------

def detect_twap_pattern(
    transfers: list[dict],
    wallet_address: str,
) -> list[dict]:
    """Detect repeated similar-size swaps via DEX settlement contracts."""
    events = []
    wallet_lower = wallet_address.lower()

    # Group outgoing transfers by settlement contract
    by_settlement = {}
    for tx in transfers:
        from_addr = (tx.get("from") or "").lower()
        to_addr = (tx.get("to") or "").lower()

        if from_addr != wallet_lower:
            continue

        if to_addr in SETTLEMENT_CONTRACTS:
            by_settlement.setdefault(to_addr, []).append(tx)

    for settlement_addr, txs in by_settlement.items():
        if len(txs) < 3:
            continue

        # Group by sell token (contract address)
        by_token = {}
        for tx in txs:
            contract = (tx.get("contractAddress") or "").lower()
            by_token.setdefault(contract, []).append(tx)

        for token_contract, token_txs in by_token.items():
            if len(token_txs) < 3:
                continue

            # Compute USD values
            fills = []
            for tx in token_txs:
                usd = _transfer_value_usd(tx)
                ts_val = int(tx.get("timeStamp", 0))
                if usd > 0 and ts_val > 0:
                    fills.append({"usd": usd, "ts": ts_val, "tx": tx})

            if len(fills) < 3:
                continue

            fills.sort(key=lambda f: f["ts"])
            usd_values = [f["usd"] for f in fills]
            median_fill = statistics.median(usd_values)

            # Check uniformity: fills within 50% of median
            uniform = all(abs(v - median_fill) / max(median_fill, 1) < 0.50 for v in usd_values)
            if not uniform:
                continue

            # Check time span > 4 hours
            time_span_hours = (fills[-1]["ts"] - fills[0]["ts"]) / 3600
            if time_span_hours < 4:
                continue

            # Compute stats
            total_usd = sum(usd_values)
            avg_fill = total_usd / len(fills)
            stddev_pct = (statistics.stdev(usd_values) / max(avg_fill, 1)) * 100 if len(fills) > 1 else 0

            intervals = [(fills[i+1]["ts"] - fills[i]["ts"]) / 60 for i in range(len(fills)-1)]
            avg_interval = statistics.mean(intervals) if intervals else 0

            confidence = "high" if len(fills) >= 5 and stddev_pct < 30 else "medium"

            symbol = fills[0]["tx"].get("tokenSymbol", "UNKNOWN")

            events.append({
                "event_type": "twap_conversion",
                "sell_token": token_contract,
                "sell_symbol": symbol,
                "settlement_contract": settlement_addr,
                "settlement_name": SETTLEMENT_CONTRACTS.get(settlement_addr, "Unknown DEX"),
                "num_fills": len(fills),
                "total_sell_value_usd": round(total_usd, 2),
                "avg_fill_size_usd": round(avg_fill, 2),
                "fill_size_stddev_pct": round(stddev_pct, 1),
                "time_span_hours": round(time_span_hours, 1),
                "avg_interval_minutes": round(avg_interval, 1),
                "detected_at": datetime.now(timezone.utc).isoformat(),
                "confidence": confidence,
                "severity": "info",
            })

    return events


# ---------------------------------------------------------------------------
# Event Detector 3: Concentration Drift
# ---------------------------------------------------------------------------

def detect_concentration_drift(wallet_address: str) -> list[dict]:
    """Detect significant HHI changes over 7d and 30d windows."""
    events = []
    wallet_lower = wallet_address.lower()

    rows = fetch_all("""
        SELECT concentration_hhi, computed_at::date as score_date
        FROM wallet_graph.wallet_risk_scores
        WHERE wallet_address = %s AND chain = 'ethereum'
        ORDER BY computed_at DESC LIMIT 30
    """, (wallet_lower,))

    if not rows or len(rows) < 2:
        return []

    hhi_current = float(rows[0]["concentration_hhi"]) if rows[0].get("concentration_hhi") else None
    if hhi_current is None:
        return []

    # Find 7d ago and 30d ago values
    hhi_7d = None
    hhi_30d = None
    now_date = rows[0]["score_date"]
    for r in rows:
        days_ago = (now_date - r["score_date"]).days
        if days_ago >= 7 and hhi_7d is None:
            hhi_7d = float(r["concentration_hhi"]) if r.get("concentration_hhi") else None
        if days_ago >= 28 and hhi_30d is None:
            hhi_30d = float(r["concentration_hhi"]) if r.get("concentration_hhi") else None

    drift_7d = (hhi_current - hhi_7d) if hhi_7d is not None else None
    drift_30d = (hhi_current - hhi_30d) if hhi_30d is not None else None

    # Significant if HHI changed by > 500 points in 7 days
    if drift_7d is not None and abs(drift_7d) > 500:
        direction = "concentrating" if drift_7d > 0 else "diversifying"
        severity = "warning"
        if hhi_current > SEVERITY_ESCALATION["concentration_drift"]["critical"]:
            severity = "critical"

        # Get dominant asset
        holdings = fetch_all("""
            SELECT symbol, pct_of_wallet FROM wallet_graph.wallet_holdings
            WHERE wallet_address = %s AND chain = 'ethereum'
            ORDER BY indexed_at DESC, value_usd DESC LIMIT 5
        """, (wallet_lower,))
        dominant = holdings[0] if holdings else {}

        events.append({
            "event_type": "concentration_drift",
            "hhi_current": round(hhi_current, 0),
            "hhi_7d_ago": round(hhi_7d, 0) if hhi_7d else None,
            "hhi_30d_ago": round(hhi_30d, 0) if hhi_30d else None,
            "drift_7d": round(drift_7d, 0),
            "drift_30d": round(drift_30d, 0) if drift_30d else None,
            "direction": direction,
            "dominant_asset": dominant.get("symbol"),
            "dominant_pct": float(dominant["pct_of_wallet"]) if dominant.get("pct_of_wallet") else None,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "confidence": "high",
            "severity": severity,
        })

    return events


# ---------------------------------------------------------------------------
# Event Detector 4: Quality Shift
# ---------------------------------------------------------------------------

def detect_quality_shift(wallet_address: str) -> list[dict]:
    """Detect significant changes in quality-weighted SII exposure."""
    events = []
    wallet_lower = wallet_address.lower()

    rows = fetch_all("""
        SELECT risk_score, computed_at::date as score_date
        FROM wallet_graph.wallet_risk_scores
        WHERE wallet_address = %s AND chain = 'ethereum' AND risk_score IS NOT NULL
        ORDER BY computed_at DESC LIMIT 10
    """, (wallet_lower,))

    if not rows or len(rows) < 2:
        return []

    current_score = float(rows[0]["risk_score"])
    now_date = rows[0]["score_date"]

    # Find 7d ago score
    score_7d = None
    for r in rows:
        days_ago = (now_date - r["score_date"]).days
        if days_ago >= 7:
            score_7d = float(r["risk_score"])
            break

    if score_7d is None:
        return []

    shift = current_score - score_7d
    if abs(shift) < 5:
        return []

    severity = "warning"
    if current_score < SEVERITY_ESCALATION["quality_shift"]["critical"]:
        severity = "critical"

    # Determine cause — check if holdings changed
    holdings_now = fetch_all("""
        SELECT symbol, pct_of_wallet, sii_score
        FROM wallet_graph.wallet_holdings
        WHERE wallet_address = %s AND chain = 'ethereum'
        ORDER BY indexed_at DESC, value_usd DESC LIMIT 10
    """, (wallet_lower,))

    events.append({
        "event_type": "quality_shift",
        "risk_score_current": round(current_score, 1),
        "risk_score_7d_ago": round(score_7d, 1),
        "shift_7d": round(shift, 1),
        "direction": "improved" if shift > 0 else "degraded",
        "top_holdings": [
            {"symbol": h["symbol"], "pct": float(h["pct_of_wallet"]) if h.get("pct_of_wallet") else None,
             "sii": float(h["sii_score"]) if h.get("sii_score") else None}
            for h in (holdings_now or [])[:5]
        ],
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "confidence": "high",
        "severity": severity,
    })

    return events


# ---------------------------------------------------------------------------
# Event Detector 5: Rebalancing Event
# ---------------------------------------------------------------------------

def detect_rebalance(
    transfers: list[dict],
    wallet_address: str,
    labels: dict,
) -> list[dict]:
    """Detect protocol rebalancing — moving assets between DeFi protocols."""
    events = []
    wallet_lower = wallet_address.lower()

    # Filter transfers in last 24h
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    recent = []
    for tx in transfers:
        try:
            ts = int(tx.get("timeStamp", 0))
            if ts > 0 and datetime.fromtimestamp(ts, tz=timezone.utc) >= cutoff:
                recent.append(tx)
        except Exception:
            continue

    if len(recent) < 2:
        return []

    # Classify transfers as exits (from wallet to DeFi) or entries (from DeFi to wallet)
    exits = []
    entries = []
    for tx in recent:
        from_addr = (tx.get("from") or "").lower()
        to_addr = (tx.get("to") or "").lower()
        usd = _transfer_value_usd(tx)
        if usd < 10_000:
            continue

        symbol = tx.get("tokenSymbol", "UNKNOWN")

        if from_addr == wallet_lower:
            to_label = labels.get(to_addr, {})
            if to_label.get("type") == "defi" or to_addr in SETTLEMENT_CONTRACTS:
                exits.append({
                    "protocol": to_label.get("label", to_addr[:10]),
                    "token": symbol,
                    "value_usd": round(usd, 2),
                })
        elif to_addr == wallet_lower:
            from_label = labels.get(from_addr, {})
            if from_label.get("type") == "defi" or from_addr in SETTLEMENT_CONTRACTS:
                entries.append({
                    "protocol": from_label.get("label", from_addr[:10]),
                    "token": symbol,
                    "value_usd": round(usd, 2),
                })

    # A rebalance requires both exits and entries
    total_exits = sum(e["value_usd"] for e in exits)
    total_entries = sum(e["value_usd"] for e in entries)

    if exits and entries and (total_exits + total_entries) > 100_000:
        events.append({
            "event_type": "rebalance",
            "exits": exits,
            "entries": entries,
            "total_exit_usd": round(total_exits, 2),
            "total_entry_usd": round(total_entries, 2),
            "net_flow_usd": round(total_entries - total_exits, 2),
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "confidence": "medium",
            "severity": "info",
        })

    return events


# ---------------------------------------------------------------------------
# Main collector entry point
# ---------------------------------------------------------------------------

async def collect_treasury_events(
    client: httpx.AsyncClient = None,
) -> list[dict]:
    """
    Run all treasury event detectors on all registered wallets.
    Returns list of detected events (also stored in DB).
    """
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        logger.warning("Treasury flows: no ETHERSCAN_API_KEY set")
        return []

    treasuries = get_registered_treasuries()
    if not treasuries:
        logger.info("Treasury flows: no registered treasuries")
        return []

    labels = _get_known_labels()
    all_events = []
    own_client = client is None

    if own_client:
        client = httpx.AsyncClient()

    try:
        for treasury in treasuries:
            addr = treasury["address"].lower()
            entity = treasury["entity_name"]
            logger.info(f"Treasury flows: scanning {entity} ({addr[:10]}...)")

            try:
                # Fetch recent transfers (1 Etherscan call)
                transfers = await _fetch_transfers(client, addr, api_key, pages=2)
                await asyncio.sleep(RATE_LIMIT_DELAY)

                # Run all detectors
                wallet_events = []

                # 1. Large transfers
                wallet_events.extend(detect_large_transfers(transfers, addr, labels))

                # 2. TWAP patterns
                wallet_events.extend(detect_twap_pattern(transfers, addr))

                # 3. Concentration drift (reads existing DB data, no API call)
                wallet_events.extend(detect_concentration_drift(addr))

                # 4. Quality shift (reads existing DB data, no API call)
                wallet_events.extend(detect_quality_shift(addr))

                # 5. Rebalance (uses transfers already fetched)
                wallet_events.extend(detect_rebalance(transfers, addr, labels))

                # Store events
                for event in wallet_events:
                    _store_event(addr, event)
                    all_events.append({**event, "wallet_address": addr, "entity_name": entity})

                logger.info(f"Treasury flows: {entity} — {len(wallet_events)} events detected")

            except Exception as e:
                logger.error(f"Treasury flows: error scanning {entity}: {e}")

    finally:
        if own_client:
            await client.aclose()

    logger.info(f"Treasury flow detection complete: {len(all_events)} total events from {len(treasuries)} treasuries")
    return all_events


def _store_event(wallet_address: str, event: dict):
    """Store a treasury event in the database."""
    try:
        import json
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO wallet_graph.treasury_events
                        (wallet_address, event_type, event_data, severity, confidence,
                         stablecoins_involved, protocols_involved,
                         risk_score_before, risk_score_after, risk_score_delta)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    wallet_address,
                    event.get("event_type", "unknown"),
                    json.dumps(event),
                    event.get("severity", "info"),
                    event.get("confidence", "medium"),
                    event.get("stablecoins_involved"),
                    event.get("protocols_involved"),
                    event.get("risk_score_before"),
                    event.get("risk_score_after") or event.get("risk_score_current"),
                    event.get("risk_score_delta") or event.get("shift_7d"),
                ))
            conn.commit()
    except Exception as e:
        logger.debug(f"Treasury event store failed (table may not exist yet): {e}")
