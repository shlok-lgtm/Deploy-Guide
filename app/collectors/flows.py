"""
Flows Collector — Mint/Burn Event Detection
=============================================
Fetches token transfer events from Etherscan to detect mints and burns.
Produces 5 components for the SII Flows category (15% weight):

  - daily_mint_volume:          USD value of mints in last 24h
  - daily_burn_volume:          USD value of burns in last 24h
  - net_mint_burn_ratio:        mint / (mint + burn), 0.5 = balanced
  - supply_change_velocity:     abs daily market cap change %
  - unusual_minting_detection:  z-score of today's mint vs 30d rolling average

Data source: Etherscan tokentx API (17 calls per cycle, well within budget).
"""

import os
import math
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import httpx

from app.database import fetch_all, fetch_one
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
RATE_LIMIT_DELAY = 0.15  # slightly conservative to avoid contention with holder queries
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def _get_stablecoin_config(stablecoin_id: str) -> dict | None:
    """Get stablecoin config from DB (works for both registry and promoted coins)."""
    row = fetch_one(
        "SELECT id, symbol, contract, decimals, coingecko_id FROM stablecoins WHERE id = %s",
        (stablecoin_id,),
    )
    if not row or not row.get("contract"):
        return None
    return {
        "id": row["id"],
        "symbol": row["symbol"],
        "contract": row["contract"],
        "decimals": row["decimals"] or 18,
        "coingecko_id": row.get("coingecko_id"),
    }


async def _fetch_recent_transfers(
    client: httpx.AsyncClient, contract: str, api_key: str
) -> list[dict]:
    """Fetch the most recent 200 token transfers for a contract."""
    params = {
        "chainid": 1,
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract,
        "page": 1,
        "offset": 200,
        "sort": "desc",
        "apikey": api_key,
    }
    try:
        resp = await client.get(ETHERSCAN_V2_BASE, params=params, timeout=20)
        data = resp.json()
        if data.get("status") == "1" and data.get("result"):
            return data["result"]
        if "Max rate limit" in str(data.get("result", "")):
            logger.warning("Etherscan rate limit hit in flows collector")
            await asyncio.sleep(1.0)
        return []
    except Exception as e:
        logger.error(f"Etherscan tokentx error for {contract}: {e}")
        return []


def _parse_mint_burn(transfers: list[dict], decimals: int, price: float) -> dict:
    """
    Parse transfers into mint/burn volumes.

    Mint = from is zero address.
    Burn = to is zero address.
    Returns USD volumes for last 24 hours.
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=24)

    mint_total = 0.0
    burn_total = 0.0

    for tx in transfers:
        try:
            ts = int(tx.get("timeStamp", 0))
            tx_time = datetime.fromtimestamp(ts, tz=timezone.utc)
            if tx_time < cutoff:
                continue

            raw_value = int(tx.get("value", "0"))
            token_amount = raw_value / (10 ** decimals)
            usd_value = token_amount * price

            from_addr = (tx.get("from") or "").lower()
            to_addr = (tx.get("to") or "").lower()

            if from_addr == ZERO_ADDRESS:
                mint_total += usd_value
            elif to_addr == ZERO_ADDRESS:
                burn_total += usd_value
        except (ValueError, TypeError):
            continue

    return {"mint_usd": mint_total, "burn_usd": burn_total}


def _get_current_price(stablecoin_id: str) -> float:
    """Get current price from scores table. Stablecoins ≈ $1."""
    row = fetch_one(
        "SELECT current_price FROM scores WHERE stablecoin_id = %s",
        (stablecoin_id,),
    )
    if row and row.get("current_price"):
        return float(row["current_price"])
    return 1.0  # safe default for stablecoins


def _get_market_cap(stablecoin_id: str) -> float | None:
    """Get current market cap from scores table."""
    row = fetch_one(
        "SELECT market_cap FROM scores WHERE stablecoin_id = %s",
        (stablecoin_id,),
    )
    if row and row.get("market_cap"):
        return float(row["market_cap"])
    return None


def _compute_supply_change_velocity(stablecoin_id: str) -> float | None:
    """
    Compute daily supply change velocity from stored market_cap readings.
    Returns abs percentage change, or None if insufficient data.
    """
    rows = fetch_all("""
        SELECT raw_value, collected_at
        FROM component_readings
        WHERE stablecoin_id = %s AND component_id = 'market_cap'
        ORDER BY collected_at DESC LIMIT 2
    """, (stablecoin_id,))

    if len(rows) < 2:
        return None

    today = float(rows[0]["raw_value"])
    yesterday = float(rows[1]["raw_value"])

    if yesterday <= 0:
        return None

    return abs(today - yesterday) / yesterday * 100  # as percentage


def _compute_unusual_minting(stablecoin_id: str, today_mint: float) -> float:
    """
    Compute z-score of today's mint volume vs 30-day rolling average.
    Returns neutral (50) if < 7 days of history.
    """
    rows = fetch_all("""
        SELECT raw_value
        FROM component_readings
        WHERE stablecoin_id = %s AND component_id = 'daily_mint_volume'
        ORDER BY collected_at DESC LIMIT 30
    """, (stablecoin_id,))

    values = [float(r["raw_value"]) for r in rows if r["raw_value"] is not None]

    if len(values) < 7:
        return None  # not enough history

    mean = sum(values) / len(values)
    if len(values) < 2:
        return None

    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    stddev = math.sqrt(variance) if variance > 0 else 0

    if stddev == 0:
        return 100.0 if today_mint <= mean else 0.0

    z_score = abs(today_mint - mean) / stddev
    return z_score


# =============================================================================
# Normalization helpers (keep in sync with scoring.py specs)
# =============================================================================

def _normalize_mint_burn_relative(volume: float, market_cap: float) -> float:
    """Mint or burn volume relative to market cap. Lower ratio = better."""
    if market_cap <= 0:
        return 50.0
    ratio = volume / market_cap
    if ratio <= 0.001:  # < 0.1%
        return 100.0
    if ratio >= 0.05:   # > 5%
        return 0.0
    # Log scale between 0.1% and 5%
    log_ratio = math.log10(ratio)
    log_min = math.log10(0.001)
    log_max = math.log10(0.05)
    return max(0, min(100, 100 * (1 - (log_ratio - log_min) / (log_max - log_min))))


def _normalize_ratio(ratio: float) -> float:
    """Sigmoid-like scoring centered on 0.5. Score 100 at 0.5, drops to 0 at extremes."""
    deviation = abs(ratio - 0.5)
    # Score = 100 * (1 - (2*deviation)^2), clamped to [0, 100]
    score = 100.0 * (1.0 - (2.0 * deviation) ** 2)
    return max(0.0, min(100.0, score))


def _normalize_velocity(velocity_pct: float) -> float:
    """Lower daily supply change = better. < 0.1% = 100, > 3% = 0."""
    if velocity_pct <= 0.1:
        return 100.0
    if velocity_pct >= 3.0:
        return 0.0
    return 100.0 - ((velocity_pct - 0.1) / (3.0 - 0.1) * 100.0)


def _normalize_zscore(z_score: float) -> float:
    """Z-score inversion. Low z-score = normal = high score."""
    if z_score < 1.0:
        return 100.0
    if z_score < 2.0:
        return 80.0
    if z_score < 3.0:
        return 50.0
    if z_score < 5.0:
        return 20.0
    return 0.0


# =============================================================================
# Main collector function
# =============================================================================

async def collect_flows_components(
    client: httpx.AsyncClient, stablecoin_id: str
) -> list[dict]:
    """
    Collect mint/burn flow components for one stablecoin.
    Returns list of component dicts ready for DB insert.
    """
    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        logger.warning("ETHERSCAN_API_KEY not set — skipping flows collection")
        return []

    cfg = _get_stablecoin_config(stablecoin_id)
    if not cfg:
        return []

    contract = cfg["contract"]
    decimals = cfg["decimals"]
    price = _get_current_price(stablecoin_id)
    market_cap = _get_market_cap(stablecoin_id)

    # Fetch recent transfers
    transfers = await _fetch_recent_transfers(client, contract, api_key)
    await asyncio.sleep(RATE_LIMIT_DELAY)

    if not transfers:
        logger.debug(f"No transfers found for {stablecoin_id}")
        return []

    # Parse mint/burn volumes
    mb = _parse_mint_burn(transfers, decimals, price)
    mint_usd = mb["mint_usd"]
    burn_usd = mb["burn_usd"]
    total = mint_usd + burn_usd

    components = []

    # 1. daily_mint_volume
    mint_score = _normalize_mint_burn_relative(mint_usd, market_cap) if market_cap else 50.0
    components.append({
        "component_id": "daily_mint_volume",
        "category": "flows",
        "raw_value": round(mint_usd, 2),
        "normalized_score": round(mint_score, 2),
        "data_source": "etherscan",
    })

    # 2. daily_burn_volume
    burn_score = _normalize_mint_burn_relative(burn_usd, market_cap) if market_cap else 50.0
    components.append({
        "component_id": "daily_burn_volume",
        "category": "flows",
        "raw_value": round(burn_usd, 2),
        "normalized_score": round(burn_score, 2),
        "data_source": "etherscan",
    })

    # 3. net_mint_burn_ratio
    ratio = mint_usd / total if total > 0 else 0.5
    ratio_score = _normalize_ratio(ratio)
    components.append({
        "component_id": "net_mint_burn_ratio",
        "category": "flows",
        "raw_value": round(ratio, 4),
        "normalized_score": round(ratio_score, 2),
        "data_source": "etherscan",
    })

    # 4. supply_change_velocity (from stored market cap history)
    velocity = _compute_supply_change_velocity(stablecoin_id)
    if velocity is not None:
        velocity_score = _normalize_velocity(velocity)
        components.append({
            "component_id": "supply_change_velocity",
            "category": "flows",
            "raw_value": round(velocity, 4),
            "normalized_score": round(velocity_score, 2),
            "data_source": "derived",
        })

    # 5. unusual_minting_detection (z-score vs 30d rolling)
    z_score = _compute_unusual_minting(stablecoin_id, mint_usd)
    # z_score is None when insufficient history — default to neutral score
    if z_score is None:
        z_raw = 0.0
        z_normalized = 50.0  # neutral — not enough history
    else:
        z_raw = z_score
        z_normalized = _normalize_zscore(z_score)
    components.append({
        "component_id": "unusual_minting_detection",
        "category": "flows",
        "raw_value": round(z_raw, 4),
        "normalized_score": round(z_normalized, 2),
        "data_source": "derived",
    })

    # Attest flow components
    try:
        from app.state_attestation import attest_state
        if components:
            attest_state("flows", [{"id": c.get("component_id"), "score": c.get("normalized_score")} for c in components], entity_id=stablecoin_id)
    except Exception:
        pass  # attestation is non-critical

    return components
