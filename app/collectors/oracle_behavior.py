"""
Oracle Deviation and Latency Behavioral Record (Pipeline 10)
===============================================================
Continuously records oracle price feed behavior — deviation from CEX
prices and update latency — for every oracle feeding scored entities.

Runs in the FAST CYCLE (hourly).  Oracle behavioral records during
stress events cannot be reconstructed after the fact.

Never raises — all errors logged and skipped.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, execute, get_cursor

logger = logging.getLogger(__name__)

# Chainlink AggregatorV3Interface.latestRoundData() selector
# Returns (roundId, answer, startedAt, updatedAt, answeredInRound)
CHAINLINK_LATEST_ROUND_DATA = "0xfeaf968c"

# Pyth getPrice(bytes32 priceId) selector
PYTH_GET_PRICE = "0x31d98b3f"

# Pyth USDC/USD price feed ID
PYTH_USDC_FEED_ID = "eaa020c61cc479712813461ce153894a96a6c00b2657731e30bd0f0d0a39c7"

# CoinGecko symbol → ID mapping
SYMBOL_TO_COINGECKO = {
    "USDC": "usd-coin",
    "USDT": "tether",
    "DAI": "dai",
    "ETH": "ethereum",
    "BTC": "bitcoin",
    "stETH": "staked-ether",
}

# Stress thresholds — FALLBACK defaults when per-feed config is missing
DEVIATION_STRESS_THRESHOLD = 0.5   # percent (fallback)
LATENCY_STRESS_THRESHOLD = 3600    # seconds (fallback — 1 hour)
HEARTBEAT_BUFFER = 1.1             # 10% buffer past heartbeat before flagging

# Pre-stress context window: readings in this window before a newly-opened
# event get tagged so the triptych endpoint can render before / during / after.
PRE_STRESS_WINDOW_HOURS = 72

# Fallback public RPCs
FALLBACK_RPCS = {
    "ethereum": "https://eth.llamarpc.com",
    "base": "https://mainnet.base.org",
}


# ---------------------------------------------------------------------------
# RPC helpers
# ---------------------------------------------------------------------------

def _get_rpc_url(chain: str) -> str:
    """Get RPC URL for chain, preferring Alchemy, falling back to public."""
    alchemy_key = os.environ.get("ALCHEMY_API_KEY", "")
    if alchemy_key:
        chain_map = {
            "ethereum": "eth-mainnet",
            "base": "base-mainnet",
            "arbitrum": "arb-mainnet",
        }
        network = chain_map.get(chain)
        if network:
            return f"https://{network}.g.alchemy.com/v2/{alchemy_key}"

    # Chain-specific env vars
    env_map = {"ethereum": "ETHEREUM_RPC_URL", "base": "BASE_RPC_URL"}
    env_url = os.environ.get(env_map.get(chain, ""), "")
    if env_url:
        return env_url

    return FALLBACK_RPCS.get(chain, "")


async def _async_eth_call(client: httpx.AsyncClient, rpc_url: str, to: str, data: str) -> str:
    """Async eth_call. Returns hex result or empty string."""
    if not rpc_url:
        return "0x"
    try:
        resp = await client.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_call",
                "params": [{"to": to, "data": data}, "latest"],
            },
            timeout=15,
        )
        result = resp.json().get("result", "0x")
        return result if result else "0x"
    except Exception as e:
        logger.debug(f"eth_call failed for {to}: {e}")
        return "0x"


async def _async_get_block_timestamp(client: httpx.AsyncClient, rpc_url: str) -> int:
    """Get current block timestamp."""
    if not rpc_url:
        return int(time.time())
    try:
        resp = await client.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_getBlockByNumber",
                "params": ["latest", False],
            },
            timeout=10,
        )
        block = resp.json().get("result", {})
        return int(block.get("timestamp", "0x0"), 16)
    except Exception:
        return int(time.time())


def _decode_uint256(hex_str: str, offset: int = 0) -> int:
    """Decode a uint256 from hex at a 32-byte word offset."""
    start = 2 + (offset * 64)
    end = start + 64
    if len(hex_str) < end:
        return 0
    return int(hex_str[start:end], 16)


def _decode_int256(hex_str: str, offset: int = 0) -> int:
    """Decode a signed int256 from hex at a 32-byte word offset."""
    raw = _decode_uint256(hex_str, offset)
    if raw >= 2**255:
        raw -= 2**256
    return raw


# ---------------------------------------------------------------------------
# Oracle reading
# ---------------------------------------------------------------------------

async def _read_chainlink_oracle(
    client: httpx.AsyncClient, oracle: dict, rpc_url: str, block_ts: int
) -> dict | None:
    """Read a Chainlink oracle via latestRoundData()."""
    result = await _async_eth_call(client, rpc_url, oracle["oracle_address"], CHAINLINK_LATEST_ROUND_DATA)
    if not result or result == "0x" or len(result) < 322:
        return None

    round_id = _decode_uint256(result, 0)
    answer = _decode_int256(result, 1)
    updated_at = _decode_uint256(result, 3)

    if answer <= 0:
        return None

    decimals = oracle.get("decimals", 8)
    price = answer / (10 ** decimals)
    latency = max(0, block_ts - updated_at) if updated_at > 0 else 0

    return {
        "oracle_price": price,
        "oracle_price_raw": str(answer),
        "oracle_decimals": decimals,
        "latency_seconds": latency,
        "round_id": str(round_id),
        "answer_timestamp": datetime.fromtimestamp(updated_at, tz=timezone.utc) if updated_at > 0 else None,
    }


async def _read_pyth_oracle(
    client: httpx.AsyncClient, oracle: dict, rpc_url: str, block_ts: int
) -> dict | None:
    """Read a Pyth oracle via getPrice(bytes32)."""
    feed_id = PYTH_USDC_FEED_ID.zfill(64)
    calldata = PYTH_GET_PRICE + feed_id

    result = await _async_eth_call(client, rpc_url, oracle["oracle_address"], calldata)
    if not result or result == "0x" or len(result) < 194:
        return None

    price_raw = _decode_int256(result, 0)
    conf = _decode_uint256(result, 1)
    expo = _decode_int256(result, 2)
    publish_time = _decode_uint256(result, 3)

    if price_raw <= 0:
        return None

    price = price_raw * (10 ** expo)
    latency = max(0, block_ts - publish_time) if publish_time > 0 else 0

    return {
        "oracle_price": price,
        "oracle_price_raw": str(price_raw),
        "oracle_decimals": abs(expo),
        "latency_seconds": latency,
        "round_id": None,
        "answer_timestamp": datetime.fromtimestamp(publish_time, tz=timezone.utc) if publish_time > 0 else None,
    }


async def _read_oracle(client: httpx.AsyncClient, oracle: dict) -> dict | None:
    """Read a single oracle. Dispatches by provider."""
    chain = oracle["chain"]
    rpc_url = _get_rpc_url(chain)
    if not rpc_url:
        logger.debug(f"No RPC for {chain}, skipping {oracle['oracle_name']}")
        return None

    block_ts = await _async_get_block_timestamp(client, rpc_url)
    provider = oracle.get("oracle_provider", "").lower()

    if provider == "chainlink":
        return await _read_chainlink_oracle(client, oracle, rpc_url, block_ts)
    elif provider == "pyth":
        return await _read_pyth_oracle(client, oracle, rpc_url, block_ts)
    else:
        logger.debug(f"Unknown oracle provider: {provider}")
        return None


# ---------------------------------------------------------------------------
# CEX reference prices
# ---------------------------------------------------------------------------

_last_cex_prices: dict[str, float] = {}

async def _fetch_cex_prices(client: httpx.AsyncClient, symbols: list[str]) -> dict[str, float]:
    """Fetch CEX prices from CoinGecko for all symbols in one call."""
    global _last_cex_prices

    cg_ids = []
    id_to_symbol = {}
    for sym in symbols:
        cg_id = SYMBOL_TO_COINGECKO.get(sym.upper())
        if cg_id:
            cg_ids.append(cg_id)
            id_to_symbol[cg_id] = sym.upper()

    if not cg_ids:
        return _last_cex_prices

    api_key = os.environ.get("COINGECKO_API_KEY", "")
    params = {"ids": ",".join(cg_ids), "vs_currencies": "usd"}
    headers = {}
    base_url = "https://api.coingecko.com/api/v3/simple/price"

    if api_key:
        headers["x-cg-pro-api-key"] = api_key
        base_url = "https://pro-api.coingecko.com/api/v3/simple/price"

    try:
        resp = await client.get(base_url, params=params, headers=headers, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            prices = {}
            for cg_id, sym in id_to_symbol.items():
                if cg_id in data and "usd" in data[cg_id]:
                    prices[sym] = data[cg_id]["usd"]
            _last_cex_prices.update(prices)
            return _last_cex_prices
    except Exception as e:
        logger.warning(f"CoinGecko price fetch failed: {e}")

    # Return last known prices as fallback
    return _last_cex_prices


# ---------------------------------------------------------------------------
# Stress event handling
# ---------------------------------------------------------------------------

def tag_pre_stress_readings(
    event_id: int,
    oracle_address: str,
    chain: str,
    asset_symbol: str,
    event_start,
    window_hours: int = PRE_STRESS_WINDOW_HOURS,
) -> int:
    """Retroactively tag the preceding `window_hours` of readings for a
    newly-opened stress event. Writes the tagged count back onto the event
    row so the triptych endpoint can verify completeness without a scan.

    Never raises — tagging failure must not block the stress event from
    opening. Returns the number of readings tagged, or 0 on any error.
    Idempotent via `pre_stress_event_id IS NULL` guard (no re-tagging on
    subsequent updates to the same event).
    """
    try:
        with get_cursor() as cur:
            cur.execute(
                """UPDATE oracle_price_readings
                   SET pre_stress_event_id = %s
                   WHERE oracle_address = %s
                     AND chain = %s
                     AND asset_symbol = %s
                     AND recorded_at >= %s - (%s || ' hours')::INTERVAL
                     AND recorded_at < %s
                     AND pre_stress_event_id IS NULL""",
                (event_id, oracle_address, chain, asset_symbol,
                 event_start, str(window_hours), event_start),
            )
            tagged = cur.rowcount or 0
            cur.execute(
                "UPDATE oracle_stress_events SET pre_stress_readings_tagged = %s WHERE id = %s",
                (tagged, event_id),
            )
        logger.info(
            f"Pre-stress tagging: event {event_id} ({oracle_address[:10]}.. "
            f"{asset_symbol}/{chain}) — tagged {tagged} readings in prior "
            f"{window_hours}h"
        )
        return tagged
    except Exception as e:
        logger.warning(f"Pre-stress tagging failed for event {event_id}: {e}")
        return 0


def _handle_stress_event(oracle: dict, reading: dict, deviation_pct: float, latency: int):
    """Handle oracle stress event: create or update."""
    oracle_addr = oracle["oracle_address"]
    asset = oracle["asset_symbol"]
    chain = oracle["chain"]

    # Classify event type using per-feed thresholds
    abs_dev = abs(deviation_pct)
    feed_dev = float(oracle.get("deviation_threshold_pct") or DEVIATION_STRESS_THRESHOLD)
    feed_hb = oracle.get("heartbeat_seconds")
    lat_thresh = int(float(feed_hb) * HEARTBEAT_BUFFER) if feed_hb else LATENCY_STRESS_THRESHOLD

    if latency > lat_thresh and abs_dev < feed_dev:
        event_type = "stale_price"
    elif abs_dev > 5.0:
        event_type = "flash_deviation"
    elif abs_dev > feed_dev:
        event_type = "high_deviation"
    else:
        event_type = "stale_price"

    # Check for open event
    open_event = fetch_one(
        """SELECT id, max_deviation_pct, max_latency_seconds, reading_count
           FROM oracle_stress_events
           WHERE oracle_address = %s AND chain = %s AND event_end IS NULL
           ORDER BY event_start DESC LIMIT 1""",
        (oracle_addr, chain),
    )

    if open_event:
        # Update existing event
        new_max_dev = max(abs_dev, float(open_event.get("max_deviation_pct") or 0))
        new_max_lat = max(latency, open_event.get("max_latency_seconds") or 0)
        new_count = (open_event.get("reading_count") or 1) + 1
        execute(
            """UPDATE oracle_stress_events
               SET max_deviation_pct = %s, max_latency_seconds = %s,
                   reading_count = %s
               WHERE id = %s""",
            (new_max_dev, new_max_lat, new_count, open_event["id"]),
        )
    else:
        # Fetch concurrent scores
        concurrent_sii = None
        concurrent_psi = {}
        entity_slug = oracle.get("entity_slug")

        if entity_slug:
            try:
                sii_row = fetch_one(
                    "SELECT overall_score FROM scores WHERE stablecoin_id = LOWER(%s) ORDER BY scored_at DESC LIMIT 1",
                    (entity_slug,),
                )
                if sii_row:
                    concurrent_sii = float(sii_row["overall_score"])
            except Exception:
                pass

        # Find affected protocols (protocols that use this oracle's asset)
        affected = []
        try:
            dep_rows = fetch_all(
                """SELECT DISTINCT entity_slug FROM contract_dependencies
                   WHERE depends_on_address = LOWER(%s) AND removed_at IS NULL""",
                (oracle_addr,),
            )
            affected = [r["entity_slug"] for r in (dep_rows or []) if r.get("entity_slug")]
        except Exception:
            pass

        for slug in affected:
            try:
                psi_row = fetch_one(
                    "SELECT overall_score FROM psi_scores WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1",
                    (slug,),
                )
                if psi_row:
                    concurrent_psi[slug] = float(psi_row["overall_score"])
            except Exception:
                pass

        now = datetime.now(timezone.utc)
        content_data = f"{oracle_addr}{chain}{event_type}{now.isoformat()}"
        content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

        new_event = fetch_one(
            """INSERT INTO oracle_stress_events
                (oracle_address, oracle_name, asset_symbol, chain,
                 event_type, event_start, max_deviation_pct, max_latency_seconds,
                 reading_count, concurrent_sii_score, concurrent_psi_scores,
                 affected_protocols, content_hash, attested_at,
                 pre_stress_window_hours)
               VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s, 1, %s, %s, %s, %s, NOW(), %s)
               RETURNING id, event_start""",
            (
                oracle_addr, oracle.get("oracle_name"), asset, chain,
                event_type, abs_dev, latency,
                concurrent_sii,
                json.dumps(concurrent_psi) if concurrent_psi else None,
                json.dumps(affected) if affected else None,
                content_hash,
                PRE_STRESS_WINDOW_HOURS,
            ),
        )

        if new_event and new_event.get("id"):
            tag_pre_stress_readings(
                event_id=new_event["id"],
                oracle_address=oracle_addr,
                chain=chain,
                asset_symbol=asset,
                event_start=new_event["event_start"],
                window_hours=PRE_STRESS_WINDOW_HOURS,
            )

        try:
            from app.state_attestation import attest_state
            attest_state("oracle_stress_events", [{
                "oracle_address": oracle_addr,
                "event_type": event_type,
                "asset_symbol": asset,
                "deviation_pct": deviation_pct,
            }])
        except Exception:
            pass

        logger.warning(
            f"ORACLE STRESS EVENT: {oracle.get('oracle_name')} "
            f"type={event_type} deviation={deviation_pct:.4f}% "
            f"latency={latency}s"
        )


def _close_stress_event_if_open(oracle: dict):
    """Close an open stress event if the oracle is now healthy."""
    oracle_addr = oracle["oracle_address"]
    chain = oracle["chain"]

    open_event = fetch_one(
        """SELECT id, event_start FROM oracle_stress_events
           WHERE oracle_address = %s AND chain = %s AND event_end IS NULL
           ORDER BY event_start DESC LIMIT 1""",
        (oracle_addr, chain),
    )
    if not open_event:
        return

    now = datetime.now(timezone.utc)
    event_start = open_event["event_start"]
    if event_start and event_start.tzinfo is None:
        event_start = event_start.replace(tzinfo=timezone.utc)
    duration = int((now - event_start).total_seconds()) if event_start else 0

    execute(
        """UPDATE oracle_stress_events
           SET event_end = NOW(), duration_seconds = %s
           WHERE id = %s""",
        (duration, open_event["id"]),
    )
    logger.info(
        f"ORACLE STRESS EVENT CLOSED: {oracle.get('oracle_name')} "
        f"duration={duration}s"
    )


# ---------------------------------------------------------------------------
# Main collector (FAST CYCLE)
# ---------------------------------------------------------------------------

async def collect_oracle_readings() -> dict:
    """
    Read all active oracles, compute deviation from CEX, detect stress events.
    Runs in the fast cycle (hourly). All oracle reads run concurrently.
    """
    results = {
        "oracles_read": 0,
        "stress_events_detected": 0,
        "readings_stored": 0,
        "errors": [],
    }

    # Load active oracles
    oracles = fetch_all("SELECT * FROM oracle_registry WHERE is_active = TRUE")
    if not oracles:
        logger.info("Oracle behavior: no active oracles in registry")
        return results

    # Collect distinct asset symbols for CEX price fetch
    symbols = list({o["asset_symbol"] for o in oracles})

    async with httpx.AsyncClient(timeout=20) as client:
        # Fetch CEX prices (one call)
        cex_prices = await _fetch_cex_prices(client, symbols)

        # Read all oracles concurrently
        async def _process_oracle(oracle: dict) -> dict | None:
            try:
                reading = await _read_oracle(client, oracle)
                if not reading:
                    logger.error(f"[oracle] {oracle.get('oracle_name', '?')}: read returned None (no RPC or bad response)")
                    return None

                oracle_price = reading["oracle_price"]
                asset = oracle["asset_symbol"]
                latency = reading.get("latency_seconds", 0)

                # Get CEX reference price
                # For stETH/ETH feed, compare against ETH price ratio
                cex_price = cex_prices.get(asset.upper())
                if oracle.get("quote_symbol") == "eth" and asset.upper() == "STETH":
                    eth_price = cex_prices.get("ETH")
                    steth_price = cex_prices.get("STETH")
                    if eth_price and steth_price:
                        cex_price = steth_price / eth_price

                # Compute deviation
                deviation_pct = 0.0
                deviation_abs = 0.0
                if cex_price and cex_price > 0:
                    deviation_pct = ((oracle_price - cex_price) / cex_price) * 100
                    deviation_abs = abs(oracle_price - cex_price)

                # Per-feed stress thresholds from oracle_registry
                feed_dev_threshold = oracle.get("deviation_threshold_pct")
                feed_heartbeat = oracle.get("heartbeat_seconds")
                dev_thresh = float(feed_dev_threshold) if feed_dev_threshold else DEVIATION_STRESS_THRESHOLD
                lat_thresh = int(float(feed_heartbeat) * HEARTBEAT_BUFFER) if feed_heartbeat else LATENCY_STRESS_THRESHOLD

                is_stress = (
                    abs(deviation_pct) > dev_thresh
                    or latency > lat_thresh
                )

                now = datetime.now(timezone.utc)
                content_data = f"{oracle['oracle_address']}{now.isoformat()}{reading['oracle_price_raw']}"
                content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

                # Store reading
                execute(
                    """INSERT INTO oracle_price_readings
                        (oracle_address, oracle_name, oracle_provider, chain,
                         asset_symbol, quote_symbol, oracle_price, oracle_price_raw,
                         oracle_decimals, cex_price, deviation_pct, deviation_abs,
                         latency_seconds, round_id, answer_timestamp,
                         is_stress_event, content_hash, attested_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                               %s, %s, %s, %s, %s, NOW())""",
                    (
                        oracle["oracle_address"], oracle.get("oracle_name"),
                        oracle.get("oracle_provider"), oracle["chain"],
                        asset, oracle.get("quote_symbol", "usd"),
                        oracle_price, reading["oracle_price_raw"],
                        reading.get("oracle_decimals"),
                        cex_price, round(deviation_pct, 6), round(deviation_abs, 8),
                        latency, reading.get("round_id"),
                        reading.get("answer_timestamp"),
                        is_stress, content_hash,
                    ),
                )

                try:
                    from app.state_attestation import attest_state
                    attest_state("oracle_readings", [{
                        "oracle_address": oracle["oracle_address"],
                        "oracle_price": oracle_price,
                        "deviation_pct": deviation_pct,
                    }])
                except Exception:
                    pass

                # Handle stress events
                if is_stress:
                    _handle_stress_event(oracle, reading, deviation_pct, latency)
                    return {"stress": True}
                else:
                    _close_stress_event_if_open(oracle)
                    return {"stress": False}

            except Exception as e:
                logger.error(f"[oracle] {oracle.get('oracle_name', '?')}: {type(e).__name__} — {e}")
                return None

        # Run all oracle reads concurrently
        tasks = [_process_oracle(o) for o in oracles]
        oracle_results = await asyncio.gather(*tasks)

        for i, res in enumerate(oracle_results):
            if res is not None:
                results["oracles_read"] += 1
                results["readings_stored"] += 1
                if res.get("stress"):
                    results["stress_events_detected"] += 1
            else:
                results["errors"].append(oracles[i].get("oracle_name", "unknown"))

    logger.error(
        f"[oracle] summary: read={results['oracles_read']} "
        f"stored={results['readings_stored']} "
        f"stress={results['stress_events_detected']} "
        f"errors={results['errors']}"
    )
    return results
