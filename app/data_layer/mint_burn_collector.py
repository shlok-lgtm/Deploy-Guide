"""
Per-Event Mint/Burn Capture
============================
Etherscan event logs: Transfer events from/to zero address = mints and burns.
Pull for all scored stablecoins, all three chains, continuously.
Store individual events with timestamp, block, amount, sender/receiver, chain.

Detects: "$50M mint at 3am UTC" → discovery signal before anyone notices.
"Redemption acceleration: 14 burns >$1M in last 6h, 3.2σ above 30d mean."

Sources:
- Etherscan V2: tokentx endpoint filtered for zero-address transfers

Schedule: Daily (with lookback to last collected block)
"""

import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Chain IDs for Etherscan V2
CHAIN_IDS = {
    "ethereum": 1,
    "base": 8453,
    "arbitrum": 42161,
}


async def _fetch_token_transfers(
    client: httpx.AsyncClient,
    contract: str,
    chain: str,
    start_block: int = 0,
    page: int = 1,
) -> list[dict]:
    """Fetch token transfer events from Etherscan V2."""
    from app.shared_rate_limiter import rate_limiter
    from app.api_usage_tracker import track_api_call

    await rate_limiter.acquire("etherscan")

    chain_id = CHAIN_IDS.get(chain, 1)
    params = {
        "chainid": chain_id,
        "module": "account",
        "action": "tokentx",
        "contractaddress": contract,
        "startblock": start_block,
        "endblock": 99999999,
        "page": page,
        "offset": 100,
        "sort": "desc",
        "apikey": ETHERSCAN_API_KEY,
    }

    start = time.time()
    try:
        resp = await client.get(ETHERSCAN_V2_BASE, params=params, timeout=15)
        latency = int((time.time() - start) * 1000)
        track_api_call("etherscan", "/tokentx", caller="mint_burn_collector",
                       status=resp.status_code, latency_ms=latency)

        if resp.status_code == 429 or "Max rate limit" in resp.text:
            rate_limiter.report_429("etherscan")
            return []

        resp.raise_for_status()
        rate_limiter.report_success("etherscan")
        data = resp.json()

        if data.get("status") == "1":
            return data.get("result", [])
        return []
    except Exception as e:
        latency = int((time.time() - start) * 1000)
        track_api_call("etherscan", "/tokentx", caller="mint_burn_collector",
                       status=500, latency_ms=latency)
        logger.warning(f"Token transfer fetch failed for {contract} on {chain}: {e}")
        return []


def _is_mint(tx: dict) -> bool:
    """Check if transfer is a mint (from zero address)."""
    return (tx.get("from") or "").lower() == ZERO_ADDRESS


def _is_burn(tx: dict) -> bool:
    """Check if transfer is a burn (to zero address)."""
    return (tx.get("to") or "").lower() == ZERO_ADDRESS


def _parse_amount(tx: dict) -> float:
    """Parse token amount from raw value and decimals."""
    try:
        raw = int(tx.get("value", "0"))
        decimals = int(tx.get("tokenDecimal", "18"))
        return raw / (10 ** decimals)
    except (ValueError, ZeroDivisionError):
        return 0


def _safe_float(val):
    """Return None if val is NaN or Infinity, otherwise float."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    except (TypeError, ValueError):
        return None


def _store_mint_burn_events(events: list[dict]):
    """Store mint/burn events to database (per-row transactions)."""
    if not events:
        return

    from app.database import get_cursor

    stored = 0
    errors = 0

    for evt in events:
        try:
            with get_cursor() as cur:
                cur.execute(
                    """INSERT INTO mint_burn_events
                       (stablecoin_id, chain, event_type, amount, tx_hash,
                        block_number, from_address, to_address, timestamp,
                        raw_data, collected_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (chain, tx_hash, event_type) DO NOTHING""",
                    (
                        evt["stablecoin_id"], evt["chain"], evt["event_type"],
                        _safe_float(evt["amount"]), evt["tx_hash"],
                        evt.get("block_number"),
                        evt.get("from_address"), evt.get("to_address"),
                        evt.get("timestamp"),
                        json.dumps(evt.get("raw_data")) if evt.get("raw_data") else None,
                    ),
                )
            stored += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.error(f"Failed to store mint_burn event tx_hash={evt.get('tx_hash')}: {e}")

    if errors:
        logger.error(f"mint_burn_events: stored={stored}, errors={errors} out of {len(events)}")
    else:
        logger.info(f"Stored {stored} mint/burn events")


def _get_last_block(stablecoin_id: str, chain: str) -> int:
    """Get the last block we collected for this stablecoin on this chain."""
    from app.database import fetch_one
    row = fetch_one(
        """SELECT MAX(block_number) as last_block FROM mint_burn_events
           WHERE stablecoin_id = %s AND chain = %s""",
        (stablecoin_id, chain),
    )
    return int(row["last_block"]) if row and row.get("last_block") else 0


async def run_mint_burn_collection() -> dict:
    """
    Full mint/burn collection cycle.
    For each stablecoin on each chain, fetch recent Transfer events
    from/to zero address. Store individual events.

    Returns summary + any anomaly signals.
    """
    from app.database import fetch_all

    rows = fetch_all(
        """SELECT id, symbol, contract, decimals
           FROM stablecoins WHERE scoring_enabled = TRUE AND contract IS NOT NULL"""
    )
    if not rows:
        return {"error": "no stablecoins with contracts found"}

    total_mints = 0
    total_burns = 0
    large_events = []

    async with httpx.AsyncClient(timeout=30) as client:
        for row in rows:
            stablecoin_id = row["id"]
            contract = row.get("contract", "")
            if not contract or not contract.startswith("0x"):
                continue

            for chain in ["ethereum", "base", "arbitrum"]:
                try:
                    last_block = _get_last_block(stablecoin_id, chain)
                    transfers = await _fetch_token_transfers(
                        client, contract, chain, start_block=last_block
                    )

                    events = []
                    for tx in transfers:
                        if not _is_mint(tx) and not _is_burn(tx):
                            continue

                        amount = _parse_amount(tx)
                        if amount < 1000:  # Skip dust
                            continue

                        event_type = "mint" if _is_mint(tx) else "burn"
                        ts = None
                        if tx.get("timeStamp"):
                            try:
                                ts = datetime.fromtimestamp(
                                    int(tx["timeStamp"]), tz=timezone.utc
                                )
                            except (ValueError, OSError):
                                pass

                        evt = {
                            "stablecoin_id": stablecoin_id,
                            "chain": chain,
                            "event_type": event_type,
                            "amount": amount,
                            "tx_hash": tx.get("hash", ""),
                            "block_number": int(tx.get("blockNumber", 0)),
                            "from_address": tx.get("from"),
                            "to_address": tx.get("to"),
                            "timestamp": ts,
                            "raw_data": {
                                "gas_used": tx.get("gasUsed"),
                                "gas_price": tx.get("gasPrice"),
                                "nonce": tx.get("nonce"),
                            },
                        }
                        events.append(evt)

                        if event_type == "mint":
                            total_mints += 1
                        else:
                            total_burns += 1

                        # Flag large events (>$1M)
                        if amount >= 1_000_000:
                            large_events.append({
                                "stablecoin": stablecoin_id,
                                "type": event_type,
                                "amount": amount,
                                "chain": chain,
                                "tx_hash": tx.get("hash", ""),
                                "timestamp": ts.isoformat() if ts else None,
                            })

                    if events:
                        _store_mint_burn_events(events)

                except Exception as e:
                    logger.warning(
                        f"Mint/burn collection failed for {stablecoin_id} on {chain}: {e}"
                    )

    # Emit discovery signals for large events
    if large_events:
        try:
            from app.database import execute as db_execute
            for evt in large_events[:10]:  # Cap at 10 signals per cycle
                db_execute(
                    """INSERT INTO discovery_signals
                       (signal_type, domain, entity_id, severity, title, details, created_at)
                       VALUES ('large_mint_burn', 'sii', %s, 'notable', %s, %s, NOW())
                       ON CONFLICT DO NOTHING""",
                    (
                        evt["stablecoin"],
                        f"Large {evt['type']}: {evt['stablecoin']} ${evt['amount']:,.0f}",
                        json.dumps(evt),
                    ),
                )
        except Exception as e:
            logger.debug(f"Mint/burn signal emission failed: {e}")

    # Check for redemption acceleration pattern
    anomalies = _detect_redemption_acceleration()

    # Provenance
    try:
        from app.data_layer.provenance_scaling import attest_data_batch, link_batch_to_proof
        if total_mints + total_burns > 0:
            attest_data_batch("mint_burn_events", [{"mints": total_mints, "burns": total_burns}])
            link_batch_to_proof("mint_burn_events", "mint_burn_events")
    except Exception as e:
        logger.debug(f"Mint/burn provenance failed: {e}")

    logger.info(
        f"Mint/burn collection complete: {total_mints} mints, {total_burns} burns, "
        f"{len(large_events)} large events, {len(anomalies)} anomalies"
    )

    return {
        "total_mints": total_mints,
        "total_burns": total_burns,
        "large_events": large_events,
        "anomalies": anomalies,
    }


def _detect_redemption_acceleration() -> list[dict]:
    """
    Detect acceleration patterns: burns >$1M in last 6h significantly above
    30d mean. Emit discovery signals.
    """
    from app.database import fetch_all
    import statistics

    anomalies = []

    try:
        # Get 6h burn counts per stablecoin
        recent = fetch_all(
            """SELECT stablecoin_id, COUNT(*) as burn_count, SUM(amount) as total_amount
               FROM mint_burn_events
               WHERE event_type = 'burn' AND amount >= 1000000
                 AND timestamp >= NOW() - INTERVAL '6 hours'
               GROUP BY stablecoin_id"""
        )

        for row in (recent or []):
            stablecoin_id = row["stablecoin_id"]
            recent_count = row["burn_count"]

            # Get 30d mean per 6h window
            historical = fetch_all(
                """SELECT DATE_TRUNC('hour', timestamp) as hour_bucket,
                          COUNT(*) as burn_count
                   FROM mint_burn_events
                   WHERE stablecoin_id = %s AND event_type = 'burn'
                     AND amount >= 1000000
                     AND timestamp >= NOW() - INTERVAL '30 days'
                   GROUP BY hour_bucket""",
                (stablecoin_id,),
            )

            if not historical or len(historical) < 5:
                continue

            counts = [h["burn_count"] for h in historical]
            mean = statistics.mean(counts)
            stdev = statistics.stdev(counts) if len(counts) > 1 else 1

            if stdev > 0:
                z_score = (recent_count - mean) / stdev
                if z_score >= 2.0:
                    anomalies.append({
                        "stablecoin_id": stablecoin_id,
                        "recent_large_burns_6h": recent_count,
                        "total_amount_6h": float(row["total_amount"]),
                        "mean_per_6h_window": round(mean, 2),
                        "z_score": round(z_score, 2),
                    })
    except Exception as e:
        logger.debug(f"Redemption acceleration detection failed: {e}")

    return anomalies
