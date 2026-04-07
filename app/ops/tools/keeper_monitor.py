"""
Keeper Wallet Balance & Gas Burn Tracker
=========================================
Monitors the oracle keeper wallet on Base and Arbitrum.
Uses JSON-RPC for balances and Etherscan V2 for transaction history.
Results cached for 5 minutes.
"""

import logging
import os
import time
from datetime import datetime, timezone

import httpx

logger = logging.getLogger(__name__)

KEEPER_ADDRESS = "0x2dF0f62D1861Aa59A4430e3B2b2E7a0D29Cb723b"
ORACLE_CONTRACT = "0x01aAa1D20Fe68D55d0C5B6b42399b91024F8cD99"

CHAINS = {
    "base": {"chain_id": 8453, "rpc_env": "BASE_RPC_URL", "name": "Base"},
    "arbitrum": {"chain_id": 42161, "rpc_env": "ARBITRUM_RPC_URL", "name": "Arbitrum"},
}

ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"
COINGECKO_ETH_URL = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"

# Simple in-memory cache: key -> (timestamp, data)
_cache: dict[str, tuple[float, dict]] = {}
CACHE_TTL = 300  # 5 minutes


def _cache_get(key: str) -> dict | None:
    entry = _cache.get(key)
    if entry and (time.time() - entry[0]) < CACHE_TTL:
        return entry[1]
    return None


def _cache_set(key: str, data: dict):
    _cache[key] = (time.time(), data)


async def _rpc_get_balance(client: httpx.AsyncClient, rpc_url: str, address: str) -> float:
    """Get ETH balance via JSON-RPC. Returns balance in ETH."""
    resp = await client.post(
        rpc_url,
        json={
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": [address, "latest"],
            "id": 1,
        },
        timeout=10,
    )
    data = resp.json()
    hex_balance = data.get("result", "0x0")
    return int(hex_balance, 16) / 1e18


async def _fetch_eth_price(client: httpx.AsyncClient) -> float:
    """Fetch current ETH/USD price from CoinGecko."""
    cached = _cache_get("eth_price")
    if cached:
        return cached.get("price", 0)
    try:
        api_key = os.environ.get("COINGECKO_API_KEY", "")
        headers = {"x-cg-pro-api-key": api_key} if api_key else {}
        resp = await client.get(COINGECKO_ETH_URL, headers=headers, timeout=10)
        price = resp.json().get("ethereum", {}).get("usd", 0)
        _cache_set("eth_price", {"price": price})
        return price
    except Exception as e:
        logger.warning(f"Failed to fetch ETH price: {e}")
        return 0


async def _fetch_keeper_txs(
    client: httpx.AsyncClient, chain_id: int, api_key: str, limit: int = 200
) -> list[dict]:
    """Fetch recent transactions FROM keeper TO oracle contract via Etherscan V2."""
    params = {
        "chainid": chain_id,
        "module": "account",
        "action": "txlist",
        "address": KEEPER_ADDRESS,
        "sort": "desc",
        "page": 1,
        "offset": limit,
        "apikey": api_key,
    }
    try:
        resp = await client.get(ETHERSCAN_V2_BASE, params=params, timeout=20)
        data = resp.json()
        if data.get("status") == "1" and isinstance(data.get("result"), list):
            # Filter to only txs sent to the oracle contract
            return [
                tx for tx in data["result"]
                if tx.get("to", "").lower() == ORACLE_CONTRACT.lower()
            ]
        return []
    except Exception as e:
        logger.error(f"Etherscan tx fetch error (chain {chain_id}): {e}")
        return []


def _parse_tx(tx: dict, eth_price: float) -> dict:
    """Parse an Etherscan tx into a clean dict with gas cost."""
    gas_used = int(tx.get("gasUsed", 0))
    gas_price_wei = int(tx.get("gasPrice", 0))
    gas_cost_eth = gas_used * gas_price_wei / 1e18
    ts = int(tx.get("timeStamp", 0))
    return {
        "hash": tx.get("hash", ""),
        "block": int(tx.get("blockNumber", 0)),
        "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else None,
        "gas_used": gas_used,
        "gas_price_gwei": round(gas_price_wei / 1e9, 4),
        "gas_cost_eth": round(gas_cost_eth, 8),
        "gas_cost_usd": round(gas_cost_eth * eth_price, 4),
        "status": "success" if tx.get("txreceipt_status") == "1" else "failed",
    }


def _sum_gas(parsed_txs: list[dict], since_ts: float) -> dict:
    """Sum gas costs for txs after since_ts."""
    total_eth = 0.0
    total_usd = 0.0
    count = 0
    for tx in parsed_txs:
        if tx["timestamp"] and datetime.fromisoformat(tx["timestamp"]).timestamp() >= since_ts:
            total_eth += tx["gas_cost_eth"]
            total_usd += tx["gas_cost_usd"]
            count += 1
    return {"eth": round(total_eth, 8), "usd": round(total_usd, 4), "tx_count": count}


async def get_keeper_status() -> dict:
    """Full keeper status across both chains. Cached for 5 minutes."""
    cached = _cache_get("keeper_status")
    if cached:
        return cached

    etherscan_key = os.environ.get("ETHERSCAN_API_KEY", "")
    now = time.time()
    since_24h = now - 86400
    since_7d = now - 86400 * 7

    async with httpx.AsyncClient() as client:
        eth_price = await _fetch_eth_price(client)

        chain_results = {}
        for chain_key, chain_cfg in CHAINS.items():
            rpc_url = os.environ.get(chain_cfg["rpc_env"], "")
            if not rpc_url:
                chain_results[chain_key] = {
                    "error": f"{chain_cfg['rpc_env']} not configured",
                    "balance_eth": 0, "balance_usd": 0,
                    "gas_24h": {"eth": 0, "usd": 0, "tx_count": 0},
                    "gas_7d": {"eth": 0, "usd": 0, "tx_count": 0},
                    "last_tx": None, "txs": [],
                }
                continue

            # Fetch balance and txs concurrently
            try:
                balance_eth = await _rpc_get_balance(client, rpc_url, KEEPER_ADDRESS)
            except Exception as e:
                logger.error(f"RPC balance error ({chain_key}): {e}")
                balance_eth = 0

            txs_raw = await _fetch_keeper_txs(client, chain_cfg["chain_id"], etherscan_key)
            parsed = [_parse_tx(tx, eth_price) for tx in txs_raw]

            gas_24h = _sum_gas(parsed, since_24h)
            gas_7d = _sum_gas(parsed, since_7d)
            last_tx = parsed[0] if parsed else None

            chain_results[chain_key] = {
                "balance_eth": round(balance_eth, 6),
                "balance_usd": round(balance_eth * eth_price, 2),
                "gas_24h": gas_24h,
                "gas_7d": gas_7d,
                "last_tx": last_tx,
                "txs": parsed,
            }

        total_balance_eth = sum(c["balance_eth"] for c in chain_results.values())
        total_balance_usd = sum(c["balance_usd"] for c in chain_results.values())

        # Estimated runway: total balance / avg daily gas burn
        total_7d_gas_eth = sum(c["gas_7d"]["eth"] for c in chain_results.values())
        avg_daily_gas = total_7d_gas_eth / 7 if total_7d_gas_eth > 0 else 0
        runway_days = round(total_balance_eth / avg_daily_gas) if avg_daily_gas > 0 else None

        result = {
            "keeper_address": KEEPER_ADDRESS,
            "oracle_address": ORACLE_CONTRACT,
            "eth_price_usd": eth_price,
            "balance_base_eth": chain_results.get("base", {}).get("balance_eth", 0),
            "balance_base_usd": chain_results.get("base", {}).get("balance_usd", 0),
            "balance_arbitrum_eth": chain_results.get("arbitrum", {}).get("balance_eth", 0),
            "balance_arbitrum_usd": chain_results.get("arbitrum", {}).get("balance_usd", 0),
            "total_balance_eth": round(total_balance_eth, 6),
            "total_balance_usd": round(total_balance_usd, 2),
            "last_24h_gas_spent_base": chain_results.get("base", {}).get("gas_24h", {}),
            "last_24h_gas_spent_arbitrum": chain_results.get("arbitrum", {}).get("gas_24h", {}),
            "last_7d_gas_spent_total": {
                "eth": round(total_7d_gas_eth, 8),
                "usd": round(sum(c["gas_7d"]["usd"] for c in chain_results.values()), 4),
                "tx_count": sum(c["gas_7d"]["tx_count"] for c in chain_results.values()),
            },
            "estimated_runway_days": runway_days,
            "avg_daily_gas_eth": round(avg_daily_gas, 8),
            "last_tx_base": chain_results.get("base", {}).get("last_tx"),
            "last_tx_arbitrum": chain_results.get("arbitrum", {}).get("last_tx"),
            "chains": {k: {"error": v.get("error")} for k, v in chain_results.items() if v.get("error")},
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }

    _cache_set("keeper_status", result)
    return result


async def get_keeper_history(limit: int = 50) -> dict:
    """Recent keeper transactions across both chains, merged and sorted."""
    cached = _cache_get(f"keeper_history_{limit}")
    if cached:
        return cached

    etherscan_key = os.environ.get("ETHERSCAN_API_KEY", "")

    async with httpx.AsyncClient() as client:
        eth_price = await _fetch_eth_price(client)

        all_txs = []
        for chain_key, chain_cfg in CHAINS.items():
            txs_raw = await _fetch_keeper_txs(
                client, chain_cfg["chain_id"], etherscan_key, limit=limit
            )
            for tx in txs_raw:
                parsed = _parse_tx(tx, eth_price)
                parsed["chain"] = chain_cfg["name"]
                parsed["chain_id"] = chain_cfg["chain_id"]
                all_txs.append(parsed)

        # Sort by timestamp descending
        all_txs.sort(key=lambda t: t.get("timestamp") or "", reverse=True)
        all_txs = all_txs[:limit]

        # Running gas total
        cumulative_eth = 0.0
        for tx in reversed(all_txs):
            cumulative_eth += tx["gas_cost_eth"]
            tx["cumulative_gas_eth"] = round(cumulative_eth, 8)

        result = {
            "keeper_address": KEEPER_ADDRESS,
            "oracle_address": ORACLE_CONTRACT,
            "transactions": all_txs,
            "total_gas_eth": round(cumulative_eth, 8),
            "total_gas_usd": round(sum(t["gas_cost_usd"] for t in all_txs), 4),
            "tx_count": len(all_txs),
            "cached_at": datetime.now(timezone.utc).isoformat(),
        }

    _cache_set(f"keeper_history_{limit}", result)
    return result
