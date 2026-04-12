"""
CoinGecko ID Resolver
======================
Resolves CoinGecko IDs for assets that were discovered from sources
(DeFiLlama, on-chain indexer) that don't include CoinGecko mappings.

Uses the CoinGecko /coins/list endpoint (one call, returns ~15K coins)
and matches by contract address first, then by symbol.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

import httpx

from app.database import execute, fetch_all, fetch_one
from app.data_source_registry import register_data_source

logger = logging.getLogger(__name__)

API_KEY = os.environ.get("COINGECKO_API_KEY", "")
BASE_URL = "https://pro-api.coingecko.com/api/v3" if API_KEY else "https://api.coingecko.com/api/v3"
CACHE_PATH = Path("/tmp/coingecko_coins_list.json")
CACHE_TTL = 86400  # 24 hours


def _headers() -> dict:
    h = {"Accept": "application/json"}
    if API_KEY:
        h["x-cg-pro-api-key"] = API_KEY
    return h


def fetch_coins_list() -> list[dict]:
    """Fetch the full CoinGecko coins list with platform addresses.

    Returns ~15K entries. Cached locally for 24 hours.
    """
    # Check cache
    if CACHE_PATH.exists():
        age = time.time() - CACHE_PATH.stat().st_mtime
        if age < CACHE_TTL:
            try:
                data = json.loads(CACHE_PATH.read_text())
                logger.info(f"CoinGecko coins list loaded from cache ({len(data)} coins, {age/3600:.1f}h old)")
                return data
            except Exception:
                pass

    # Fetch from API
    register_data_source("pro-api.coingecko.com", "/api/v3/coins/list", "coingecko_resolver",
                         description="CoinGecko coins list for ID resolution",
                         params_template={"include_platform": "true"}, prove_frequency="daily")
    logger.info("Fetching CoinGecko coins list (include_platform=true)...")
    try:
        resp = httpx.get(
            f"{BASE_URL}/coins/list",
            params={"include_platform": "true"},
            headers=_headers(),
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
        # Cache it
        CACHE_PATH.write_text(json.dumps(data))
        logger.info(f"CoinGecko coins list fetched and cached: {len(data)} coins")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch CoinGecko coins list: {e}")
        # Try stale cache
        if CACHE_PATH.exists():
            return json.loads(CACHE_PATH.read_text())
        return []


def build_lookup_indices(coins_list: list[dict]) -> dict:
    """Build fast lookup indices from the coins list.

    Returns dict with:
      - by_address: {lowercase_address: coingecko_id}
      - by_symbol: {lowercase_symbol: [list of coingecko_ids]}
      - by_name: {lowercase_name: coingecko_id}
    """
    by_address = {}
    by_symbol = {}
    by_name = {}

    for coin in coins_list:
        cg_id = coin.get("id", "")
        symbol = (coin.get("symbol") or "").lower()
        name = (coin.get("name") or "").lower()
        platforms = coin.get("platforms") or {}

        # Index all contract addresses across all chains
        for chain, addr in platforms.items():
            if addr:
                by_address[addr.lower()] = cg_id

        # Index by symbol (can have collisions)
        by_symbol.setdefault(symbol, []).append(cg_id)

        # Index by name
        if name:
            by_name[name] = cg_id

    return {"by_address": by_address, "by_symbol": by_symbol, "by_name": by_name}


def resolve_single(
    indices: dict,
    token_address: str,
    symbol: str,
    name: str,
) -> tuple[Optional[str], Optional[str]]:
    """Resolve a CoinGecko ID for a single asset.

    Returns (coingecko_id, match_method) or (None, None).
    """
    # 1. Match by contract address (highest confidence)
    addr_lower = (token_address or "").lower()
    if addr_lower and addr_lower in indices["by_address"]:
        return indices["by_address"][addr_lower], "contract_address"

    # 2. Match by symbol (medium confidence — only if unambiguous)
    sym_lower = (symbol or "").lower()
    if sym_lower and sym_lower in indices["by_symbol"]:
        candidates = indices["by_symbol"][sym_lower]
        if len(candidates) == 1:
            return candidates[0], "symbol_unique"
        # Try to disambiguate by name
        name_lower = (name or "").lower()
        for cg_id in candidates:
            if name_lower and name_lower in cg_id:
                return cg_id, "symbol_name_match"

    # 3. Match by name (lower confidence)
    name_lower = (name or "").lower()
    if name_lower and name_lower in indices["by_name"]:
        return indices["by_name"][name_lower], "name"

    return None, None


def resolve_unscored_stablecoins() -> dict:
    """Resolve CoinGecko IDs for all unscored stablecoins that lack one.

    Returns summary dict with counts.
    """
    coins_list = fetch_coins_list()
    if not coins_list:
        return {"error": "Failed to fetch CoinGecko coins list"}

    indices = build_lookup_indices(coins_list)
    logger.info(
        f"CoinGecko indices built: {len(indices['by_address'])} addresses, "
        f"{len(indices['by_symbol'])} symbols, {len(indices['by_name'])} names"
    )

    # Get unresolved stablecoins
    unresolved = fetch_all("""
        SELECT token_address, symbol, name
        FROM wallet_graph.unscored_assets
        WHERE token_type = 'stablecoin'
          AND coingecko_id IS NULL
          AND scoring_status = 'unscored'
        ORDER BY (total_value_held + COALESCE(protocol_collateral_tvl, 0) * 2) DESC
    """)

    if not unresolved:
        return {"unresolved": 0, "resolved": 0}

    results = {"by_contract_address": 0, "by_symbol_unique": 0,
               "by_symbol_name_match": 0, "by_name": 0, "unresolved": 0}
    resolved_details = []

    for asset in unresolved:
        # Skip obvious spam
        if asset["name"] and len(asset["name"]) > 60:
            results["unresolved"] += 1
            continue

        cg_id, method = resolve_single(
            indices, asset["token_address"], asset["symbol"], asset["name"]
        )

        if cg_id:
            # Update the database
            try:
                execute(
                    "UPDATE wallet_graph.unscored_assets SET coingecko_id = %s WHERE token_address = %s",
                    (cg_id, asset["token_address"]),
                )
                key = f"by_{method}"
                if key in results:
                    results[key] += 1
                else:
                    results[key] = results.get(key, 0) + 1
                resolved_details.append({
                    "symbol": asset["symbol"],
                    "name": asset["name"],
                    "coingecko_id": cg_id,
                    "method": method,
                })
                logger.info(f"Resolved {asset['symbol']} → {cg_id} (via {method})")
            except Exception as e:
                logger.warning(f"Failed to update {asset['symbol']}: {e}")
                results["unresolved"] += 1
        else:
            results["unresolved"] += 1

    total_resolved = sum(v for k, v in results.items() if k != "unresolved")
    return {
        "total_unresolved_before": len(unresolved),
        "resolved": total_resolved,
        "still_unresolved": results["unresolved"],
        "by_method": {k: v for k, v in results.items() if k != "unresolved"},
        "details": resolved_details,
    }


# Known protocol → CoinGecko token ID mappings for PSI protocols
# These are the governance/utility tokens for DeFi protocols
PROTOCOL_TOKEN_MAP = {
    "merkl": "angle-protocol",       # ANGLE token
    "sky-lending": "maker",          # MKR/SKY token (Sky is rebranded MakerDAO)
    "morpho-v1": "morpho",           # MORPHO token
    "maple": "maple",                # MPL token
    "ethena-usde": "ethena",         # ENA token
    "pendle": "pendle",
    "instadapp": "instadapp",
    "yearn-finance": "yearn-finance",
    "pancakeswap": "pancakeswap-token",
    "sushi": "sushi",
    "balancer": "balancer",
    "gmx": "gmx",
    "venus": "venus",
    "benqi": "benqi",
    "radiant": "radiant-capital",
}


def resolve_psi_protocol_tokens() -> dict:
    """Resolve CoinGecko IDs for PSI protocols that are missing gecko_id.

    Uses known protocol→token mappings first, then falls back to
    CoinGecko search by protocol name.
    """
    coins_list = fetch_coins_list()
    indices = build_lookup_indices(coins_list)

    # Get protocols missing gecko_id
    unresolved = fetch_all("""
        SELECT slug, name FROM protocol_backlog
        WHERE gecko_id IS NULL
          AND enrichment_status IN ('discovered', 'enriching')
        ORDER BY stablecoin_exposure_usd DESC
    """)

    if not unresolved:
        return {"unresolved": 0, "resolved": 0}

    resolved = []
    still_unresolved = []

    for proto in unresolved:
        slug = proto["slug"]
        name = (proto["name"] or "").lower()

        # Try known mapping first
        cg_id = PROTOCOL_TOKEN_MAP.get(slug)
        method = "known_mapping"

        if not cg_id:
            # Try symbol match (protocol slug often matches token symbol)
            cg_id, method = resolve_single(indices, "", slug, proto["name"])

        if not cg_id:
            # Try name-based search
            # Many protocols have tokens named after them
            if name in indices["by_name"]:
                cg_id = indices["by_name"][name]
                method = "name"

        if cg_id:
            try:
                execute(
                    "UPDATE protocol_backlog SET gecko_id = %s WHERE slug = %s",
                    (cg_id, slug),
                )
                resolved.append({"slug": slug, "gecko_id": cg_id, "method": method})
                logger.info(f"PSI: Resolved {slug} → {cg_id} (via {method})")
            except Exception as e:
                logger.warning(f"PSI: Failed to update {slug}: {e}")
                still_unresolved.append(slug)
        else:
            still_unresolved.append(slug)

    return {
        "total": len(unresolved),
        "resolved": len(resolved),
        "still_unresolved": len(still_unresolved),
        "details": resolved,
        "unresolved_slugs": still_unresolved,
    }
