"""
PSI Data Collector
===================
Fetches protocol data from DeFiLlama's free API and scores protocols
using the generic scoring engine with the PSI v0.1 definition.
"""

import json
import logging
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.psi_v01 import PSI_V01_DEFINITION, TARGET_PROTOCOLS
from app.scoring_engine import score_entity

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"

# Known stablecoin symbols for treasury matching (case-insensitive)
KNOWN_STABLECOIN_SYMBOLS = {
    "USDC", "USDT", "DAI", "FRAX", "PYUSD", "TUSD", "USDE", "USDD",
    "FDUSD", "SUSD", "LUSD", "CRVUSD", "GHO", "DOLA", "MIM", "USD1",
    "BUSD", "GUSD", "USDP", "RAI", "BEAN", "EUSD", "ZUSD", "HAY",
    "ALUSD", "MKUSD", "USDJ", "USDN", "UST", "CUSD", "MUSD", "HUSD",
    "OUSD", "SFRAX", "SDAI",
}

# Partial name matches for stablecoins (lowercase)
STABLECOIN_NAME_PATTERNS = [
    "usd coin", "tether", "dai stablecoin", "frax", "binance usd",
    "trueusd", "paypal usd", "first digital usd",
]


def _is_stablecoin_token(name, symbol):
    """Check if a token is a stablecoin based on name/symbol."""
    if not symbol and not name:
        return False
    sym_upper = (symbol or "").upper().strip()
    if sym_upper in KNOWN_STABLECOIN_SYMBOLS:
        return True
    name_lower = (name or "").lower().strip()
    for pat in STABLECOIN_NAME_PATTERNS:
        if pat in name_lower:
            return True
    return False


def parse_treasury_tokens(treasury_data):
    """Parse per-token holdings from DeFiLlama treasury response.

    Returns list of dicts: {token_name, token_symbol, usd_value, chain, is_stablecoin}
    """
    if not treasury_data:
        return []

    holdings = []
    chain_tvls = treasury_data.get("chainTvls", {})

    for chain_name, chain_data in chain_tvls.items():
        if not isinstance(chain_data, dict):
            continue

        # Each chain has a "tokens" list — each entry has a "tokens" dict
        tokens_list = chain_data.get("tokens", [])
        if not tokens_list:
            continue

        # Take the latest snapshot (last entry)
        latest = tokens_list[-1] if tokens_list else {}
        token_map = latest.get("tokens", {})

        for token_name, usd_value in token_map.items():
            if not isinstance(usd_value, (int, float)) or usd_value <= 0:
                continue

            # Derive symbol from name — DeFiLlama uses names like "USD Coin" or symbols like "USDC"
            # Try to normalize: if name is short and all-caps-ish, treat as symbol
            symbol = token_name.strip()
            name = token_name.strip()
            if len(symbol) <= 10 and symbol.replace("-", "").replace(".", "").isalpha():
                symbol = symbol.upper()
            else:
                symbol = symbol.split("(")[-1].rstrip(")").strip() if "(" in symbol else symbol
                symbol = symbol.upper()

            holdings.append({
                "token_name": name,
                "token_symbol": symbol,
                "usd_value": usd_value,
                "chain": chain_name,
                "is_stablecoin": _is_stablecoin_token(name, symbol),
            })

    return holdings

# Governance token CoinGecko IDs for protocols that have one
PROTOCOL_GOVERNANCE_TOKENS = {
    "aave": "aave",
    "lido": "lido-dao",
    "eigenlayer": "eigenlayer",
    "sky": "maker",  # MKR is still the governance token
    "compound-finance": "compound-governance-token",
    "uniswap": "uniswap",
    "curve-finance": "curve-dao-token",
    "morpho": "morpho",
    "spark": None,  # no separate governance token
    "convex-finance": "convex-finance",
    "drift": "drift-protocol",
    "jupiter-perpetual-exchange": "jupiter-exchange-solana",
    "raydium": "raydium",
}

# Snapshot space IDs for governance proposal queries
SNAPSHOT_SPACES = {
    "aave": "aave.eth",
    "lido": "lido-snapshot.eth",
    "sky": "makerdao.eth",
    "compound-finance": "comp-vote.eth",
    "uniswap": "uniswapgovernance.eth",
    "curve-finance": "curve.eth",
    "convex-finance": "cvx.eth",
    # Solana protocols use Realms (SPL Governance) — no Snapshot spaces
}


def fetch_protocol_data(slug):
    """Fetch protocol data from DeFiLlama."""
    time.sleep(1)  # rate limit
    try:
        resp = requests.get(f"{DEFILLAMA_BASE}/protocol/{slug}", timeout=45)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch {slug}: {e}")
        return None


def fetch_fees_data(slug):
    """Fetch fee/revenue data from DeFiLlama."""
    time.sleep(1)
    try:
        resp = requests.get(f"{DEFILLAMA_BASE}/summary/fees/{slug}", timeout=45)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def fetch_treasury_data(slug):
    """Fetch protocol treasury data from DeFiLlama."""
    time.sleep(1)
    try:
        resp = requests.get(f"{DEFILLAMA_BASE}/treasury/{slug}", timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.debug(f"Treasury fetch failed for {slug}: {e}")
    return None


# Known bad debt events (static config — updated manually)
KNOWN_BAD_DEBT = {
    "aave": 0,
    "lido": 0,
    "eigenlayer": 0,
    "sky": 0,  # historically had some, long resolved
    "compound-finance": 0,
    "uniswap": 0,
    "curve-finance": 0,
    "morpho": 0,
    "spark": 0,
    "convex-finance": 0,
    "drift": 0,
    "jupiter-perpetual-exchange": 0,
    "raydium": 0,
}

# Protocol main contract addresses for admin key analysis
PROTOCOL_CONTRACTS = {
    "aave": "0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",        # AAVE token
    "lido": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",        # stETH
    "eigenlayer": "0x858646372CC42E1A627fcE94aa7A7033e7CF075A",   # Strategy Manager
    "sky": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2",         # MKR token
    "compound-finance": "0xc0Da02939E1441F497fd74F78cE7Decb17B66529", # Governance
    "uniswap": "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",     # UNI token
    "curve-finance": "0xD533a949740bb3306d119CC777fa900bA034cd52",  # CRV token
    "morpho": "0x9994E35Db50125E0DF82e4c2dde62496CE330999",       # Morpho token
    "spark": None,
    "convex-finance": "0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B", # CVX token
    "drift": None,  # Solana program — no EVM contract
    "jupiter-perpetual-exchange": None,  # Solana program
    "raydium": None,  # Solana program
}


def fetch_coingecko_token(gecko_id):
    """Fetch governance token data from CoinGecko for holder count and volume."""
    if not gecko_id:
        return None
    time.sleep(1)
    try:
        from app.config import STABLECOIN_REGISTRY
        # Use the same API key pattern as the SII collectors
        import os
        api_key = os.environ.get("COINGECKO_API_KEY", "")
        headers = {"x-cg-pro-api-key": api_key} if api_key else {}
        base = "https://pro-api.coingecko.com/api/v3" if api_key else "https://api.coingecko.com/api/v3"

        resp = requests.get(
            f"{base}/coins/{gecko_id}",
            params={"localization": "false", "tickers": "false", "market_data": "true",
                    "community_data": "false", "developer_data": "false"},
            headers=headers,
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.debug(f"CoinGecko token fetch failed for {gecko_id}: {e}")
    return None


def fetch_snapshot_proposals(space_id):
    """Fetch governance proposal count from Snapshot in the last 90 days."""
    if not space_id:
        return None
    time.sleep(0.5)
    try:
        query = """
        query {
          proposals(
            first: 100,
            skip: 0,
            where: {space: "%s", created_gte: %d},
            orderBy: "created",
            orderDirection: desc
          ) { id }
        }
        """ % (space_id, int((datetime.now(timezone.utc).timestamp()) - 90 * 86400))

        resp = requests.post(
            "https://hub.snapshot.org/graphql",
            json={"query": query},
            timeout=10,
        )
        if resp.status_code == 200:
            data = resp.json()
            proposals = data.get("data", {}).get("proposals", [])
            return len(proposals)
    except Exception as e:
        logger.debug(f"Snapshot fetch failed for {space_id}: {e}")
    return None


def extract_raw_values(protocol_data, fees_data, treasury_data=None):
    """Extract raw component values from DeFiLlama data."""
    raw = {}

    if not protocol_data:
        return raw

    # TVL — /protocol/ returns tvl as a historical list; extract latest value
    tvl_raw = protocol_data.get("tvl")
    if isinstance(tvl_raw, list) and tvl_raw:
        last_entry = tvl_raw[-1]
        raw["tvl"] = last_entry.get("totalLiquidityUSD", 0) if isinstance(last_entry, dict) else 0
    elif isinstance(tvl_raw, (int, float)):
        raw["tvl"] = tvl_raw

    # TVL changes
    change_1w = protocol_data.get("change_7d")
    change_1m = protocol_data.get("change_1m")
    if change_1w is not None:
        raw["tvl_7d_change"] = change_1w
    if change_1m is not None:
        raw["tvl_30d_change"] = change_1m

    # Chain count and concentration — use currentChainTvls (snapshot) over chainTvls (historical)
    current_chain_tvls = protocol_data.get("currentChainTvls", {})
    if current_chain_tvls:
        # Filter out derivative entries like "Ethereum-borrowed", "Ethereum-staking", "Ethereum-pool2"
        chains_with_tvl = {
            k: v for k, v in current_chain_tvls.items()
            if "-" not in k and isinstance(v, (int, float)) and v > 0
        }

        raw["chain_count"] = len(chains_with_tvl)
        if chains_with_tvl:
            total_chain = sum(chains_with_tvl.values())
            if total_chain > 0:
                max_chain = max(chains_with_tvl.values())
                raw["tvl_concentration"] = (max_chain / total_chain) * 100

    # Audit info
    audits = protocol_data.get("audits")
    audit_links = protocol_data.get("audit_links", [])
    if audits is not None:
        raw["audit_count"] = int(audits) if audits else len(audit_links)
    elif audit_links:
        raw["audit_count"] = len(audit_links)

    # Audit recency — estimate from audit_links or audit_note
    # DeFiLlama doesn't always include timestamps, so use protocol launch date as fallback
    if audit_links:
        # Many audit links contain dates in the URL or name
        # Conservative estimate: if audits exist, assume most recent was within 365 days
        # unless the protocol is very old
        raw["audit_recency_days"] = 365  # conservative default if audits exist
    else:
        raw["audit_recency_days"] = 730  # no audits known

    # Liquidity & Utilization — use TVL as liquidity proxy
    # For lending protocols, compute utilization from borrowed/supplied
    tvl_val = raw.get("tvl", 0)
    if tvl_val and tvl_val > 0:
        raw["protocol_dex_tvl"] = tvl_val  # TVL IS the liquidity for protocols

        # Pool depth: number of active pools approximated from chain count × 2
        chain_ct = raw.get("chain_count", 1)
        raw["pool_depth"] = chain_ct * 3  # rough approximation: ~3 pools per chain

    # Utilization rate from borrowed TVL if available
    borrowed_tvl = 0
    staking_tvl = 0
    for k, v in current_chain_tvls.items() if current_chain_tvls else []:
        if "-borrowed" in k and isinstance(v, (int, float)):
            borrowed_tvl += v
        if "-staking" in k and isinstance(v, (int, float)):
            staking_tvl += v

    if borrowed_tvl > 0 and tvl_val > 0:
        raw["utilization_rate"] = (borrowed_tvl / (tvl_val + borrowed_tvl)) * 100
    elif staking_tvl > 0 and tvl_val > 0:
        # For staking protocols: staking ratio as utilization proxy
        raw["utilization_rate"] = (staking_tvl / (tvl_val + staking_tvl)) * 100

    # Token data (if available)
    mcap = protocol_data.get("mcap")
    if mcap:
        raw["token_mcap"] = mcap
        if raw.get("tvl") and raw["tvl"] > 0:
            raw["mcap_tvl_ratio"] = mcap / raw["tvl"]

    # Fees and revenue
    if fees_data:
        total_30d = fees_data.get("total30d")
        if total_30d:
            raw["fees_30d"] = total_30d
        revenue_30d = fees_data.get("totalRevenue30d")
        if revenue_30d:
            raw["revenue_30d"] = revenue_30d
        elif total_30d:
            raw["revenue_30d"] = total_30d * 0.3  # estimate if not available

        if raw.get("fees_30d") and raw.get("tvl") and raw["tvl"] > 0:
            raw["fees_tvl_ratio"] = (raw["fees_30d"] * 12) / raw["tvl"]  # annualized

        # Revenue efficiency — revenue / TVL annualized
        if raw.get("revenue_30d") and raw.get("tvl") and raw["tvl"] > 0:
            raw["fees_tvl_efficiency"] = (raw["revenue_30d"] * 12) / raw["tvl"]

    # Treasury data — parse per-token breakdown
    if treasury_data:
        chain_tvls = treasury_data.get("chainTvls", {})
        treasury_total = 0

        for chain_name, chain_data in chain_tvls.items():
            if isinstance(chain_data, dict):
                tvl_list = chain_data.get("tvl", [])
                if tvl_list:
                    last = tvl_list[-1]
                    if isinstance(last, dict):
                        treasury_total += last.get("totalLiquidityUSD", 0)

        if treasury_total > 0:
            raw["treasury_total_usd"] = treasury_total

        # Parse individual token holdings
        token_holdings = parse_treasury_tokens(treasury_data)
        if token_holdings:
            stablecoin_usd = sum(h["usd_value"] for h in token_holdings if h["is_stablecoin"])
            total_token_usd = sum(h["usd_value"] for h in token_holdings)
            if total_token_usd > 0:
                raw["treasury_stablecoin_pct"] = (stablecoin_usd / total_token_usd) * 100
            else:
                raw["treasury_stablecoin_pct"] = 0.0
            raw["_token_holdings"] = token_holdings  # carry forward for storage
        elif treasury_total > 0:
            raw["treasury_stablecoin_pct"] = 0.0  # no token data available

    return raw


def score_protocol(slug):
    """Fetch data and score a single protocol."""
    protocol_data = fetch_protocol_data(slug)
    fees_data = fetch_fees_data(slug)

    if not protocol_data:
        return None

    treasury_data = fetch_treasury_data(slug)
    raw_values = extract_raw_values(protocol_data, fees_data, treasury_data)

    # Bad debt (static config)
    bad_debt = KNOWN_BAD_DEBT.get(slug, 0)
    tvl = raw_values.get("tvl", 0)
    if tvl > 0:
        raw_values["bad_debt_ratio"] = (bad_debt / tvl) * 100  # as percentage of TVL
    else:
        raw_values["bad_debt_ratio"] = 0

    # Governance token data from CoinGecko
    # Fall back to backlog for promoted protocols not in hardcoded dicts
    gecko_id = PROTOCOL_GOVERNANCE_TOKENS.get(slug) or _get_backlog_field(slug, "gecko_id")
    token_data = None
    if gecko_id:
        token_data = fetch_coingecko_token(gecko_id)
        if token_data:
            market = token_data.get("market_data", {})
            # token_volume_24h
            vol = market.get("total_volume", {}).get("usd")
            if vol:
                raw_values["token_volume_24h"] = vol
            # token_liquidity_depth — volume/mcap ratio
            mcap = market.get("market_cap", {}).get("usd")
            if vol and mcap and mcap > 0:
                raw_values["token_liquidity_depth"] = vol / mcap
            # token_price_volatility_30d — use 30d price change as proxy
            pct_30d = market.get("price_change_percentage_30d")
            if pct_30d is not None:
                raw_values["token_price_volatility_30d"] = abs(pct_30d)
            # governance_token_holders
            holders = token_data.get("community_data", {}).get("token_holders")
            if holders and holders > 0:
                raw_values["governance_token_holders"] = holders

    # Governance proposals from Snapshot
    space_id = SNAPSHOT_SPACES.get(slug) or _get_backlog_field(slug, "snapshot_space")
    if space_id:
        proposal_count = fetch_snapshot_proposals(space_id)
        if proposal_count is not None:
            raw_values["governance_proposals_90d"] = proposal_count

    # Protocol admin key risk — reuse SII smart contract analyzer config
    from app.collectors.smart_contract import ADMIN_KEY_RISK
    contract = PROTOCOL_CONTRACTS.get(slug) or _get_backlog_field(slug, "main_contract")
    if contract:
        # Use SII admin key scores if protocol has a matching stablecoin entry
        # Otherwise use a reasonable default based on protocol type
        admin_score = ADMIN_KEY_RISK.get(slug)
        if admin_score is None:
            # Map protocols to admin risk based on known governance structure
            admin_score = _PROTOCOL_ADMIN_SCORES.get(slug, 50)
        raw_values["protocol_admin_key_risk"] = admin_score
    elif slug in _PROTOCOL_ADMIN_SCORES:
        # Non-EVM protocols (e.g. Solana) — use static score when no contract to analyze
        raw_values["protocol_admin_key_risk"] = _PROTOCOL_ADMIN_SCORES[slug]

    result = score_entity(PSI_V01_DEFINITION, raw_values)
    result["protocol_slug"] = slug
    result["protocol_name"] = protocol_data.get("name", slug)
    result["raw_values"] = raw_values

    return result


# Protocol admin risk scores (separate from stablecoin scores)
_PROTOCOL_ADMIN_SCORES = {
    "aave": 90,        # Aave Gov V3 — on-chain governance
    "lido": 85,        # LidoDAO — on-chain governance + multisig
    "eigenlayer": 60,   # Early stage, team-controlled
    "sky": 90,         # MakerDAO — on-chain governance (DSChief)
    "compound-finance": 85,  # Compound Governor Bravo
    "uniswap": 85,     # Uniswap Governance — on-chain
    "curve-finance": 85, # veCRV governance
    "morpho": 65,      # Newer protocol, multisig governance
    "spark": 70,       # Sub-DAO of MakerDAO
    "convex-finance": 75, # Multisig + veCVX governance
    "drift": 50,          # Solana program — upgrade authority unknown, score as neutral
    "jupiter-perpetual-exchange": 55,  # Solana program — JUP DAO governance active
    "raydium": 50,        # Solana program — upgrade authority unknown
}


def _get_backlog_field(slug, field):
    """Look up a protocol backlog field for fallback config resolution."""
    allowed = {"gecko_id", "snapshot_space", "main_contract"}
    if field not in allowed:
        return None
    try:
        row = fetch_one(
            f"SELECT {field} FROM protocol_backlog WHERE slug = %s",
            (slug,),
        )
        return row[field] if row else None
    except Exception:
        return None


def get_scoring_protocols():
    """Get protocols to score: hardcoded TARGET_PROTOCOLS + promoted from backlog."""
    protocols = list(TARGET_PROTOCOLS)
    try:
        promoted = fetch_all("""
            SELECT slug FROM protocol_backlog
            WHERE enrichment_status IN ('promoted', 'scored')
        """)
        for row in promoted:
            if row["slug"] not in protocols:
                protocols.append(row["slug"])
    except Exception as e:
        logger.debug(f"Could not fetch promoted protocols from backlog: {e}")
    return protocols


def _get_sii_score_map():
    """Build a map of stablecoin symbol -> latest SII score."""
    try:
        rows = fetch_all("""
            SELECT st.symbol, s.overall_score
            FROM scores s
            JOIN stablecoins st ON st.id = s.stablecoin_id
        """)
        return {row["symbol"].upper(): float(row["overall_score"]) for row in rows if row.get("overall_score")}
    except Exception as e:
        logger.debug(f"Could not fetch SII scores: {e}")
        return {}


def store_treasury_holdings(slug, token_holdings):
    """Store per-token treasury holdings with SII cross-reference."""
    if not token_holdings:
        return

    sii_map = _get_sii_score_map()

    for h in token_holdings:
        sym = h["token_symbol"]
        sii_score = sii_map.get(sym)

        try:
            execute("""
                INSERT INTO protocol_treasury_holdings
                    (protocol_slug, token_name, token_symbol, chain, usd_value,
                     is_stablecoin, sii_score, snapshot_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_DATE)
                ON CONFLICT (protocol_slug, token_symbol, chain, snapshot_date)
                DO UPDATE SET
                    token_name = EXCLUDED.token_name,
                    usd_value = EXCLUDED.usd_value,
                    is_stablecoin = EXCLUDED.is_stablecoin,
                    sii_score = EXCLUDED.sii_score
            """, (
                slug,
                h["token_name"],
                sym,
                h["chain"],
                h["usd_value"],
                h["is_stablecoin"],
                sii_score,
            ))
        except Exception as e:
            logger.debug(f"Failed to store holding {sym} for {slug}: {e}")


# =========================================================================
# Collateral / Pool Exposure — what stablecoins protocols *accept*
# =========================================================================

DEFILLAMA_PROJECT_MAP = {
    # Aave
    "aave-v3": "aave",
    "aave-v2": "aave",
    "aave-v1": "aave",
    # Lido
    "lido": "lido",
    # EigenLayer
    "eigenlayer": "eigenlayer",
    # Sky (MakerDAO)
    "makerdao": "sky",
    "maker": "sky",
    "sky": "sky",
    "maker-rwa": "sky",
    # Spark
    "spark": "spark",
    "spark-lending": "spark",
    # Compound
    "compound-v3": "compound-finance",
    "compound-v2": "compound-finance",
    "compound": "compound-finance",
    # Uniswap
    "uniswap-v3": "uniswap",
    "uniswap-v2": "uniswap",
    "uniswap-v4": "uniswap",
    # Curve
    "curve-dex": "curve-finance",
    "curve": "curve-finance",
    "curve-lending": "curve-finance",
    # Morpho
    "morpho": "morpho",
    "morpho-blue": "morpho",
    "morpho-aave": "morpho",
    "morpho-compound": "morpho",
    # Convex
    "convex-finance": "convex-finance",
    "convex": "convex-finance",
    # Drift
    "drift": "drift",
    "drift-trade": "drift",
    "drift-protocol": "drift",
    "drift v2": "drift",
    # Jupiter
    "jupiter-perpetual-exchange": "jupiter-perpetual-exchange",
    "jupiter-perps": "jupiter-perpetual-exchange",
    "jupiter": "jupiter-perpetual-exchange",
    # Raydium
    "raydium": "raydium",
    "raydium-amm": "raydium",
    "raydium-clmm": "raydium",
    "raydium-cpmm": "raydium",
}

# Protocol type hints for pool_type classification
_LENDING_PROTOCOLS = {"aave", "compound-finance", "morpho", "spark", "sky"}
_DEX_PROTOCOLS = {"uniswap", "curve-finance", "drift", "jupiter-perpetual-exchange", "raydium"}
_STAKING_PROTOCOLS = {"lido", "eigenlayer"}
_YIELD_PROTOCOLS = {"convex-finance"}

# SII-scored stablecoin symbols
SII_SCORED_SYMBOLS = {"USDC", "USDT", "DAI", "FRAX", "PYUSD", "FDUSD", "TUSD", "USDD", "USDE", "USD1"}


def fetch_protocol_pools():
    """Fetch all pools from DeFiLlama yields API to map protocol stablecoin exposure."""
    try:
        resp = requests.get("https://yields.llama.fi/pools", timeout=60)
        if resp.status_code == 200:
            return resp.json().get("data", [])
    except Exception as e:
        logger.error(f"Failed to fetch pools: {e}")
    return []


def _classify_pool_type(slug):
    """Return pool_type string based on protocol category."""
    if slug in _LENDING_PROTOCOLS:
        return "lending"
    if slug in _DEX_PROTOCOLS:
        return "dex"
    if slug in _STAKING_PROTOCOLS:
        return "staking"
    if slug in _YIELD_PROTOCOLS:
        return "yield"
    return "other"


def _extract_stablecoin_symbols(pool_symbol):
    """Extract individual token symbols from a pool symbol like 'USDC-USDT' or 'WETH-USDC'."""
    if not pool_symbol:
        return []
    # Split on common delimiters
    parts = pool_symbol.replace("/", "-").replace("+", "-").replace("_", "-").split("-")
    return [p.strip().upper() for p in parts if p.strip()]


def collect_collateral_exposure():
    """Fetch DeFiLlama pool data and store stablecoin collateral exposure for all target protocols."""
    logger.info("Collecting collateral exposure from DeFiLlama pools...")
    pools = fetch_protocol_pools()
    if not pools:
        logger.warning("No pool data returned from DeFiLlama")
        return []

    sii_map = _get_sii_score_map()
    target_slugs = set(TARGET_PROTOCOLS)

    # Build reverse map of all DeFiLlama project names we care about
    matched_pools = []
    seen_projects = set()

    for pool in pools:
        project = pool.get("project", "")
        if not project:
            continue
        seen_projects.add(project)

        slug = DEFILLAMA_PROJECT_MAP.get(project)
        if not slug or slug not in target_slugs:
            continue

        tvl = pool.get("tvlUsd") or 0
        if tvl <= 0:
            continue

        pool_symbol = pool.get("symbol", "")
        chain = pool.get("chain", "")
        pool_id = pool.get("pool", "")
        is_stable_pool = pool.get("stablecoin", False)

        # Extract individual symbols from pool name
        symbols = _extract_stablecoin_symbols(pool_symbol)

        for sym in symbols:
            is_stable = _is_stablecoin_token(sym, sym) or is_stable_pool
            if not is_stable:
                continue

            # For multi-asset pools, split TVL evenly among stablecoin components
            stable_syms_in_pool = [s for s in symbols if _is_stablecoin_token(s, s)]
            tvl_share = tvl / len(stable_syms_in_pool) if stable_syms_in_pool else tvl

            is_sii = sym in SII_SCORED_SYMBOLS
            sii_score = sii_map.get(sym)

            matched_pools.append({
                "protocol_slug": slug,
                "pool_id": pool_id,
                "token_symbol": sym,
                "chain": chain,
                "tvl_usd": tvl_share,
                "is_stablecoin": True,
                "is_sii_scored": is_sii,
                "sii_score": sii_score,
                "pool_type": _classify_pool_type(slug),
            })

    # Log unmatched project names that look like they might be related
    protocol_keywords = {"aave", "lido", "eigen", "maker", "sky", "spark",
                         "compound", "uniswap", "curve", "morpho", "convex",
                         "drift", "jupiter", "raydium"}
    for proj in sorted(seen_projects):
        proj_lower = proj.lower()
        if any(kw in proj_lower for kw in protocol_keywords) and proj not in DEFILLAMA_PROJECT_MAP:
            logger.info(f"  Unmatched DeFiLlama project (potential alias): {proj}")

    # Aggregate by protocol + token_symbol (sum TVL across chains/versions)
    agg = {}
    for p in matched_pools:
        key = (p["protocol_slug"], p["token_symbol"])
        if key not in agg:
            agg[key] = {
                "protocol_slug": p["protocol_slug"],
                "pool_id": f"{p['protocol_slug']}:{p['token_symbol']}:agg",
                "token_symbol": p["token_symbol"],
                "chain": "all",
                "tvl_usd": 0.0,
                "is_stablecoin": True,
                "is_sii_scored": p["is_sii_scored"],
                "sii_score": p["sii_score"],
                "pool_type": p["pool_type"],
            }
        agg[key]["tvl_usd"] += p["tvl_usd"]

    # Store aggregated rows
    stored = 0
    for row in agg.values():
        try:
            execute("""
                INSERT INTO protocol_collateral_exposure
                    (protocol_slug, pool_id, token_symbol, chain, tvl_usd,
                     is_stablecoin, is_sii_scored, sii_score, pool_type, snapshot_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE)
                ON CONFLICT (protocol_slug, pool_id, snapshot_date)
                DO UPDATE SET
                    token_symbol = EXCLUDED.token_symbol,
                    tvl_usd = EXCLUDED.tvl_usd,
                    is_stablecoin = EXCLUDED.is_stablecoin,
                    is_sii_scored = EXCLUDED.is_sii_scored,
                    sii_score = EXCLUDED.sii_score,
                    pool_type = EXCLUDED.pool_type
            """, (
                row["protocol_slug"],
                row["pool_id"],
                row["token_symbol"],
                row["chain"],
                row["tvl_usd"],
                row["is_stablecoin"],
                row["is_sii_scored"],
                row["sii_score"],
                row["pool_type"],
            ))
            stored += 1
        except Exception as e:
            logger.debug(f"Failed to store collateral {row['token_symbol']} for {row['protocol_slug']}: {e}")

    logger.info(f"Collateral exposure: stored {stored} aggregated rows from {len(matched_pools)} pool matches")
    return list(agg.values())


def sync_collateral_to_backlog():
    """Feed unscored stablecoins from collateral exposure into the auto-promote backlog.

    Queries today's protocol_collateral_exposure for unscored stablecoins,
    groups by symbol, and upserts into wallet_graph.unscored_assets with
    collateral TVL as a demand signal.

    Returns number of assets synced.
    """
    from app.indexer.backlog import upsert_unscored_asset
    from app.indexer.config import UNSCORED_CONTRACTS

    # Build reverse lookup: symbol -> (address, info) from UNSCORED_CONTRACTS
    symbol_to_address = {}
    for addr, info in UNSCORED_CONTRACTS.items():
        symbol_to_address[info["symbol"].upper()] = (addr, info)

    # Query today's unscored stablecoin collateral exposure, grouped by symbol
    rows = fetch_all("""
        SELECT token_symbol,
               SUM(tvl_usd) AS total_collateral_tvl,
               COUNT(DISTINCT protocol_slug) AS protocol_count
        FROM protocol_collateral_exposure
        WHERE is_stablecoin = TRUE
          AND is_sii_scored = FALSE
          AND snapshot_date = CURRENT_DATE
        GROUP BY token_symbol
    """)

    if not rows:
        logger.info("No unscored stablecoins in collateral exposure today")
        return 0

    synced = 0
    for row in rows:
        sym = row["token_symbol"].upper()
        total_tvl = float(row["total_collateral_tvl"])
        num_protocols = int(row["protocol_count"])

        # Look up contract address from known unscored stablecoins
        if sym not in symbol_to_address:
            logger.info(f"Collateral stablecoin needs manual mapping: {sym} (${total_tvl:,.0f} TVL)")
            continue

        addr, info = symbol_to_address[sym]

        # Ensure asset exists in backlog
        upsert_unscored_asset(
            token_address=addr,
            symbol=info["symbol"],
            name=info["name"],
            decimals=info["decimals"],
            coingecko_id=info.get("coingecko_id"),
            token_type="stablecoin",
        )

        # Update collateral demand signals
        execute("""
            UPDATE wallet_graph.unscored_assets SET
                protocol_collateral_tvl = %s,
                protocol_count = %s,
                updated_at = NOW()
            WHERE token_address = %s
        """, (total_tvl, num_protocols, addr.lower()))

        synced += 1
        logger.info(
            f"Synced collateral signal: {sym} — ${total_tvl:,.0f} TVL across {num_protocols} protocol(s)"
        )

    return synced


# =========================================================================
# Protocol Backlog — discovery, enrichment, and promotion
# =========================================================================

_DISCOVERY_TVL_THRESHOLD = 10_000_000  # $10M stablecoin exposure minimum
_ENRICHMENT_DAILY_LIMIT = 5


def discover_protocols():
    """Discover new protocols from DeFiLlama pool data that have meaningful stablecoin exposure.

    Scans all pools, finds protocols NOT in TARGET_PROTOCOLS with >$10M stablecoin TVL,
    and upserts them into protocol_backlog with demand signals.

    Returns number of protocols discovered.
    """
    pools = fetch_protocol_pools()
    if not pools:
        return 0

    sii_map = _get_sii_score_map()
    known_slugs = set(TARGET_PROTOCOLS)

    # Also exclude protocols already in backlog as promoted/scored
    try:
        already_promoted = fetch_all("""
            SELECT slug FROM protocol_backlog
            WHERE enrichment_status IN ('promoted', 'scored')
        """)
        for row in already_promoted:
            known_slugs.add(row["slug"])
    except Exception:
        pass

    # Aggregate stablecoin TVL per unknown project
    project_exposure = {}  # project_name -> {total_stable_tvl, unscored_tvl, unscored_symbols}

    for pool in pools:
        project = pool.get("project", "")
        if not project:
            continue

        # Skip projects that map to known protocols
        mapped = DEFILLAMA_PROJECT_MAP.get(project)
        if mapped and mapped in known_slugs:
            continue
        # Also skip if the project itself is a known slug
        if project in known_slugs:
            continue

        tvl = pool.get("tvlUsd") or 0
        if tvl <= 0:
            continue

        pool_symbol = pool.get("symbol", "")
        symbols = _extract_stablecoin_symbols(pool_symbol)
        is_stable_pool = pool.get("stablecoin", False)

        for sym in symbols:
            is_stable = _is_stablecoin_token(sym, sym) or is_stable_pool
            if not is_stable:
                continue

            stable_syms = [s for s in symbols if _is_stablecoin_token(s, s)]
            tvl_share = tvl / len(stable_syms) if stable_syms else tvl

            if project not in project_exposure:
                project_exposure[project] = {
                    "total_stable_tvl": 0.0,
                    "unscored_tvl": 0.0,
                    "unscored_symbols": set(),
                }

            project_exposure[project]["total_stable_tvl"] += tvl_share

            is_sii = sym in SII_SCORED_SYMBOLS
            if not is_sii:
                project_exposure[project]["unscored_tvl"] += tvl_share
                project_exposure[project]["unscored_symbols"].add(sym)

    # Filter to projects above threshold and upsert into backlog
    discovered = 0
    for project, exp in sorted(
        project_exposure.items(),
        key=lambda x: x[1]["total_stable_tvl"],
        reverse=True,
    ):
        if exp["total_stable_tvl"] < _DISCOVERY_TVL_THRESHOLD:
            continue

        # Use project name as slug (DeFiLlama convention)
        slug = project
        unscored_list = sorted(exp["unscored_symbols"])

        try:
            execute("""
                INSERT INTO protocol_backlog
                    (slug, stablecoin_exposure_usd, unscored_stablecoin_exposure_usd,
                     unscored_stablecoins, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (slug) DO UPDATE SET
                    stablecoin_exposure_usd = EXCLUDED.stablecoin_exposure_usd,
                    unscored_stablecoin_exposure_usd = EXCLUDED.unscored_stablecoin_exposure_usd,
                    unscored_stablecoins = EXCLUDED.unscored_stablecoins,
                    updated_at = NOW()
            """, (
                slug,
                exp["total_stable_tvl"],
                exp["unscored_tvl"],
                unscored_list,
            ))
            discovered += 1
        except Exception as e:
            logger.debug(f"Failed to upsert protocol backlog for {slug}: {e}")

    if discovered:
        logger.info(f"Protocol discovery: {discovered} protocols with >${_DISCOVERY_TVL_THRESHOLD / 1e6:.0f}M stablecoin exposure")

    return discovered


def enrich_protocol_backlog():
    """Enrich discovered protocols with DeFiLlama data and dry-run PSI scoring.

    Fetches protocol data, extracts metadata (gecko_id, snapshot space, contract),
    runs a dry-run score to count available components, and updates enrichment status.

    Rate limited to 5 protocols per day. Prioritized by stablecoin_exposure_usd.

    Returns number of protocols enriched.
    """
    import os
    coverage_threshold = float(os.environ.get("PROTOCOL_PROMOTE_COVERAGE_PCT", "60"))

    candidates = fetch_all("""
        SELECT slug FROM protocol_backlog
        WHERE enrichment_status IN ('discovered', 'enriching')
        ORDER BY stablecoin_exposure_usd DESC
        LIMIT %s
    """, (_ENRICHMENT_DAILY_LIMIT,))

    if not candidates:
        return 0

    enriched = 0
    for row in candidates:
        slug = row["slug"]

        protocol_data = fetch_protocol_data(slug)
        if not protocol_data:
            logger.debug(f"Enrichment: no DeFiLlama data for {slug}")
            continue

        fees_data = fetch_fees_data(slug)
        treasury_data = fetch_treasury_data(slug)

        # Extract metadata from DeFiLlama response
        name = protocol_data.get("name", slug)
        category = protocol_data.get("category", "")
        gecko_id = protocol_data.get("gecko_id") or None
        governance_id = protocol_data.get("governanceID") or None

        # Snapshot space from governanceID (DeFiLlama returns list or string)
        snapshot_space = None
        if governance_id:
            if isinstance(governance_id, list) and governance_id:
                # Take first Snapshot entry
                for gid in governance_id:
                    if isinstance(gid, str) and ".eth" in gid:
                        snapshot_space = gid
                        break
                if not snapshot_space:
                    snapshot_space = str(governance_id[0])
            elif isinstance(governance_id, str):
                snapshot_space = governance_id

        # Primary contract address
        main_contract = protocol_data.get("address") or None

        # Extract TVL
        raw_values = extract_raw_values(protocol_data, fees_data, treasury_data)
        tvl = raw_values.get("tvl", 0)

        # Dry-run: count how many PSI components have values
        # Add governance/token fields the same way score_protocol does
        bad_debt = KNOWN_BAD_DEBT.get(slug, 0)
        if tvl > 0:
            raw_values["bad_debt_ratio"] = (bad_debt / tvl) * 100
        else:
            raw_values["bad_debt_ratio"] = 0

        if gecko_id:
            token_data = fetch_coingecko_token(gecko_id)
            if token_data:
                market = token_data.get("market_data", {})
                vol = market.get("total_volume", {}).get("usd")
                if vol:
                    raw_values["token_volume_24h"] = vol
                mcap = market.get("market_cap", {}).get("usd")
                if vol and mcap and mcap > 0:
                    raw_values["token_liquidity_depth"] = vol / mcap
                pct_30d = market.get("price_change_percentage_30d")
                if pct_30d is not None:
                    raw_values["token_price_volatility_30d"] = abs(pct_30d)
                holders = token_data.get("community_data", {}).get("token_holders")
                if holders and holders > 0:
                    raw_values["governance_token_holders"] = holders

        if snapshot_space:
            proposal_count = fetch_snapshot_proposals(snapshot_space)
            if proposal_count is not None:
                raw_values["governance_proposals_90d"] = proposal_count

        raw_values["protocol_admin_key_risk"] = _PROTOCOL_ADMIN_SCORES.get(slug, 50)

        # Count available components
        components_total = len(PSI_V01_DEFINITION["components"])
        components_available = sum(
            1 for comp_id in PSI_V01_DEFINITION["components"]
            if comp_id in raw_values and raw_values[comp_id] is not None
        )
        coverage_pct = (components_available / components_total * 100) if components_total else 0

        # Determine enrichment status
        if coverage_pct >= coverage_threshold:
            new_status = "ready"
        else:
            new_status = "enriching"

        try:
            execute("""
                UPDATE protocol_backlog SET
                    name = %s,
                    category = %s,
                    tvl_usd = %s,
                    gecko_id = %s,
                    snapshot_space = %s,
                    main_contract = %s,
                    components_available = %s,
                    components_total = %s,
                    coverage_pct = %s,
                    enrichment_status = %s,
                    last_enrichment_at = NOW(),
                    updated_at = NOW()
                WHERE slug = %s
            """, (
                name, category, tvl, gecko_id, snapshot_space, main_contract,
                components_available, components_total, round(coverage_pct, 1),
                new_status, slug,
            ))
            enriched += 1
            logger.info(
                f"Enriched {name} ({slug}): {components_available}/{components_total} components "
                f"({coverage_pct:.0f}%) → {new_status}"
            )
        except Exception as e:
            logger.warning(f"Failed to update backlog for {slug}: {e}")

    return enriched


def promote_eligible_protocols():
    """Promote protocols with sufficient component coverage to PSI scoring.

    Protocols with enrichment_status='ready' get promoted. On the next
    run_psi_scoring() call, get_scoring_protocols() will include them.

    Returns number of protocols promoted.
    """
    import os
    coverage_threshold = float(os.environ.get("PROTOCOL_PROMOTE_COVERAGE_PCT", "60"))

    eligible = fetch_all("""
        SELECT slug, name, stablecoin_exposure_usd, coverage_pct,
               components_available, components_total
        FROM protocol_backlog
        WHERE enrichment_status = 'ready'
          AND coverage_pct >= %s
        ORDER BY stablecoin_exposure_usd DESC
    """, (coverage_threshold,))

    if not eligible:
        return 0

    promoted = 0
    for row in eligible:
        try:
            execute("""
                UPDATE protocol_backlog SET
                    enrichment_status = 'promoted',
                    updated_at = NOW()
                WHERE slug = %s
            """, (row["slug"],))
            promoted += 1
            logger.info(
                f"AUTO-PROMOTE protocol: {row['name']} ({row['slug']}) — "
                f"${row['stablecoin_exposure_usd']:,.0f} stablecoin exposure, "
                f"{row['coverage_pct']:.0f}% component coverage "
                f"({row['components_available']}/{row['components_total']})"
            )
        except Exception as e:
            logger.warning(f"Failed to promote protocol {row['slug']}: {e}")

    if promoted:
        logger.info(f"Promoted {promoted} protocol(s) to PSI scoring queue")

    return promoted


def run_psi_scoring():
    """Score all target protocols and promoted backlog protocols."""
    results = []
    scoring_protocols = get_scoring_protocols()
    for slug in scoring_protocols:
        logger.info(f"Scoring protocol: {slug}")
        result = score_protocol(slug)
        if result:
            # Strip internal _token_holdings before storing (saved separately)
            raw_for_storage = {k: v for k, v in result["raw_values"].items() if not k.startswith("_")}
            execute("""
                INSERT INTO psi_scores (protocol_slug, protocol_name, overall_score, grade,
                    category_scores, component_scores, raw_values, formula_version)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT psi_scores_protocol_slug_scored_date_key
                DO UPDATE SET
                    protocol_name = EXCLUDED.protocol_name,
                    overall_score = EXCLUDED.overall_score,
                    grade = EXCLUDED.grade,
                    category_scores = EXCLUDED.category_scores,
                    component_scores = EXCLUDED.component_scores,
                    raw_values = EXCLUDED.raw_values,
                    computed_at = NOW()
            """, (
                result["protocol_slug"],
                result["protocol_name"],
                result["overall_score"],
                result["grade"],
                json.dumps(result["category_scores"]),
                json.dumps(result["component_scores"]),
                json.dumps(raw_for_storage, default=str),
                result["version"],
            ))
            # Store per-token treasury holdings with SII cross-reference
            token_holdings = result.get("raw_values", {}).get("_token_holdings")
            if token_holdings:
                store_treasury_holdings(slug, token_holdings)
                logger.info(f"  Stored {len(token_holdings)} treasury holdings for {slug}")

            results.append(result)
            logger.info(
                f"  {result['protocol_name']}: {result['overall_score']} ({result['grade']}) "
                f"- {result['components_available']}/{result['components_total']} components"
            )

            # Mark promoted backlog protocols as scored after first successful score
            if slug not in TARGET_PROTOCOLS:
                try:
                    execute("""
                        UPDATE protocol_backlog SET
                            enrichment_status = 'scored',
                            updated_at = NOW()
                        WHERE slug = %s AND enrichment_status = 'promoted'
                    """, (slug,))
                except Exception:
                    pass

    return results
