"""
PSI Data Collector
===================
Fetches protocol data from DeFiLlama's free API and scores protocols
using the generic scoring engine with the PSI v0.1 definition.
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.psi_v01 import PSI_V01_DEFINITION, TARGET_PROTOCOLS
from app.scoring_engine import score_entity

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"

# =============================================================================
# Solana program upgrade authority check via Helius RPC
# =============================================================================

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
_HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}" if HELIUS_API_KEY else ""

# Solana program IDs for PSI-scored protocols
SOLANA_PROGRAM_IDS = {
    "drift": "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH",
    "jupiter-perpetual-exchange": "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4",
    "raydium": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
}

# Cache: {program_id: (timestamp, admin_risk_score)}
_solana_authority_cache: dict[str, tuple[float, int]] = {}
_AUTHORITY_CACHE_TTL = 86400  # 24 hours


def get_solana_program_authority(program_id: str) -> dict | None:
    """Check a Solana program's upgrade authority via Helius getAccountInfo.

    Returns dict with 'upgradeable' (bool) and 'authority' (str or None),
    or None if the call fails.

    Solana BPF Upgradeable Loader programs have a programData account
    that contains the upgrade authority. If authority is null, the program
    is immutable.
    """
    if not _HELIUS_RPC_URL:
        return None

    # Check cache
    cached = _solana_authority_cache.get(program_id)
    if cached and (time.time() - cached[0]) < _AUTHORITY_CACHE_TTL:
        return cached[1]

    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAccountInfo",
            "params": [program_id, {"encoding": "jsonParsed"}],
        }
        resp = requests.post(_HELIUS_RPC_URL, json=payload, timeout=15)
        data = resp.json()
        account = data.get("result", {}).get("value")
        if not account:
            return None

        parsed = account.get("data", {})
        if isinstance(parsed, dict):
            parsed_info = parsed.get("parsed", {})
            info = parsed_info.get("info", {})

            # BPF Upgradeable Loader: the program account points to programData
            program_data_addr = info.get("programData")
            if program_data_addr:
                # Fetch the programData account to get the authority
                pd_payload = {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "getAccountInfo",
                    "params": [program_data_addr, {"encoding": "jsonParsed"}],
                }
                pd_resp = requests.post(_HELIUS_RPC_URL, json=pd_payload, timeout=15)
                pd_data = pd_resp.json()
                pd_account = pd_data.get("result", {}).get("value")
                if pd_account:
                    pd_parsed = pd_account.get("data", {})
                    if isinstance(pd_parsed, dict):
                        pd_info = pd_parsed.get("parsed", {}).get("info", {})
                        authority = pd_info.get("authority")
                        result = {
                            "upgradeable": authority is not None,
                            "authority": authority,
                        }
                        _solana_authority_cache[program_id] = (time.time(), result)
                        return result

            # If no programData pointer, may be a native or non-upgradeable program
            result = {"upgradeable": False, "authority": None}
            _solana_authority_cache[program_id] = (time.time(), result)
            return result

    except Exception as e:
        logger.warning(f"Helius getAccountInfo failed for {program_id}: {e}")
        return None


def get_solana_admin_risk_score(slug: str) -> int | None:
    """Get admin risk score for a Solana protocol based on on-chain upgrade authority.

    Returns an integer score (0-100) or None if lookup fails.
    - Immutable (no upgrade authority): 90 (very safe)
    - Upgradeable with known multisig/DAO: 55-65
    - Upgradeable with single authority: 40-50
    """
    program_id = SOLANA_PROGRAM_IDS.get(slug)
    if not program_id:
        return None

    authority_info = get_solana_program_authority(program_id)
    if authority_info is None:
        return None  # API unavailable, caller should fall back to static

    if not authority_info["upgradeable"]:
        # Program is immutable — very safe
        return 90

    # Program is upgradeable — score based on authority type
    # Without further on-chain analysis of the authority account (multisig vs EOA),
    # use a moderate score. Protocols with active governance get a small bump.
    if slug == "jupiter-perpetual-exchange":
        return 55  # JUP DAO active, but program still upgradeable
    elif slug == "drift":
        return 50  # Upgradeable, governance active
    else:
        return 45  # Upgradeable, unknown authority type

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
    "aave": "aavedao.eth",  # migrated from aave.eth (deleted)
    "lido": "lido-snapshot.eth",
    "compound-finance": "comp-vote.eth",
    "uniswap": "uniswapgovernance.eth",
    "curve-finance": "curve.eth",
    "convex-finance": "cvx.eth",
    # Sky (formerly MakerDAO) uses fully on-chain governance — no Snapshot space
    # Solana protocols use Realms (SPL Governance) — no Snapshot spaces
}

# Fallback governance proposal counts for protocols that use on-chain governance
# without a Snapshot space. Updated periodically via manual review or on-chain queries.
# Sky governance: https://vote.makerdao.com/ — executive votes + polls
ONCHAIN_GOVERNANCE_FALLBACK = {
    "sky": {"governance_proposals_90d": 15},  # Sky averages ~15 executive votes + polls per 90d
}


def fetch_protocol_data(slug):
    """Fetch protocol data from DeFiLlama."""
    time.sleep(1)  # rate limit
    try:
        resp = requests.get(f"{DEFILLAMA_BASE}/protocol/{slug}", timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        logger.error(f"[psi_collector] {slug}: DL /protocol timeout after 15s, skipping")
        return None
    except Exception as e:
        logger.error(f"Failed to fetch {slug}: {e}")
        return None


def fetch_fees_data(slug):
    """Fetch fee/revenue data from DeFiLlama."""
    time.sleep(1)
    try:
        resp = requests.get(f"{DEFILLAMA_BASE}/summary/fees/{slug}", timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except requests.exceptions.Timeout:
        logger.error(f"[psi_collector] {slug}: DL /fees timeout after 15s, skipping")
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
    "drift": {"amount": 270000000, "since": "2026-04-01"},
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
                    "community_data": "true", "developer_data": "false"},
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
    else:
        raw["audit_count"] = 0  # explicit zero so scoring engine includes it in weighted avg

    # Audit recency — DeFiLlama's 'audits' field is only a count (e.g. 2), and
    # 'audit_links' are URLs to audit pages without parseable dates. The API does
    # not expose individual audit timestamps, so we use conservative defaults.
    if audit_links or (audits and int(audits) > 0):
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

    # Bad debt (static config — live scoring always uses current amount)
    bad_debt_entry = KNOWN_BAD_DEBT.get(slug, 0)
    bad_debt = bad_debt_entry["amount"] if isinstance(bad_debt_entry, dict) else bad_debt_entry
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

    # Governance proposals from Snapshot (or on-chain fallback)
    space_id = SNAPSHOT_SPACES.get(slug) or _get_backlog_field(slug, "snapshot_space")
    if space_id:
        proposal_count = fetch_snapshot_proposals(space_id)
        if proposal_count is not None:
            raw_values["governance_proposals_90d"] = proposal_count
    elif slug in ONCHAIN_GOVERNANCE_FALLBACK:
        fallback = ONCHAIN_GOVERNANCE_FALLBACK[slug]
        if "governance_proposals_90d" in fallback:
            raw_values["governance_proposals_90d"] = fallback["governance_proposals_90d"]

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
    elif slug in SOLANA_PROGRAM_IDS:
        # Solana protocols — check on-chain upgrade authority via Helius
        onchain_score = get_solana_admin_risk_score(slug)
        if onchain_score is not None:
            raw_values["protocol_admin_key_risk"] = onchain_score
            logger.info(f"PSI {slug}: admin risk from on-chain authority = {onchain_score}")
        else:
            # Helius unavailable — fall back to static score
            raw_values["protocol_admin_key_risk"] = _PROTOCOL_ADMIN_SCORES.get(slug, 50)
    elif slug in _PROTOCOL_ADMIN_SCORES:
        # Non-EVM protocols without Solana program ID — use static score
        raw_values["protocol_admin_key_risk"] = _PROTOCOL_ADMIN_SCORES[slug]

    # Governance stability — temporal signal from config change detection
    try:
        from app.collectors.governance_detector import compute_governance_stability
        raw_values["governance_stability"] = compute_governance_stability(slug)
    except Exception as e:
        logger.debug(f"governance_stability unavailable for {slug}: {e}")

    # Collateral coverage ratio — pre-computed by collect_coverage_and_markets()
    try:
        from app.collectors.collateral_coverage import normalize_coverage_ratio
        coverage_row = fetch_one("""
            SELECT collateral_coverage_ratio FROM (
                SELECT protocol_slug,
                    SUM(CASE WHEN is_sii_scored THEN tvl_usd ELSE 0 END) /
                    NULLIF(SUM(tvl_usd), 0) * 100 AS collateral_coverage_ratio
                FROM protocol_collateral_exposure
                WHERE protocol_slug = %s AND snapshot_date = CURRENT_DATE
                GROUP BY protocol_slug
            ) sub
        """, (slug,))
        if coverage_row and coverage_row["collateral_coverage_ratio"] is not None:
            raw_values["collateral_coverage_ratio"] = normalize_coverage_ratio(
                float(coverage_row["collateral_coverage_ratio"])
            )
    except Exception as e:
        logger.debug(f"collateral_coverage_ratio unavailable for {slug}: {e}")

    # Market listing velocity — pre-computed from market snapshots
    try:
        from app.collectors.collateral_coverage import compute_market_listing_velocity
        raw_values["market_listing_velocity"] = compute_market_listing_velocity(slug)
    except Exception as e:
        logger.debug(f"market_listing_velocity unavailable for {slug}: {e}")

    result = score_entity(PSI_V01_DEFINITION, raw_values)
    result["protocol_slug"] = slug
    result["protocol_name"] = protocol_data.get("name", slug)
    result["raw_values"] = raw_values

    return result


def score_protocol_from_raw(slug, raw_values):
    """Score a protocol from stored raw_values (no API calls). For verification."""
    if not raw_values:
        return None
    result = score_entity(PSI_V01_DEFINITION, raw_values)
    result["protocol_slug"] = slug
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
    coverage_threshold = float(os.environ.get("PROTOCOL_PROMOTE_COVERAGE_PCT", "52"))

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

        # Fallback: use gecko_id from database if DeFiLlama didn't provide one
        if not gecko_id:
            db_row = fetch_one("SELECT gecko_id FROM protocol_backlog WHERE slug = %s", (slug,))
            if db_row and db_row.get("gecko_id"):
                gecko_id = db_row["gecko_id"]
                logger.info(f"Using resolved gecko_id for {slug}: {gecko_id}")

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
        bad_debt_entry = KNOWN_BAD_DEBT.get(slug, 0)
        bad_debt = bad_debt_entry["amount"] if isinstance(bad_debt_entry, dict) else bad_debt_entry
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

        # Admin risk: try on-chain Solana authority, then static fallback
        if slug in SOLANA_PROGRAM_IDS:
            onchain_score = get_solana_admin_risk_score(slug)
            if onchain_score is not None:
                raw_values["protocol_admin_key_risk"] = onchain_score
            else:
                raw_values["protocol_admin_key_risk"] = _PROTOCOL_ADMIN_SCORES.get(slug, 50)
        else:
            raw_values["protocol_admin_key_risk"] = _PROTOCOL_ADMIN_SCORES.get(slug, 50)

        # Count available components
        components_total = len(PSI_V01_DEFINITION["components"])
        components_available = sum(
            1 for comp_id in PSI_V01_DEFINITION["components"]
            if comp_id in raw_values and raw_values[comp_id] is not None
        )
        coverage_pct = (components_available / components_total * 100) if components_total else 0

        # Determine enrichment status via category-completeness gate
        from app.scoring_engine import is_category_complete
        is_complete, missing_cats = is_category_complete(raw_values, PSI_V01_DEFINITION)
        if is_complete:
            new_status = "ready"
        else:
            new_status = "enriching"
            logger.debug(
                f"Enrichment: {name} ({slug}) category-incomplete — "
                f"missing: {', '.join(missing_cats)}"
            )

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
    """Promote protocols with category-complete data to PSI scoring.

    Protocols with enrichment_status='ready' get promoted. The enrichment
    step already verifies category completeness (every PSI category has >= 1
    populated component). On the next run_psi_scoring() call,
    get_scoring_protocols() will include them.

    Returns number of protocols promoted.
    """
    eligible = fetch_all("""
        SELECT slug, name, stablecoin_exposure_usd, coverage_pct,
               components_available, components_total
        FROM protocol_backlog
        WHERE enrichment_status = 'ready'
        ORDER BY stablecoin_exposure_usd DESC
    """)

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
    # Pre-scoring: capture governance snapshots and collateral/market data
    try:
        from app.collectors.governance_detector import capture_all_governance_snapshots
        capture_all_governance_snapshots()
    except Exception as e:
        logger.warning(f"Governance snapshot capture failed: {e}")

    try:
        from app.collectors.collateral_coverage import collect_coverage_and_markets
        collect_coverage_and_markets()
    except Exception as e:
        logger.warning(f"Coverage/market collection failed: {e}")

    results = []
    scoring_protocols = get_scoring_protocols()
    for slug in scoring_protocols:
        logger.info(f"Scoring protocol: {slug}")
        result = score_protocol(slug)
        if result:
            # Strip internal _token_holdings before storing (saved separately)
            raw_for_storage = {k: v for k, v in result["raw_values"].items() if not k.startswith("_")}

            # Compute inputs_hash for computation attestation
            raw_canonical = json.dumps(raw_for_storage, sort_keys=True, default=str)
            inputs_hash = "0x" + hashlib.sha256(raw_canonical.encode()).hexdigest()

            execute("""
                INSERT INTO psi_scores (protocol_slug, protocol_name, overall_score, grade,
                    category_scores, component_scores, raw_values, formula_version, inputs_hash,
                    confidence, confidence_tag, component_coverage,
                    components_populated, components_total, missing_categories)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT ON CONSTRAINT psi_scores_protocol_slug_scored_date_key
                DO UPDATE SET
                    protocol_name = EXCLUDED.protocol_name,
                    overall_score = EXCLUDED.overall_score,
                    grade = EXCLUDED.grade,
                    category_scores = EXCLUDED.category_scores,
                    component_scores = EXCLUDED.component_scores,
                    raw_values = EXCLUDED.raw_values,
                    inputs_hash = EXCLUDED.inputs_hash,
                    confidence = EXCLUDED.confidence,
                    confidence_tag = EXCLUDED.confidence_tag,
                    component_coverage = EXCLUDED.component_coverage,
                    components_populated = EXCLUDED.components_populated,
                    components_total = EXCLUDED.components_total,
                    missing_categories = EXCLUDED.missing_categories,
                    computed_at = NOW()
            """, (
                result["protocol_slug"],
                result["protocol_name"],
                result["overall_score"],
                None,
                json.dumps(result["category_scores"]),
                json.dumps(result["component_scores"]),
                json.dumps(raw_for_storage, default=str),
                result["version"],
                inputs_hash,
                result.get("confidence"),
                result.get("confidence_tag"),
                result.get("component_coverage"),
                result.get("components_populated"),
                result.get("components_total"),
                json.dumps(result.get("missing_categories") or []),
            ))
            # Store per-token treasury holdings with SII cross-reference
            token_holdings = result.get("raw_values", {}).get("_token_holdings")
            if token_holdings:
                store_treasury_holdings(slug, token_holdings)
                logger.info(f"  Stored {len(token_holdings)} treasury holdings for {slug}")

            results.append(result)
            logger.info(
                f"  {result['protocol_name']}: {result['overall_score']} "
                f"- {result['components_available']}/{result['components_total']} components"
            )

            # --- Auto-generate assessment event on significant score change ---
            try:
                prev_row = fetch_one("""
                    SELECT overall_score FROM psi_scores
                    WHERE protocol_slug = %s AND scored_date < CURRENT_DATE
                    ORDER BY computed_at DESC LIMIT 1
                """, (slug,))

                if prev_row and prev_row.get("overall_score"):
                    prev_score = float(prev_row["overall_score"])
                    current_score = result["overall_score"]
                    delta = current_score - prev_score

                    if abs(delta) >= 3.0:
                        direction = "declined" if delta < 0 else "improved"
                        if abs(delta) >= 10:
                            severity = "critical"
                        elif abs(delta) >= 5:
                            severity = "alert"
                        else:
                            severity = "notable"

                        from app.agent.store import store_assessment
                        event = {
                            "wallet_address": f"protocol:{slug}",
                            "chain": "multi",
                            "trigger_type": "psi_score_change",
                            "trigger_detail": {
                                "entity_type": "protocol",
                                "entity_id": slug,
                                "title": f"{result['protocol_name']} PSI {direction} {abs(delta):.1f} pts",
                                "description": f"PSI score moved from {prev_score:.1f} to {current_score:.1f} ({delta:+.1f}).",
                                "previous_score": prev_score,
                                "current_score": current_score,
                                "delta": round(delta, 2),
                            },
                            "wallet_risk_score": current_score,
                            "wallet_risk_grade": None,
                            "wallet_risk_score_prev": prev_score,
                            "concentration_hhi": None,
                            "concentration_hhi_prev": None,
                            "coverage_ratio": None,
                            "total_stablecoin_value": result["raw_values"].get("tvl"),
                            "holdings_snapshot": [],
                            "severity": severity,
                            "broadcast": severity in ("alert", "critical"),
                            "content_hash": inputs_hash,
                            "methodology_version": result["version"],
                        }
                        event_id = store_assessment(event)
                        if event_id:
                            logger.info(f"PSI event: {slug} {severity} ({delta:+.1f} pts)")
            except Exception as e:
                logger.debug(f"PSI event generation error for {slug}: {e}")

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


# =========================================================================
# Chain Discovery — surface chains needing collector coverage
# =========================================================================

# Known RPC providers per chain (for spec generation)
CHAIN_RPC_PROVIDERS = {
    "Solana": {"provider": "Helius", "url": "https://helius.dev", "free_tier": "1M credits/month", "env_var": "HELIUS_API_KEY"},
    "Sui": {"provider": "Shinami or Mysten Labs", "url": "https://shinami.com", "free_tier": "varies", "env_var": "SUI_RPC_URL"},
    "Aptos": {"provider": "Nodereal or Aptos Labs", "url": "https://nodereal.io", "free_tier": "varies", "env_var": "APTOS_RPC_URL"},
    "Avalanche": {"provider": "Infura or Alchemy", "url": "https://alchemy.com", "free_tier": "300M CU/month", "env_var": "AVAX_RPC_URL"},
    "BSC": {"provider": "Nodereal or Ankr", "url": "https://nodereal.io", "free_tier": "varies", "env_var": "BSC_RPC_URL"},
    "Polygon": {"provider": "Alchemy or Infura", "url": "https://alchemy.com", "free_tier": "300M CU/month", "env_var": "POLYGON_RPC_URL"},
    "Optimism": {"provider": "Alchemy", "url": "https://alchemy.com", "free_tier": "300M CU/month", "env_var": "OP_RPC_URL"},
}

# Chains already supported (collector exists)
COVERED_CHAINS = {"Ethereum", "Base", "Arbitrum", "Solana"}

# Threshold: aggregate stablecoin TVL on a chain to consider it for expansion
CHAIN_EXPANSION_TVL_THRESHOLD = float(os.environ.get("CHAIN_EXPANSION_TVL_THRESHOLD", "500000000"))  # $500M default


def discover_chain_candidates() -> list:
    """
    Discover chains that need SII/PSI coverage based on stablecoin TVL concentration.

    Scans DeFiLlama pool data (same source as protocol discovery) and aggregates
    stablecoin TVL per chain. Chains not in COVERED_CHAINS that exceed the threshold
    are candidates for collector development.

    Returns list of candidates sorted by stablecoin TVL descending.
    """
    pools = fetch_protocol_pools()
    if not pools:
        return []

    # Aggregate stablecoin TVL by chain
    chain_tvl = {}  # chain -> {total_tvl, stablecoin_tvl, protocol_count, protocols, stablecoins}

    for pool in pools:
        chain = pool.get("chain", "")
        if not chain or chain in COVERED_CHAINS:
            continue

        tvl = pool.get("tvlUsd") or 0
        if tvl <= 0:
            continue

        project = pool.get("project", "")
        pool_symbol = pool.get("symbol", "")
        is_stable_pool = pool.get("stablecoin", False)

        symbols = _extract_stablecoin_symbols(pool_symbol)
        stable_tvl = 0
        stable_symbols = set()

        for sym in symbols:
            if _is_stablecoin_token(sym, sym) or is_stable_pool:
                stable_syms = [s for s in symbols if _is_stablecoin_token(s, s)]
                stable_tvl += tvl / len(stable_syms) if stable_syms else tvl
                stable_symbols.add(sym)

        if chain not in chain_tvl:
            chain_tvl[chain] = {
                "chain": chain,
                "total_tvl": 0,
                "stablecoin_tvl": 0,
                "protocol_count": 0,
                "protocols": set(),
                "stablecoins": set(),
            }

        chain_tvl[chain]["total_tvl"] += tvl
        chain_tvl[chain]["stablecoin_tvl"] += stable_tvl
        chain_tvl[chain]["stablecoins"].update(stable_symbols)
        if project:
            chain_tvl[chain]["protocols"].add(project)

    # Filter to candidates above threshold
    candidates = []
    for chain, data in chain_tvl.items():
        data["protocol_count"] = len(data["protocols"])
        data["protocols"] = sorted(data["protocols"])[:20]  # top 20 for display
        data["stablecoins"] = sorted(data["stablecoins"])
        data["rpc_provider"] = CHAIN_RPC_PROVIDERS.get(chain, {"provider": "Unknown", "url": "", "free_tier": "check docs", "env_var": chain.upper() + "_RPC_URL"})

        if data["stablecoin_tvl"] >= CHAIN_EXPANSION_TVL_THRESHOLD:
            candidates.append(data)

    candidates.sort(key=lambda x: x["stablecoin_tvl"], reverse=True)

    if candidates:
        logger.info(f"Chain discovery: {len(candidates)} chain(s) above ${CHAIN_EXPANSION_TVL_THRESHOLD/1e6:.0f}M threshold")
        for c in candidates[:5]:
            logger.info(f"  {c['chain']}: ${c['stablecoin_tvl']/1e6:.0f}M stablecoin TVL, {c['protocol_count']} protocols")

    return candidates


def run_chain_discovery() -> dict:
    """
    Run chain candidate discovery. If a new chain crosses the threshold,
    generate a collector spec and create an assessment event for notification.
    """
    from app.services.chain_spec_generator import generate_collector_spec

    candidates = discover_chain_candidates()
    if not candidates:
        return {"candidates": 0, "specs_generated": 0, "chains": []}

    # Check which candidates are new (not already spec'd)
    specs_generated = 0
    for candidate in candidates:
        chain = candidate["chain"]
        spec_path = os.path.join("docs", "collector_specs", f"{chain.lower()}_collector_spec.md")

        if os.path.exists(spec_path):
            continue  # already spec'd

        # Generate spec
        try:
            path = generate_collector_spec(candidate)
            specs_generated += 1

            # Create assessment event for notification
            from app.agent.store import store_assessment
            event = {
                "wallet_address": f"chain:{chain.lower()}",
                "chain": chain.lower(),
                "trigger_type": "chain_expansion_ready",
                "trigger_detail": {
                    "entity_type": "chain",
                    "entity_id": chain.lower(),
                    "title": f"Chain expansion candidate: {chain}",
                    "description": (
                        f"{chain} crossed ${candidate['stablecoin_tvl']/1e6:,.0f}M stablecoin TVL "
                        f"across {candidate['protocol_count']} protocols. "
                        f"Collector spec generated at {path}. "
                        f"Stablecoins present: {', '.join(candidate.get('stablecoins', [])[:5])}. "
                        f"Recommended RPC: {candidate.get('rpc_provider', {}).get('provider', 'Unknown')}."
                    ),
                    "spec_path": path,
                    "stablecoin_tvl": candidate["stablecoin_tvl"],
                    "protocol_count": candidate["protocol_count"],
                },
                "wallet_risk_score": None,
                "wallet_risk_grade": None,
                "wallet_risk_score_prev": None,
                "concentration_hhi": None,
                "concentration_hhi_prev": None,
                "coverage_ratio": None,
                "total_stablecoin_value": candidate["stablecoin_tvl"],
                "holdings_snapshot": [],
                "severity": "notable",
                "broadcast": True,
                "content_hash": None,
                "methodology_version": "chain-discovery-v1",
            }
            store_assessment(event)
            logger.info(f"Chain expansion event created for {chain}: ${candidate['stablecoin_tvl']/1e6:,.0f}M")

        except Exception as e:
            logger.error(f"Chain spec generation failed for {chain}: {e}")

    return {
        "candidates": len(candidates),
        "specs_generated": specs_generated,
        "chains": [c["chain"] for c in candidates],
    }
