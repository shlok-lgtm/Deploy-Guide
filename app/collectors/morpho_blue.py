"""
Morpho Blue Exposure Collector
==============================
Fills the protocol_collateral_exposure table for Morpho — an isolated-market
lending protocol where each loan/collateral pair is a distinct market.
DeFiLlama's yields API doesn't index these as stablecoin pools, so the
shared-pool collector in psi_collector produces zero rows for Morpho.

Source: Morpho Blue public GraphQL API at blue-api.morpho.org. Covers
Ethereum mainnet and Base (same singleton 0xBBBB...EFFCb on both).

Aggregation strategy: report.py:_get_cross_protocol_exposure uses
    DISTINCT ON (protocol_slug, chain)
so writing a row per Morpho market would collapse to a single market per
chain for cross-protocol lookups. This collector aggregates supply TVL by
(chain, loan_token_symbol) and writes one exposure row per (chain, token).
Per-market metadata lands in morpho_markets for auditing. This intentionally
diverges from psi_collector's chain="all" aggregate — per-chain granularity
is needed so chain_count in cross-protocol queries reflects reality.

Feature flag: MORPHO_BLUE_COLLECTOR_ENABLED (default: true).
Cadence: hourly, invoked from worker fast cycle. Never raises.
"""

import logging
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import httpx

from app.collectors.psi_collector import _is_stablecoin_token
from app.database import execute, get_cursor

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

MORPHO_GRAPHQL_ENDPOINT = os.environ.get(
    "MORPHO_GRAPHQL_ENDPOINT", "https://blue-api.morpho.org/graphql"
)

# Morpho Blue chain IDs we track. Same singleton address on both chains.
SUPPORTED_CHAINS = {
    1: "ethereum",
    8453: "base",
}

PROTOCOL_SLUG = "morpho"
POOL_TYPE = "lending"

# Markets with supply TVL below this are skipped (dust / abandoned markets).
MIN_SUPPLY_USD = 10_000.0


def _collector_enabled() -> bool:
    flag = os.environ.get("MORPHO_BLUE_COLLECTOR_ENABLED", "true")
    return flag.lower() in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------------
# Morpho GraphQL client
# ---------------------------------------------------------------------------

_MARKETS_QUERY = """
query MorphoMarkets($first: Int!, $skip: Int!, $chainIds: [Int!]) {
  markets(
    first: $first
    skip: $skip
    where: { chainId_in: $chainIds }
    orderBy: SupplyAssetsUsd
    orderDirection: Desc
  ) {
    items {
      uniqueKey
      lltv
      irmAddress
      oracleAddress
      morphoBlue { chain { id } }
      loanAsset { address symbol decimals priceUsd }
      collateralAsset { address symbol decimals }
      state {
        supplyAssets
        supplyAssetsUsd
        borrowAssets
        borrowAssetsUsd
      }
    }
  }
}
"""


def _fetch_markets_via_api() -> list[dict[str, Any]] | None:
    """Page through Morpho GraphQL markets endpoint. Returns None on failure."""
    markets: list[dict[str, Any]] = []
    page_size = 100
    skip = 0
    chain_ids = list(SUPPORTED_CHAINS.keys())
    try:
        with httpx.Client(timeout=30) as client:
            while True:
                resp = client.post(
                    MORPHO_GRAPHQL_ENDPOINT,
                    json={
                        "query": _MARKETS_QUERY,
                        "variables": {
                            "first": page_size,
                            "skip": skip,
                            "chainIds": chain_ids,
                        },
                    },
                    headers={"Accept": "application/json", "Content-Type": "application/json"},
                )
                if resp.status_code != 200:
                    logger.warning(
                        f"Morpho GraphQL returned {resp.status_code} (skip={skip}): "
                        f"{resp.text[:200]}"
                    )
                    return None
                payload = resp.json()
                if payload.get("errors"):
                    logger.warning(f"Morpho GraphQL errors: {payload['errors']}")
                    return None
                items = (
                    (payload.get("data") or {}).get("markets") or {}
                ).get("items") or []
                if not items:
                    break
                markets.extend(items)
                if len(items) < page_size:
                    break
                skip += page_size
                if skip >= 2000:
                    logger.warning("Morpho GraphQL pagination hit 2000-item cap")
                    break
        return markets
    except Exception as e:
        logger.warning(f"Morpho GraphQL fetch failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------

def _parse_market(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize one raw GraphQL market into a flat dict.

    Returns None for markets missing required fields (uniqueKey, chain,
    loan_token_symbol, loan_token_address) — these are what downstream
    aggregation and the morpho_markets NOT NULL columns require.
    """
    try:
        uniq = raw.get("uniqueKey")
        if not uniq:
            return None
        chain_block = (raw.get("morphoBlue") or {}).get("chain") or {}
        chain_id_raw = chain_block.get("id")
        try:
            chain_id = int(chain_id_raw) if chain_id_raw is not None else None
        except (TypeError, ValueError):
            chain_id = None
        chain = SUPPORTED_CHAINS.get(chain_id) if chain_id is not None else None
        if not chain:
            return None

        loan = raw.get("loanAsset") or {}
        coll = raw.get("collateralAsset") or {}
        state = raw.get("state") or {}

        loan_sym = (loan.get("symbol") or "").strip().upper() or None
        loan_addr = (loan.get("address") or "").lower() or None
        if not loan_sym or not loan_addr:
            return None

        try:
            loan_dec = int(loan.get("decimals")) if loan.get("decimals") is not None else None
        except (TypeError, ValueError):
            loan_dec = None

        coll_sym = (coll.get("symbol") or "").strip().upper() or None
        coll_addr = (coll.get("address") or "").lower() or None

        def _f(v: Any) -> float:
            try:
                return float(v) if v is not None else 0.0
            except (TypeError, ValueError):
                return 0.0

        try:
            lltv = int(raw.get("lltv")) if raw.get("lltv") is not None else None
        except (TypeError, ValueError):
            lltv = None

        return {
            "market_id": uniq,
            "chain": chain,
            "loan_token": loan_addr,
            "loan_token_symbol": loan_sym,
            "loan_token_decimals": loan_dec,
            "collateral_token": coll_addr,
            "collateral_token_symbol": coll_sym,
            "oracle": raw.get("oracleAddress"),
            "lltv": lltv,
            "irm": raw.get("irmAddress"),
            "supply_assets_usd": _f(state.get("supplyAssetsUsd")),
            "borrow_assets_usd": _f(state.get("borrowAssetsUsd")),
        }
    except Exception as e:
        logger.debug(f"Morpho market parse error: {e}")
        return None


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

_MARKET_COLUMNS = (
    "market_id", "chain", "loan_token", "loan_token_symbol", "loan_token_decimals",
    "collateral_token", "collateral_token_symbol", "oracle", "lltv", "irm",
    "supply_assets_usd", "borrow_assets_usd",
)


def _batch_upsert_markets(rows: list[dict[str, Any]]) -> int:
    """Batch upsert into morpho_markets using execute_values (one round trip)."""
    if not rows:
        return 0
    tuples = [tuple(r[c] for c in _MARKET_COLUMNS) for r in rows]
    sql = """
        INSERT INTO morpho_markets
            (market_id, chain, loan_token, loan_token_symbol, loan_token_decimals,
             collateral_token, collateral_token_symbol, oracle, lltv, irm,
             supply_assets_usd, borrow_assets_usd, last_read_at)
        VALUES %s
        ON CONFLICT (market_id) DO UPDATE SET
            chain = EXCLUDED.chain,
            loan_token = EXCLUDED.loan_token,
            loan_token_symbol = EXCLUDED.loan_token_symbol,
            loan_token_decimals = EXCLUDED.loan_token_decimals,
            collateral_token = EXCLUDED.collateral_token,
            collateral_token_symbol = EXCLUDED.collateral_token_symbol,
            oracle = EXCLUDED.oracle,
            lltv = EXCLUDED.lltv,
            irm = EXCLUDED.irm,
            supply_assets_usd = EXCLUDED.supply_assets_usd,
            borrow_assets_usd = EXCLUDED.borrow_assets_usd,
            last_read_at = NOW()
    """
    template = "(" + ",".join(["%s"] * len(_MARKET_COLUMNS)) + ", NOW())"
    try:
        from psycopg2.extras import execute_values
        with get_cursor() as cur:
            execute_values(cur, sql, tuples, template=template, page_size=200)
        return len(tuples)
    except Exception as e:
        logger.warning(f"morpho_markets batch upsert failed: {e}")
        return 0


def _write_exposure_row(
    chain: str, token_symbol: str, tvl_usd: float, is_stable: bool
) -> bool:
    """Write one aggregated exposure row per (chain, token).

    pool_id is {protocol}:{chain}:{symbol}:agg so
    _get_cross_protocol_exposure's DISTINCT ON (protocol_slug, chain) keeps
    one row per chain, then the outer SUM aggregates across chains.
    _get_stablecoin_exposure's DISTINCT ON (token_symbol, chain, pool_id)
    is already unique per row (one pool_id per (chain, symbol)).
    """
    pool_id = f"{PROTOCOL_SLUG}:{chain}:{token_symbol.upper()}:agg"
    try:
        execute(
            """
            INSERT INTO protocol_collateral_exposure
                (protocol_slug, pool_id, token_symbol, chain, tvl_usd,
                 is_stablecoin, is_sii_scored, sii_score, pool_type, snapshot_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_DATE)
            ON CONFLICT (protocol_slug, pool_id, snapshot_date)
            DO UPDATE SET
                token_symbol = EXCLUDED.token_symbol,
                tvl_usd = EXCLUDED.tvl_usd,
                is_stablecoin = EXCLUDED.is_stablecoin,
                pool_type = EXCLUDED.pool_type
            """,
            (
                PROTOCOL_SLUG,
                pool_id,
                token_symbol.upper(),
                chain,
                float(tvl_usd),
                bool(is_stable),
                False,
                None,
                POOL_TYPE,
            ),
        )
        return True
    except Exception as e:
        logger.debug(f"Failed to write morpho exposure {token_symbol}/{chain}: {e}")
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_morpho_blue_collection() -> dict[str, Any]:
    """Run one Morpho Blue collection cycle. Safe to call from worker fast cycle."""
    if not _collector_enabled():
        logger.info("Morpho Blue collector disabled via MORPHO_BLUE_COLLECTOR_ENABLED")
        return {"enabled": False, "markets": 0, "exposure_rows": 0}

    t0 = datetime.now(timezone.utc)
    raw_markets = _fetch_markets_via_api()
    if raw_markets is None:
        logger.warning("Morpho Blue: GraphQL API unreachable, skipping cycle")
        return {"enabled": True, "markets": 0, "exposure_rows": 0, "error": "api_unreachable"}

    parsed: list[dict[str, Any]] = []
    for raw in raw_markets:
        m = _parse_market(raw)
        if m is not None:
            parsed.append(m)

    markets_upserted = _batch_upsert_markets(parsed)

    agg: dict[tuple[str, str], float] = defaultdict(float)
    for m in parsed:
        if m["supply_assets_usd"] < MIN_SUPPLY_USD:
            continue
        agg[(m["chain"], m["loan_token_symbol"])] += m["supply_assets_usd"]

    rows_written = 0
    stable_rows = 0
    stable_usd_total = 0.0
    for (chain, sym), tvl in agg.items():
        is_stable = _is_stablecoin_token(None, sym)
        if _write_exposure_row(chain, sym, tvl, is_stable):
            rows_written += 1
            if is_stable:
                stable_rows += 1
                stable_usd_total += tvl

    elapsed_ms = int((datetime.now(timezone.utc) - t0).total_seconds() * 1000)
    logger.info(
        f"Morpho Blue: {len(parsed)} markets parsed, {markets_upserted} cached, "
        f"{rows_written} exposure rows ({stable_rows} stablecoin, "
        f"${stable_usd_total/1e6:.1f}M total stable supply) in {elapsed_ms}ms"
    )
    return {
        "enabled": True,
        "source": "morpho_blue",
        "markets": len(parsed),
        "markets_cached": markets_upserted,
        "exposure_rows": rows_written,
        "stablecoin_rows": stable_rows,
        "stablecoin_exposure_usd": stable_usd_total,
        "elapsed_ms": elapsed_ms,
    }
