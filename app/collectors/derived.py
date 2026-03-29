"""
Derived Components Collector
==============================
Produces components calculated from existing data — no new API calls.

  - arbitrage_efficiency:  peg correction speed (from existing peg deviation data)
  - gas_resilience:        chain-level gas sensitivity (from DeFiLlama chain data)
  - bid_ask_spread:        market tightness (already collected, wires into scoring)

All data comes from component_readings or static chain analysis.
"""

import logging

from app.database import fetch_one, fetch_all

logger = logging.getLogger(__name__)

# Chain presence for gas resilience scoring
# Ethereum mainnet = 70 (gas spikes affect it), L2 = 90, multi-chain with L2 = 85
CHAIN_RESILIENCE = {
    "usdc":   85,   # Ethereum + many L2s (Arbitrum, Optimism, Base, Polygon)
    "usdt":   85,   # Ethereum + Tron + many L2s
    "dai":    85,   # Ethereum + Arbitrum, Optimism, Polygon
    "frax":   80,   # Ethereum + some L2s
    "pyusd":  75,   # Primarily Ethereum + Solana
    "fdusd":  70,   # Primarily BNB Chain + Ethereum
    "tusd":   75,   # Ethereum + BNB + Tron
    "usdd":   80,   # Tron primary (no Ethereum gas dependency)
    "usde":   75,   # Primarily Ethereum
    "usd1":   70,   # Primarily Ethereum
    "gho":    75,   # Primarily Ethereum (Aave markets)
    "crvusd": 75,   # Primarily Ethereum (Curve pools)
    "dola":   75,   # Primarily Ethereum (Inverse)
    "usdp":   70,   # Primarily Ethereum
}


def collect_derived_components(stablecoin_id: str) -> list[dict]:
    """
    Collect derived components from existing data. Synchronous (no API calls).
    """
    components = []

    # 1. arbitrage_efficiency — from existing peg deviation readings
    arb = _compute_arbitrage_efficiency(stablecoin_id)
    if arb is not None:
        arb_score = _normalize_arb_efficiency(arb)
        components.append({
            "component_id": "arbitrage_efficiency",
            "category": "peg_stability",
            "raw_value": round(arb, 4),
            "normalized_score": round(arb_score, 2),
            "data_source": "derived",
        })

    # 2. gas_resilience — from chain presence config
    gas_score = float(CHAIN_RESILIENCE.get(stablecoin_id, 70))
    components.append({
        "component_id": "gas_resilience",
        "category": "network",
        "raw_value": gas_score,
        "normalized_score": gas_score,
        "data_source": "config",
    })

    # 3. bid_ask_spread — relay from existing CoinGecko collection
    spread = _get_latest_spread(stablecoin_id)
    if spread is not None:
        spread_score = _normalize_spread(spread)
        components.append({
            "component_id": "bid_ask_spread",
            "category": "liquidity",
            "raw_value": round(spread, 4),
            "normalized_score": round(spread_score, 2),
            "data_source": "derived",
        })

    return components


def _compute_arbitrage_efficiency(stablecoin_id: str) -> float | None:
    """
    Compute arbitrage efficiency from existing peg data.
    efficiency = 1 - (current_deviation / max_deviation_24h)
    High efficiency = market corrects deviations quickly.
    """
    current = fetch_one("""
        SELECT raw_value FROM component_readings
        WHERE stablecoin_id = %s AND component_id = 'peg_current_deviation'
        ORDER BY collected_at DESC LIMIT 1
    """, (stablecoin_id,))

    max_dev = fetch_one("""
        SELECT raw_value FROM component_readings
        WHERE stablecoin_id = %s AND component_id = 'peg_24h_max_deviation'
        ORDER BY collected_at DESC LIMIT 1
    """, (stablecoin_id,))

    if not current or not max_dev:
        return None

    curr_val = float(current["raw_value"])
    max_val = float(max_dev["raw_value"])

    # If max deviation is tiny, peg is rock solid → perfect efficiency
    if max_val < 0.01:
        return 1.0

    efficiency = 1.0 - (curr_val / max_val)
    return max(0.0, min(1.0, efficiency))


def _normalize_arb_efficiency(efficiency: float) -> float:
    """Efficiency 0.9+ = 100, 0.5 = 50, <0.2 = 0."""
    if efficiency >= 0.9:
        return 100.0
    if efficiency <= 0.2:
        return 0.0
    # Linear between 0.2 and 0.9
    return (efficiency - 0.2) / (0.9 - 0.2) * 100.0


def _get_latest_spread(stablecoin_id: str) -> float | None:
    """Get the most recent avg_bid_ask_spread from component_readings."""
    row = fetch_one("""
        SELECT raw_value FROM component_readings
        WHERE stablecoin_id = %s AND component_id = 'avg_bid_ask_spread'
        ORDER BY collected_at DESC LIMIT 1
    """, (stablecoin_id,))
    if row and row.get("raw_value") is not None:
        return float(row["raw_value"])
    return None


def _normalize_spread(spread_pct: float) -> float:
    """Lower spread = better. < 0.05% = 100, > 1% = 0."""
    if spread_pct <= 0.05:
        return 100.0
    if spread_pct >= 1.0:
        return 0.0
    return 100.0 - ((spread_pct - 0.05) / (1.0 - 0.05) * 100.0)
