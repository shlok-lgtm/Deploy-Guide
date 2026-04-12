"""
Basis Protocol - Scoring Engine
================================
Single source of truth for SII score calculation.

Formula (v1.0.0):
  SII = 0.30×Peg + 0.25×Liquidity + 0.15×MintBurn + 0.10×Distribution + 0.20×Structural
  
  Structural = 0.30×Reserves + 0.20×SmartContract + 0.15×Oracle + 0.20×Governance + 0.15×Network

All normalization functions preserved exactly from the original codebase.
"""

import math
from typing import Dict, Optional, Any

# =============================================================================
# v1.0.0 Formula Weights (canonical — do not change without versioning)
# =============================================================================

SII_V1_WEIGHTS = {
    "peg_stability": 0.30,
    "liquidity_depth": 0.25,
    "mint_burn_dynamics": 0.15,
    "holder_distribution": 0.10,
    "structural_risk_composite": 0.20,
}

STRUCTURAL_SUBWEIGHTS = {
    "reserves_collateral": 0.30,
    "smart_contract_risk": 0.20,
    "oracle_integrity": 0.15,
    "governance_operations": 0.20,
    "network_chain_risk": 0.15,
}

FORMULA_VERSION = "v1.0.0"


# =============================================================================
# Normalization Functions (exact copies from original)
# =============================================================================

def normalize_inverse_linear(value: float, perfect: float, threshold: float) -> float:
    """Lower values are better (e.g., peg deviation, volatility)."""
    if value <= perfect:
        return 100.0
    if value >= threshold:
        return 0.0
    return 100.0 - ((value - perfect) / (threshold - perfect) * 100.0)


def normalize_linear(value: float, min_val: float, max_val: float) -> float:
    """Higher values are better, linear scale."""
    if value <= min_val:
        return 0.0
    if value >= max_val:
        return 100.0
    return ((value - min_val) / (max_val - min_val)) * 100.0


def normalize_log(value: float, thresholds: dict) -> float:
    """Logarithmic scale for exponential ranges (market cap, TVL, volume)."""
    if value <= 0:
        return 0.0
    sorted_t = sorted(thresholds.items())
    for thresh, score in sorted_t:
        if value < thresh:
            return score
    return sorted_t[-1][1]


def normalize_centered(value: float, center: float, tolerance: float, extreme: float) -> float:
    """Deviation from center in either direction is bad (funding rate, pool balance)."""
    deviation = abs(value - center)
    if deviation <= tolerance:
        return 100.0
    if deviation >= extreme:
        return 0.0
    return 100.0 - ((deviation - tolerance) / (extreme - tolerance) * 100.0)


def normalize_exponential_penalty(value: float, ideal: float, decay_rate: float = 200) -> float:
    """Exponential penalty for deviation from ideal."""
    deviation = abs(value - ideal)
    return 100.0 * math.exp(-deviation * decay_rate)


def normalize_direct(value: float) -> float:
    """Direct mapping: value = score (for percentages 0-100)."""
    return max(0.0, min(100.0, float(value)))


# =============================================================================
# Grade Mapping (exact copy from original)
# =============================================================================

# DEPRECATED: retained for backward compatibility. Grade output removed from all surfaces.
def score_to_grade(score: float) -> str:
    """Convert numeric score to letter grade."""
    if score >= 90:
        return "A+"
    elif score >= 85:
        return "A"
    elif score >= 80:
        return "A-"
    elif score >= 75:
        return "B+"
    elif score >= 70:
        return "B"
    elif score >= 65:
        return "B-"
    elif score >= 60:
        return "C+"
    elif score >= 55:
        return "C"
    elif score >= 50:
        return "C-"
    elif score >= 45:
        return "D"
    else:
        return "F"


# =============================================================================
# SII Calculation
# =============================================================================

def calculate_structural_composite(subscores: Dict[str, Optional[float]]) -> Optional[float]:
    """
    Calculate Structural Risk Composite from its 5 subcategories.
    
    Formula:
      Structural = 0.30×Reserves + 0.20×SmartContract + 0.15×Oracle + 0.20×Governance + 0.15×Network
    
    Returns None if no subcategory data is available.
    """
    total = 0.0
    weight_used = 0.0
    
    for subcat, weight in STRUCTURAL_SUBWEIGHTS.items():
        score = subscores.get(subcat)
        if score is not None:
            total += score * weight
            weight_used += weight
    
    if weight_used == 0:
        return None
    
    # Renormalize if not all subcategories present
    if weight_used < 1.0:
        return total / weight_used
    
    return total


def calculate_sii(category_scores: Dict[str, Optional[float]]) -> Optional[float]:
    """
    Calculate SII using the canonical v1.0.0 formula.
    
    Formula:
      SII = 0.30×Peg + 0.25×Liquidity + 0.15×MintBurn + 0.10×Distribution + 0.20×Structural
    
    Args:
        category_scores: Dict with scores (0-100) for each canonical category.
    
    Returns:
        SII score (0-100), or None if no data available.
    """
    total = 0.0
    weight_used = 0.0
    
    for cat, weight in SII_V1_WEIGHTS.items():
        score = category_scores.get(cat)
        if score is not None:
            total += score * weight
            weight_used += weight
    
    if weight_used == 0:
        return None
    
    # Renormalize to available weight
    if weight_used < 1.0:
        return total / weight_used
    
    return total


# =============================================================================
# Legacy category mapping (old 8-cat → new 5-cat)
# =============================================================================

LEGACY_TO_V1_MAPPING = {
    "peg_stability": "peg_stability",
    "liquidity": "liquidity_depth",
    "market_activity": "mint_burn_dynamics",
    "flows": "mint_burn_dynamics",
    "holder_distribution": "holder_distribution",
    "smart_contract": "structural_risk_composite",
    # These all roll into structural_risk_composite:
    "governance": "structural_risk_composite",
    "transparency": "structural_risk_composite",
    "regulatory": "structural_risk_composite",
    "network": "structural_risk_composite",
    "reserves": "structural_risk_composite",
    "smart_contract": "structural_risk_composite",
    "oracle": "structural_risk_composite",
}

DB_TO_STRUCTURAL_MAPPING = {
    "governance": "governance_operations",
    "transparency": "governance_operations",
    "regulatory": "governance_operations",
    "network": "network_chain_risk",
    "reserves": "reserves_collateral",
    "smart_contract": "smart_contract_risk",
    "oracle": "oracle_integrity",
}


def aggregate_legacy_to_v1(
    legacy_scores: Dict[str, float],
) -> Dict[str, Optional[float]]:
    """
    Convert old 8-category scores to v1.0.0 5-category format.
    Handles the structural composite calculation internally.
    """
    v1_scores: Dict[str, Optional[float]] = {
        "peg_stability": legacy_scores.get("peg_stability"),
        "liquidity_depth": legacy_scores.get("liquidity"),
        "mint_burn_dynamics": legacy_scores.get("market_activity"),
        "holder_distribution": legacy_scores.get("holder_distribution"),
        "structural_risk_composite": None,
    }
    
    # Build structural subscores from legacy categories
    structural_buckets: Dict[str, list[float]] = {}
    for legacy_cat, structural_subcat in DB_TO_STRUCTURAL_MAPPING.items():
        if legacy_cat in legacy_scores:
            structural_buckets.setdefault(structural_subcat, []).append(legacy_scores[legacy_cat])
    
    # Average any folded subcategories, then calculate composite
    if structural_buckets:
        structural_subscores = {
            subcat: sum(scores) / len(scores)
            for subcat, scores in structural_buckets.items()
        }
        v1_scores["structural_risk_composite"] = calculate_structural_composite(structural_subscores)
    
    return v1_scores


# =============================================================================
# Component Normalization Specs (for collector → score pipeline)
# =============================================================================

# Maps component_id → normalization config
# Used by collectors to normalize raw values before storing
COMPONENT_NORMALIZATIONS = {
    # =========================================================================
    # Peg Stability (6 components, weights sum to 1.0)
    # =========================================================================
    "peg_current_deviation": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 5},
        "category": "peg_stability",
        "weight": 0.22,
    },
    "peg_24h_max_deviation": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 10},
        "category": "peg_stability",
        "weight": 0.18,
    },
    "peg_7d_stddev": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 0.02},
        "category": "peg_stability",
        "weight": 0.18,
    },
    "peg_30d_stability": {
        "fn": normalize_direct,
        "params": {},
        "category": "peg_stability",
        "weight": 0.10,
    },
    "depeg_events_30d": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 5},
        "category": "peg_stability",
        "weight": 0.12,
    },
    "max_drawdown_30d": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 5},
        "category": "peg_stability",
        "weight": 0.10,
    },

    # =========================================================================
    # Liquidity (8 components, weights sum to 1.0)
    # =========================================================================
    "market_cap": {
        "fn": normalize_log,
        "params": {"thresholds": {1e6: 10, 1e8: 40, 1e9: 60, 1e10: 80, 1e11: 100}},
        "category": "liquidity",
        "weight": 0.15,
    },
    "volume_24h": {
        "fn": normalize_log,
        "params": {"thresholds": {1e6: 20, 1e7: 40, 1e8: 60, 1e9: 80, 1e10: 100}},
        "category": "liquidity",
        "weight": 0.15,
    },
    "volume_mcap_ratio": {
        "fn": normalize_linear,
        "params": {"min_val": 0.01, "max_val": 0.15},
        "category": "liquidity",
        "weight": 0.15,
    },
    "dex_liquidity_tvl": {
        "fn": normalize_log,
        "params": {"thresholds": {1e6: 20, 1e7: 40, 1e8: 60, 5e8: 80, 1e9: 100}},
        "category": "liquidity",
        "weight": 0.10,
    },
    "curve_3pool_balance": {
        "fn": normalize_centered,
        "params": {"center": 33.33, "tolerance": 5, "extreme": 20},
        "category": "liquidity",
        "weight": 0.10,
    },
    "cex_listing_count": {
        "fn": normalize_linear,
        "params": {"min_val": 5, "max_val": 50},
        "category": "liquidity",
        "weight": 0.10,
    },
    "dex_pool_count": {
        "fn": normalize_linear,
        "params": {"min_val": 3, "max_val": 20},
        "category": "liquidity",
        "weight": 0.10,
    },
    "lending_tvl": {
        "fn": normalize_log,
        "params": {"thresholds": {1e6: 10, 1e7: 30, 1e8: 50, 1e9: 80, 5e9: 100}},
        "category": "liquidity",
        "weight": 0.10,
    },

    # =========================================================================
    # Transparency / Reserves (→ structural) (4 components, weights sum to 1.0)
    # =========================================================================
    "reserve_to_supply_ratio": {
        "fn": normalize_linear,
        "params": {"min_val": 0.98, "max_val": 1.02},
        "category": "transparency",
        "weight": 0.30,
    },
    "cash_equivalents_pct": {
        "fn": normalize_direct,
        "params": {},
        "category": "transparency",
        "weight": 0.25,
    },
    "attestation_freshness": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 90},
        "category": "transparency",
        "weight": 0.25,
    },
    "auditor_quality": {
        "fn": normalize_direct,
        "params": {},
        "category": "transparency",
        "weight": 0.20,
    },

    # =========================================================================
    # Holder Distribution (6 components, weights sum to 1.0)
    # =========================================================================
    "top_10_concentration": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 10, "threshold": 80},
        "category": "holder_distribution",
        "weight": 0.22,
    },
    "unique_holders": {
        "fn": normalize_log,
        "params": {"thresholds": {1000: 20, 10000: 40, 100000: 60, 1000000: 80, 10000000: 100}},
        "category": "holder_distribution",
        "weight": 0.19,
    },
    "exchange_concentration": {
        "fn": normalize_centered,
        "params": {"center": 30, "tolerance": 15, "extreme": 40},
        "category": "holder_distribution",
        "weight": 0.15,
    },
    "stablecoin_market_share": {
        "fn": normalize_linear,
        "params": {"min_val": 0.1, "max_val": 30},
        "category": "holder_distribution",
        "weight": 0.19,
    },
    # Primitive #21: Actor Classification — agent concentration components
    "agent_holder_share": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 5, "threshold": 60},
        "category": "holder_distribution",
        "weight": 0.15,
    },
    "correlated_response_risk": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 50},
        "category": "holder_distribution",
        "weight": 0.10,
    },

    # =========================================================================
    # Network / Chain Risk (→ structural)
    # =========================================================================
    "chain_count": {
        "fn": normalize_linear,
        "params": {"min_val": 1, "max_val": 50},
        "category": "network",
        "weight": 0.50,
    },

    # =========================================================================
    # Smart Contract Risk (→ structural) (6 components, weights sum to 1.0)
    # =========================================================================
    "contract_verified": {
        "fn": normalize_direct,
        "params": {},
        "category": "smart_contract",
        "weight": 0.15,
    },
    "pausability": {
        "fn": normalize_direct,
        "params": {},
        "category": "smart_contract",
        "weight": 0.10,
    },
    "blacklist_capability": {
        "fn": normalize_direct,
        "params": {},
        "category": "smart_contract",
        "weight": 0.10,
    },
    "admin_key_risk": {
        "fn": normalize_direct,
        "params": {},
        "category": "smart_contract",
        "weight": 0.30,
    },
    "bug_bounty_score": {
        "fn": normalize_direct,
        "params": {},
        "category": "smart_contract",
        "weight": 0.20,
    },
    "exploit_history": {
        "fn": normalize_direct,
        "params": {},
        "category": "smart_contract",
        "weight": 0.15,
    },

    # =========================================================================
    # Peg Stability additions
    # =========================================================================
    "arbitrage_efficiency": {
        "fn": normalize_linear,
        "params": {"min_val": 0.2, "max_val": 0.9},
        "category": "peg_stability",
        "weight": 0.10,
    },

    # =========================================================================
    # Network additions
    # =========================================================================
    "gas_resilience": {
        "fn": normalize_direct,
        "params": {},
        "category": "network",
        "weight": 0.50,
    },

    # =========================================================================
    # Liquidity additions
    # =========================================================================
    "bid_ask_spread": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0.05, "threshold": 1.0},
        "category": "liquidity",
        "weight": 0.05,
    },

    # =========================================================================
    # Phase 2: On-chain governance reads → Governance category
    # =========================================================================
    "dao_timelock_hours": {
        "fn": normalize_linear,
        "params": {"min_val": 0, "max_val": 168},
        "category": "governance",
        "weight": 0.30,
    },
    "multisig_config": {
        "fn": normalize_direct,
        "params": {},
        "category": "governance",
        "weight": 0.30,
    },

    # Phase 2: On-chain reads → Smart Contract category
    "dao_upgrade_mechanism": {
        "fn": normalize_direct,
        "params": {},
        "category": "smart_contract",
        "weight": 0.10,
    },
    "access_control": {
        "fn": normalize_direct,
        "params": {},
        "category": "smart_contract",
        "weight": 0.05,
    },
    "emergency_mechanism": {
        "fn": normalize_direct,
        "params": {},
        "category": "smart_contract",
        "weight": 0.05,
    },

    # Phase 2: Bridge reads
    "bridge_timelock": {
        "fn": normalize_linear,
        "params": {"min_val": 0, "max_val": 168},
        "category": "governance",
        "weight": 0.20,
    },
    "guardian_count": {
        "fn": normalize_direct,
        "params": {},
        "category": "governance",
        "weight": 0.20,
    },

    # Phase 2: DEX pool data (VSRI/PSI)
    "position_liquidity": {
        "fn": normalize_direct,
        "params": {},
        "category": "liquidity",
        "weight": 0.10,
    },
    "collateral_diversity": {
        "fn": normalize_direct,
        "params": {},
        "category": "liquidity",
        "weight": 0.05,
    },

    # Phase 2: Bridge monitoring
    "message_success_rate": {
        "fn": normalize_direct,
        "params": {},
        "category": "network",
        "weight": 0.25,
    },
    "uptime_pct": {
        "fn": normalize_direct,
        "params": {},
        "category": "network",
        "weight": 0.25,
    },

    # Phase 2: Exchange health
    "api_availability": {
        "fn": normalize_direct,
        "params": {},
        "category": "network",
        "weight": 0.25,
    },

    # Phase 4: Web research components
    "bridge_audit_research": {
        "fn": normalize_direct,
        "params": {},
        "category": "smart_contract",
        "weight": 0.05,
    },
    "por_frequency_research": {
        "fn": normalize_direct,
        "params": {},
        "category": "transparency",
        "weight": 0.10,
    },
    "compensation_transparency": {
        "fn": normalize_direct,
        "params": {},
        "category": "governance",
        "weight": 0.10,
    },
    "meeting_cadence": {
        "fn": normalize_direct,
        "params": {},
        "category": "governance",
        "weight": 0.10,
    },
    "por_method_research": {
        "fn": normalize_direct,
        "params": {},
        "category": "transparency",
        "weight": 0.10,
    },

    # =========================================================================
    # Flows / Mint-Burn Dynamics (5 components, weights sum to 1.0)
    # =========================================================================
    "daily_mint_volume": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 5},  # % of market cap
        "category": "flows",
        "weight": 0.20,
    },
    "daily_burn_volume": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 5},
        "category": "flows",
        "weight": 0.20,
    },
    "net_mint_burn_ratio": {
        "fn": normalize_centered,
        "params": {"center": 0.5, "tolerance": 0.1, "extreme": 0.5},
        "category": "flows",
        "weight": 0.20,
    },
    "supply_change_velocity": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 3},  # % daily change
        "category": "flows",
        "weight": 0.20,
    },
    "unusual_minting_detection": {
        "fn": normalize_inverse_linear,
        "params": {"perfect": 0, "threshold": 5},  # z-score
        "category": "flows",
        "weight": 0.20,
    },
}


def normalize_component(component_id: str, raw_value: float) -> Optional[float]:
    """
    Normalize a raw component value using its registered normalization function.
    
    Returns normalized score (0-100) or None if component not registered.
    """
    spec = COMPONENT_NORMALIZATIONS.get(component_id)
    if not spec:
        return None
    
    fn = spec["fn"]
    params = spec["params"]
    
    try:
        return round(fn(raw_value, **params), 2)
    except Exception:
        return None
