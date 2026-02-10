"""
Offline/Static Collector
=========================
Produces components from config data, scraped attestation files, and static analysis.
No API calls needed — these are transparency, regulatory, governance, and reserve components.
"""

import os
import json
import glob
import logging
from datetime import datetime, timezone

from app.config import STABLECOIN_REGISTRY
from app.scoring import normalize_inverse_linear, normalize_linear, normalize_direct

logger = logging.getLogger(__name__)


# =============================================================================
# Attestation / Transparency Components
# =============================================================================

# Auditor quality tiers
AUDITOR_TIERS = {
    "deloitte": 1, "kpmg": 1, "ey": 1, "pwc": 1,
    "grant thornton": 2, "bdo": 2, "bdo italia": 2,
    "withumsmith+brown": 2, "withum": 2,
    "prescient assurance": 3, "the network firm": 3,
    "state street": 2, "ankura": 2,
    "n/a": 4, "n/a (on-chain)": 3, "n/a (algorithmic)": 3,
    "various custodians": 3,
}

TIER_SCORES = {1: 100, 2: 80, 3: 60, 4: 30}


def collect_transparency_components(stablecoin_id: str) -> list[dict]:
    """Collect transparency components from config and scraped data."""
    cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
    if not cfg:
        return []
    
    components = []
    att = cfg.get("attestation", {})
    
    # Auditor Quality
    auditor = att.get("auditor", "N/A").lower()
    tier = AUDITOR_TIERS.get(auditor, 4)
    components.append({
        "component_id": "auditor_quality",
        "category": "transparency",
        "raw_value": tier,
        "normalized_score": TIER_SCORES.get(tier, 30),
        "data_source": "config",
    })
    
    # Attestation Frequency Score
    freq_days = att.get("frequency_days", 365)
    freq_score = normalize_inverse_linear(freq_days, 1, 120)
    components.append({
        "component_id": "attestation_frequency",
        "category": "transparency",
        "raw_value": freq_days,
        "normalized_score": round(freq_score, 2),
        "data_source": "config",
    })
    
    # Attestation Freshness (check scraped data directory)
    freshness = _get_attestation_freshness(stablecoin_id, freq_days)
    components.append({
        "component_id": "attestation_freshness",
        "category": "transparency",
        "raw_value": freshness["days_since"],
        "normalized_score": round(freshness["score"], 2),
        "data_source": freshness["source"],
    })
    
    # Transparency URL exists
    has_url = 1 if att.get("transparency_url") else 0
    components.append({
        "component_id": "transparency_url_exists",
        "category": "transparency",
        "raw_value": has_url,
        "normalized_score": 100 if has_url else 0,
        "data_source": "config",
    })
    
    return components


def _get_attestation_freshness(stablecoin_id: str, expected_freq_days: int) -> dict:
    """Check scraped attestation data for freshness."""
    # Look for scraped JSON in data directory
    scrape_dir = os.path.join(os.path.dirname(__file__), "..", "data", "scraped")
    pattern = os.path.join(scrape_dir, f"*{stablecoin_id}*.json")
    files = sorted(glob.glob(pattern), reverse=True)
    
    if files:
        try:
            with open(files[0]) as f:
                data = json.load(f)
            last_date_str = data.get("attestation_date") or data.get("scrape_date")
            if last_date_str:
                last_date = datetime.fromisoformat(last_date_str.replace("Z", "+00:00"))
                days_since = (datetime.now(timezone.utc) - last_date).days
                score = normalize_inverse_linear(days_since, 0, expected_freq_days * 2)
                return {"days_since": days_since, "score": score, "source": "scraped"}
        except Exception as e:
            logger.debug(f"Could not parse scraped data for {stablecoin_id}: {e}")
    
    # Fallback: use expected frequency as estimate
    return {
        "days_since": expected_freq_days,
        "score": normalize_inverse_linear(expected_freq_days, 0, 120),
        "source": "estimated",
    }


# =============================================================================
# Regulatory Components
# =============================================================================

REGULATORY_SCORES = {
    "NY BitLicense": 25,
    "NY Trust Company Charter": 25,
    "UK EMI": 20,
    "Singapore MPI": 20,
    "MiCA-compliant": 25,
    "El Salvador VASP": 10,
    "Hong Kong TCSP": 15,
    "NYDFS regulated": 25,
    "Decentralized - N/A": 10,
    "Various state licenses": 15,
}


def collect_regulatory_components(stablecoin_id: str) -> list[dict]:
    """Collect regulatory compliance components from config."""
    cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
    if not cfg:
        return []
    
    components = []
    
    # Regulatory License Score
    # Look up from the original full registry attestation config
    # For now, derive from attestation auditor quality as proxy
    att = cfg.get("attestation", {})
    auditor = att.get("auditor", "N/A").lower()
    tier = AUDITOR_TIERS.get(auditor, 4)
    
    # Regulatory score: regulated issuers get higher scores
    issuer = cfg.get("issuer", "")
    regulated_issuers = {"Circle": 90, "Paxos": 85, "Gemini": 80, "Archblock": 60}
    defi_issuers = {"MakerDAO": 55, "Frax Finance": 50, "TRON DAO": 40, "Ethena Labs": 45}
    
    reg_score = regulated_issuers.get(issuer, defi_issuers.get(issuer, 50))
    components.append({
        "component_id": "regulatory_status",
        "category": "regulatory",
        "raw_value": reg_score,
        "normalized_score": reg_score,
        "data_source": "config",
    })
    
    # Jurisdiction clarity
    jurisdiction_known = issuer not in ("", "Unknown")
    components.append({
        "component_id": "jurisdiction_clarity",
        "category": "regulatory",
        "raw_value": 1 if jurisdiction_known else 0,
        "normalized_score": 80 if jurisdiction_known else 30,
        "data_source": "config",
    })
    
    return components


# =============================================================================
# Governance Components
# =============================================================================

def collect_governance_components(stablecoin_id: str) -> list[dict]:
    """Collect governance components from config and on-chain proxy data."""
    cfg = STABLECOIN_REGISTRY.get(stablecoin_id)
    if not cfg:
        return []
    
    components = []
    issuer = cfg.get("issuer", "")
    
    # Governance model score
    centralized_issuers = {"Circle": 70, "Tether": 55, "Paxos": 70, "First Digital": 60, "Archblock": 55}
    dao_issuers = {"MakerDAO": 80, "Frax Finance": 75, "TRON DAO": 50, "Ethena Labs": 60}
    
    gov_score = centralized_issuers.get(issuer, dao_issuers.get(issuer, 50))
    components.append({
        "component_id": "governance_model",
        "category": "governance",
        "raw_value": gov_score,
        "normalized_score": gov_score,
        "data_source": "config",
    })
    
    # Team transparency
    known_teams = {"Circle", "Paxos", "MakerDAO", "Frax Finance"}
    team_known = issuer in known_teams
    components.append({
        "component_id": "team_transparency",
        "category": "governance",
        "raw_value": 1 if team_known else 0,
        "normalized_score": 85 if team_known else 50,
        "data_source": "config",
    })
    
    return components


# =============================================================================
# Reserve Quality Components
# =============================================================================

# Static reserve data (updated periodically from attestation scraping)
RESERVE_PROFILES = {
    "usdc": {"reserve_ratio": 1.01, "cash_pct": 95, "tbill_pct": 90},
    "usdt": {"reserve_ratio": 1.00, "cash_pct": 80, "tbill_pct": 75},
    "dai": {"reserve_ratio": 1.50, "cash_pct": 40, "tbill_pct": 30},
    "frax": {"reserve_ratio": 1.00, "cash_pct": 70, "tbill_pct": 60},
    "pyusd": {"reserve_ratio": 1.00, "cash_pct": 95, "tbill_pct": 85},
    "fdusd": {"reserve_ratio": 1.00, "cash_pct": 90, "tbill_pct": 80},
    "tusd": {"reserve_ratio": 1.00, "cash_pct": 85, "tbill_pct": 70},
    "usdd": {"reserve_ratio": 3.00, "cash_pct": 10, "tbill_pct": 0},
    "usde": {"reserve_ratio": 1.01, "cash_pct": 0, "tbill_pct": 0},
}


def collect_reserve_components(stablecoin_id: str) -> list[dict]:
    """Collect reserve quality components from static profiles."""
    profile = RESERVE_PROFILES.get(stablecoin_id)
    if not profile:
        return []
    
    components = []
    
    # Reserve-to-Supply Ratio
    ratio = profile["reserve_ratio"]
    components.append({
        "component_id": "reserve_to_supply_ratio",
        "category": "transparency",
        "raw_value": ratio,
        "normalized_score": round(normalize_linear(ratio, 0.98, 1.02), 2),
        "data_source": "attestation",
    })
    
    # Cash Equivalents Percentage
    cash_pct = profile["cash_pct"]
    components.append({
        "component_id": "cash_equivalents_pct",
        "category": "transparency",
        "raw_value": cash_pct,
        "normalized_score": round(normalize_direct(cash_pct), 2),
        "data_source": "attestation",
    })
    
    return components


# =============================================================================
# Network Components (static — chain analysis)
# =============================================================================

def collect_network_components(stablecoin_id: str) -> list[dict]:
    """Collect network risk components."""
    components = []
    
    # Primary chain = Ethereum for all tracked stablecoins
    components.append({
        "component_id": "primary_chain_security",
        "category": "network",
        "raw_value": 95,  # Ethereum mainnet
        "normalized_score": 95,
        "data_source": "static",
    })
    
    # Contract verification (all tracked coins are verified)
    components.append({
        "component_id": "contract_verified",
        "category": "network",
        "raw_value": 1,
        "normalized_score": 100,
        "data_source": "static",
    })
    
    return components
