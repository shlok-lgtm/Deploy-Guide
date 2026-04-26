"""
Vault/Yield Strategy Risk Index Collector
===========================================
Collects data for vault and yield strategy risk scoring.

Data sources:
- DeFiLlama /yields/pools: APY, TVL, pool metadata (already integrated)
- DeFiLlama /yields/chart/{pool}: historical APY
- Existing CQI scores for underlying asset quality
- Static config: audit status, strategy transparency, operational risk

Key insight: "Underlying Asset Quality" category reads from existing
SII/PSI scores via CQI lookup — not re-derived.
"""

import json
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one
from app.index_definitions.vsri_v01 import VSRI_V01_DEFINITION, VAULT_ENTITIES
from app.scoring_engine import score_entity
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"
ETHERSCAN_V2_BASE = "https://api.etherscan.io/v2/api"


# =============================================================================
# Phase 3C: VSRI Documentation Quality Scoring
# Follows app/rpi/docs_scorer.py pattern — keyword rubric against docs sites
# =============================================================================

VAULT_DOCS = {
    "yearn-usdc": "https://docs.yearn.fi",
    "yearn-dai": "https://docs.yearn.fi",
    "yearn-eth": "https://docs.yearn.fi",
    "morpho-usdc-aave": "https://docs.morpho.org",
    "morpho-eth-aave": "https://docs.morpho.org",
    "beefy-usdc-eth": "https://docs.beefy.finance",
    "beefy-usdt-usdc": "https://docs.beefy.finance",
    "pendle-steth": "https://docs.pendle.finance",
    "pendle-eeth": "https://docs.pendle.finance",
    "sommelier-turbo-steth": "https://docs.sommelier.finance",
}

VAULT_DOCS_RUBRIC = {
    "strategy_description": {
        "label": "Strategy description available",
        "keywords": [
            "strategy", "how it works", "yield source", "deposit", "allocation",
            "vault strategy", "strategy description", "earning mechanism",
            "yield generation", "vault overview",
        ],
        "paths": ["/vaults", "/strategies", "/products", "/how-it-works", "/overview"],
    },
    "parameter_documentation": {
        "label": "Parameters documented",
        "keywords": [
            "parameters", "fees", "management fee", "performance fee", "deposit limit",
            "allocation target", "rebalance threshold", "withdrawal fee",
            "fee structure", "protocol fees",
        ],
        "paths": ["/fees", "/parameters", "/protocol-fees", "/vaults/fees", "/tokenomics"],
    },
    "rebalance_logic": {
        "label": "Rebalance logic documented",
        "keywords": [
            "rebalance", "harvest", "strategy migration", "allocation change",
            "auto-compound", "yield optimization", "rebalancing", "compounding",
            "harvest frequency",
        ],
        "paths": ["/strategies", "/rebalancing", "/harvesting", "/mechanics"],
    },
    "risk_disclosure": {
        "label": "Risk disclosure published",
        "keywords": [
            "risk", "impermanent loss", "smart contract risk", "protocol risk",
            "liquidation", "slippage", "counterparty risk", "audit",
            "security", "risk factors", "disclaimer",
        ],
        "paths": ["/risks", "/security", "/safety", "/risk-disclosure", "/audits"],
    },
}

# In-memory cache for docs scoring (24h TTL — docs change slowly)
_vault_docs_cache: dict[str, tuple[float, dict]] = {}
_VAULT_DOCS_CACHE_TTL = 86400  # 24 hours


def _fetch_page_text(url: str) -> str | None:
    """Fetch a web page and return cleaned text content."""
    try:
        resp = requests.get(url, timeout=15, allow_redirects=True)
        if resp.status_code != 200:
            return None
        text = resp.text
        # Strip HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text.lower()
    except Exception as e:
        logger.debug(f"VSRI docs fetch failed for {url}: {e}")
        return None


def _score_vault_criterion(docs_url: str, criterion: dict) -> tuple[float, str | None, str | None]:
    """Score a single rubric criterion against a docs site.

    Returns (score, evidence_url, evidence_snippet).
    Score: 25 (full match), 12 (partial), 0 (no match).
    """
    keywords = criterion["keywords"]
    paths = criterion.get("paths", [])

    # Check main docs page
    main_text = _fetch_page_text(docs_url)
    if main_text:
        matches = [kw for kw in keywords if kw.lower() in main_text]
        if len(matches) >= 2:
            snippet = matches[0]
            return 25.0, docs_url, f"Found: {', '.join(matches[:3])}"

    # Check specific subpaths
    for path in paths:
        url = docs_url.rstrip("/") + path
        text = _fetch_page_text(url)
        if text:
            matches = [kw for kw in keywords if kw.lower() in text]
            if matches:
                return 25.0, url, f"Found: {', '.join(matches[:3])}"
        time.sleep(0.5)

    # Partial credit if main page has at least 1 keyword
    if main_text:
        matches = [kw for kw in keywords if kw.lower() in main_text]
        if matches:
            return 12.0, docs_url, f"Partial: {matches[0]}"

    return 0.0, None, None


def score_vault_docs(entity_slug: str) -> dict:
    """Score vault documentation quality against a 4-criterion rubric.

    Returns dict of {component_id: score_0_to_100} for:
    - strategy_description_avail
    - parameter_visibility
    - rebalance_logic_documented
    - risk_disclosure

    Results cached 24h and stored in rpi_doc_scores table.
    """
    # Check cache
    cached = _vault_docs_cache.get(entity_slug)
    if cached and (time.time() - cached[0]) < _VAULT_DOCS_CACHE_TTL:
        return cached[1]

    docs_url = VAULT_DOCS.get(entity_slug)
    if not docs_url:
        return {}

    results = {}
    component_map = {
        "strategy_description": "strategy_description_avail",
        "parameter_documentation": "parameter_visibility",
        "rebalance_logic": "rebalance_logic_documented",
        "risk_disclosure": "risk_disclosure",
    }

    for criterion_id, criterion_def in VAULT_DOCS_RUBRIC.items():
        score, evidence_url, snippet = _score_vault_criterion(docs_url, criterion_def)
        component_id = component_map[criterion_id]

        # Normalize: 0-25 criterion score → 0-100 component score
        normalized = min(100, score * 4)
        results[component_id] = normalized

        # Store evidence in rpi_doc_scores (reuse same table)
        try:
            execute("""
                INSERT INTO rpi_doc_scores
                    (protocol_slug, criterion, score, evidence_url, evidence_snippet, scored_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (protocol_slug, criterion) DO UPDATE SET
                    score = EXCLUDED.score,
                    evidence_url = EXCLUDED.evidence_url,
                    evidence_snippet = EXCLUDED.evidence_snippet,
                    scored_at = NOW()
            """, (entity_slug, criterion_id, score, evidence_url, snippet))
        except Exception as e:
            logger.debug(f"VSRI docs evidence store failed: {e}")

    _vault_docs_cache[entity_slug] = (time.time(), results)
    if results:
        logger.info(
            f"VSRI docs scoring {entity_slug}: "
            + " ".join(f"{k}={v:.0f}" for k, v in results.items())
        )
    return results

# =============================================================================
# Static config
# =============================================================================

VAULT_STATIC_CONFIG = {
    "yearn-usdc": {
        "strategy_description_avail": 80, "strategy_code_public": 90,
        "parameter_visibility": 75, "rebalance_logic_documented": 70,
        "risk_disclosure": 65, "il_exposure": 90,
        "withdrawal_delay": 90, "position_liquidity": 80,
        "vault_audit_status": 5, "vault_contract_age_days": 1200,
        "vault_upgrade_mechanism": 65, "dependency_chain_depth": 2,
        "composability_risk": 60, "collateral_diversity": 50,
        "correlation_risk": 70, "curator_track_record": 80,
        "rebalance_frequency": 70, "strategy_change_history": 3,
        "vault_incident_history": 75, "fee_transparency": 80,
    },
    "yearn-dai": {
        "strategy_description_avail": 80, "strategy_code_public": 90,
        "parameter_visibility": 75, "rebalance_logic_documented": 70,
        "risk_disclosure": 65, "il_exposure": 90,
        "withdrawal_delay": 90, "position_liquidity": 80,
        "vault_audit_status": 5, "vault_contract_age_days": 1200,
        "vault_upgrade_mechanism": 65, "dependency_chain_depth": 2,
        "composability_risk": 60, "collateral_diversity": 50,
        "correlation_risk": 70, "curator_track_record": 80,
        "rebalance_frequency": 70, "strategy_change_history": 3,
        "vault_incident_history": 75, "fee_transparency": 80,
    },
    "yearn-eth": {
        "strategy_description_avail": 80, "strategy_code_public": 90,
        "parameter_visibility": 75, "rebalance_logic_documented": 70,
        "risk_disclosure": 65, "il_exposure": 85,
        "withdrawal_delay": 85, "position_liquidity": 75,
        "vault_audit_status": 5, "vault_contract_age_days": 1000,
        "vault_upgrade_mechanism": 65, "dependency_chain_depth": 2,
        "composability_risk": 55, "collateral_diversity": 45,
        "correlation_risk": 65, "curator_track_record": 80,
        "rebalance_frequency": 70, "strategy_change_history": 4,
        "vault_incident_history": 75, "fee_transparency": 80,
    },
    "morpho-usdc-aave": {
        "strategy_description_avail": 75, "strategy_code_public": 85,
        "parameter_visibility": 70, "rebalance_logic_documented": 65,
        "risk_disclosure": 60, "il_exposure": 95,
        "withdrawal_delay": 85, "position_liquidity": 80,
        "vault_audit_status": 4, "vault_contract_age_days": 500,
        "vault_upgrade_mechanism": 60, "dependency_chain_depth": 3,
        "composability_risk": 50, "collateral_diversity": 40,
        "correlation_risk": 75, "curator_track_record": 70,
        "rebalance_frequency": 65, "strategy_change_history": 1,
        "vault_incident_history": 90, "fee_transparency": 75,
    },
    "morpho-eth-aave": {
        "strategy_description_avail": 75, "strategy_code_public": 85,
        "parameter_visibility": 70, "rebalance_logic_documented": 65,
        "risk_disclosure": 60, "il_exposure": 90,
        "withdrawal_delay": 85, "position_liquidity": 75,
        "vault_audit_status": 4, "vault_contract_age_days": 500,
        "vault_upgrade_mechanism": 60, "dependency_chain_depth": 3,
        "composability_risk": 50, "collateral_diversity": 40,
        "correlation_risk": 70, "curator_track_record": 70,
        "rebalance_frequency": 65, "strategy_change_history": 1,
        "vault_incident_history": 90, "fee_transparency": 75,
    },
    "beefy-usdc-eth": {
        "strategy_description_avail": 70, "strategy_code_public": 80,
        "parameter_visibility": 65, "rebalance_logic_documented": 60,
        "risk_disclosure": 55, "il_exposure": 50,
        "withdrawal_delay": 80, "position_liquidity": 65,
        "vault_audit_status": 3, "vault_contract_age_days": 800,
        "vault_upgrade_mechanism": 55, "dependency_chain_depth": 3,
        "composability_risk": 45, "collateral_diversity": 50,
        "correlation_risk": 55, "curator_track_record": 65,
        "rebalance_frequency": 75, "strategy_change_history": 5,
        "vault_incident_history": 70, "fee_transparency": 70,
    },
    "beefy-usdt-usdc": {
        "strategy_description_avail": 70, "strategy_code_public": 80,
        "parameter_visibility": 65, "rebalance_logic_documented": 60,
        "risk_disclosure": 55, "il_exposure": 95,
        "withdrawal_delay": 80, "position_liquidity": 75,
        "vault_audit_status": 3, "vault_contract_age_days": 800,
        "vault_upgrade_mechanism": 55, "dependency_chain_depth": 3,
        "composability_risk": 45, "collateral_diversity": 40,
        "correlation_risk": 80, "curator_track_record": 65,
        "rebalance_frequency": 75, "strategy_change_history": 3,
        "vault_incident_history": 70, "fee_transparency": 70,
    },
}


# =============================================================================
# DeFiLlama yield pool data
# =============================================================================

def fetch_yield_pools() -> list[dict]:
    """Fetch all yield pools from DeFiLlama."""
    try:
        resp = requests.get(f"https://yields.llama.fi/pools", timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("data", [])
    except Exception as e:
        logger.warning(f"DeFiLlama yields fetch failed: {e}")
    return []


def match_vault_pools(entity: dict, all_pools: list[dict]) -> list[dict]:
    """Match a vault entity to DeFiLlama yield pools."""
    protocol = entity.get("protocol", "").lower()
    slug = entity["slug"]

    matched = []
    for pool in all_pools:
        pool_project = (pool.get("project") or "").lower()
        pool_symbol = (pool.get("symbol") or "").lower()

        if protocol in pool_project:
            # Match by token symbol hints in the slug
            if "usdc" in slug and "usdc" in pool_symbol:
                matched.append(pool)
            elif "dai" in slug and "dai" in pool_symbol:
                matched.append(pool)
            elif "eth" in slug and "eth" in pool_symbol and "steth" not in slug:
                matched.append(pool)
            elif "usdt" in slug and "usdt" in pool_symbol:
                matched.append(pool)
            elif "steth" in slug and "steth" in pool_symbol:
                matched.append(pool)
            elif "eeth" in slug and "eeth" in pool_symbol:
                matched.append(pool)

    # Sort by TVL and take top match
    matched.sort(key=lambda p: p.get("tvlUsd", 0), reverse=True)
    return matched[:3]


# =============================================================================
# Phase 1: Live data automation for static components
# =============================================================================

def _automate_vault_contract_age(entity: dict, static: dict) -> dict:
    """Fetch vault contract age via Etherscan V2 first transaction lookup."""
    automated = {}
    contract = entity.get("contract")
    if not contract:
        return automated

    api_key = os.environ.get("ETHERSCAN_API_KEY", "")
    if not api_key:
        return automated

    try:
        time.sleep(0.15)
        resp = requests.get(ETHERSCAN_V2_BASE, params={
            "chainid": 1,
            "module": "account",
            "action": "txlist",
            "address": contract,
            "startblock": 0,
            "endblock": 99999999,
            "page": 1,
            "offset": 1,
            "sort": "asc",
            "apikey": api_key,
        }, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            txs = data.get("result", [])
            if isinstance(txs, list) and txs:
                first_ts = int(txs[0].get("timeStamp", 0))
                if first_ts > 0:
                    first_date = datetime.fromtimestamp(first_ts, tz=timezone.utc)
                    age_days = (datetime.now(timezone.utc) - first_date).days
                    static_age = static.get("vault_contract_age_days", 0)
                    automated["vault_contract_age_days"] = max(age_days, static_age)
    except Exception as e:
        logger.debug(f"VSRI contract age failed for {entity['slug']}: {e}")

    return automated


def _automate_vault_smart_contract(entity: dict, static: dict) -> dict:
    """Automate vault_audit_status and vault_upgrade_mechanism using smart contract analysis."""
    automated = {}
    contract = entity.get("contract")
    if not contract:
        return automated

    try:
        from app.collectors.smart_contract import analyze_contract_for_index_sync
        analysis = analyze_contract_for_index_sync(contract)

        # vault_audit_status: log normalization {1:30, 2:50, 3:70, 5:85, 10:100}
        # Raw value is audit count. Verification = base of 3.
        static_audit = static.get("vault_audit_status", 1)
        if analysis.get("audit_verified"):
            live_audit = 3
            if analysis.get("is_proxy") and analysis.get("implementation_verified"):
                live_audit = 5
            automated["vault_audit_status"] = max(live_audit, static_audit)
        else:
            automated["vault_audit_status"] = static_audit

        # vault_upgrade_mechanism: from proxy detection (0-100, direct normalization)
        live_upgrade = analysis.get("upgradeability_risk", 50)
        static_upgrade = static.get("vault_upgrade_mechanism", 50)
        automated["vault_upgrade_mechanism"] = max(live_upgrade, static_upgrade)
    except Exception as e:
        logger.warning(f"VSRI smart contract automation failed for {entity['slug']}: {e}")

    return automated


def _automate_vault_dependency_depth(entity: dict, static: dict, matched_pools: list[dict]) -> dict:
    """Derive dependency_chain_depth from DeFiLlama yields pool metadata."""
    automated = {}

    if not matched_pools:
        return automated

    best = matched_pools[0]
    underlying = best.get("underlyingTokens") or []
    chain = best.get("chain", "")

    # Count dependency layers:
    # 1 = direct asset (simple vault)
    # 2 = vault on a lending protocol (one layer)
    # 3 = vault on a vault on a lending protocol
    depth = 1
    if underlying:
        depth = max(1, len(underlying))
    # If the pool is a "metapool" or references another pool, add depth
    pool_symbol = (best.get("symbol") or "").lower()
    if any(kw in pool_symbol for kw in ["meta", "vault", "yield", "leverage"]):
        depth += 1

    if depth > 0:
        static_depth = static.get("dependency_chain_depth", 1)
        automated["dependency_chain_depth"] = max(depth, static_depth)

    return automated


def _automate_vault_incident_history(entity: dict, static: dict, hacks_cache: list = None) -> dict:
    """Automate vault_incident_history from DeFiLlama hacks data."""
    automated = {}

    try:
        from app.collectors.defillama import (
            fetch_defillama_hacks, filter_hacks_by_name, score_exploit_history_from_hacks,
        )
        hacks = hacks_cache if hacks_cache is not None else fetch_defillama_hacks()
        protocol = entity.get("protocol", "")
        matched = filter_hacks_by_name(hacks, protocol)
        if not matched:
            matched = filter_hacks_by_name(hacks, entity["slug"].split("-")[0])

        live_score = score_exploit_history_from_hacks(matched)
        static_score = static.get("vault_incident_history", 100)
        automated["vault_incident_history"] = min(live_score, static_score)
    except Exception as e:
        logger.warning(f"VSRI incident history failed for {entity['slug']}: {e}")

    return automated


def _automate_vault_withdrawal_delay(entity: dict, static: dict, matched_pools: list[dict]) -> dict:
    """Derive withdrawal_delay from DeFiLlama yields pool metadata."""
    automated = {}

    if not matched_pools:
        return automated

    best = matched_pools[0]
    # Some pools have lockup or withdrawal period info
    # Check for common metadata fields
    exposure = best.get("exposure", "")
    pool_meta = best.get("poolMeta") or ""

    # If pool indicates no lock: score high
    # If pool has lockup indicators: score lower
    if "no lock" in str(pool_meta).lower() or "instant" in str(pool_meta).lower():
        automated["withdrawal_delay"] = 95
    elif "lock" in str(pool_meta).lower():
        automated["withdrawal_delay"] = 50
    # Otherwise keep static value

    return automated


def extract_vault_raw_values(entity: dict, all_pools: list[dict]) -> dict:
    """Extract raw component values from DeFiLlama yield data + static config."""
    raw = {}

    # Match pools
    matched_pools = match_vault_pools(entity, all_pools)
    if matched_pools:
        best = matched_pools[0]
        raw["vault_tvl"] = best.get("tvlUsd", 0)
        raw["apy_7d"] = best.get("apyBase7d") or best.get("apy", 0)
        raw["apy_30d"] = best.get("apyBase30d") or best.get("apyMean30d") or best.get("apy", 0)

        # APY volatility from standard deviation
        apy_std = best.get("apyBaseInception") or 0
        if apy_std and isinstance(apy_std, (int, float)):
            raw["apy_volatility"] = abs(apy_std)

    # CQI lookup for underlying asset quality
    try:
        # Look up SII score for the underlying stablecoin
        if "usdc" in entity["slug"]:
            sii_row = fetch_one("SELECT overall_score FROM scores WHERE stablecoin_id = 'usdc'")
            if sii_row:
                raw["underlying_sii_score"] = float(sii_row["overall_score"])
        elif "dai" in entity["slug"]:
            sii_row = fetch_one("SELECT overall_score FROM scores WHERE stablecoin_id = 'dai'")
            if sii_row:
                raw["underlying_sii_score"] = float(sii_row["overall_score"])

        # Look up PSI score for the underlying protocol
        protocol_slug = entity.get("protocol")
        if protocol_slug:
            psi_row = fetch_one(
                "SELECT overall_score FROM psi_scores WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1",
                (protocol_slug,),
            )
            if psi_row:
                raw["underlying_psi_score"] = float(psi_row["overall_score"])
    except Exception as e:
        logger.debug(f"CQI lookup failed for {entity['slug']}: {e}")

    # Static config
    static = VAULT_STATIC_CONFIG.get(entity["slug"], {})
    raw.update(static)

    return raw


# =============================================================================
# Score and store
# =============================================================================

def score_vault(entity: dict, all_pools: list[dict], hacks_cache: list = None) -> dict | None:
    """Score a single vault entity."""
    slug = entity["slug"]
    logger.info(f"Scoring vault: {slug}")

    raw_values = extract_vault_raw_values(entity, all_pools)
    if not raw_values:
        logger.warning(f"No data collected for vault {slug}")
        return None

    static = VAULT_STATIC_CONFIG.get(slug, {})
    matched_pools = match_vault_pools(entity, all_pools)

    # --- Phase 1 automation: replace static with live data ---
    # Contract age from Etherscan
    age_automated = _automate_vault_contract_age(entity, static)
    raw_values.update(age_automated)

    # Audit status and upgrade mechanism from smart contract analysis
    sc_automated = _automate_vault_smart_contract(entity, static)
    raw_values.update(sc_automated)

    # Dependency chain depth from DeFiLlama yields metadata
    depth_automated = _automate_vault_dependency_depth(entity, static, matched_pools)
    raw_values.update(depth_automated)

    # Incident history from DeFiLlama hacks
    incident_automated = _automate_vault_incident_history(entity, static, hacks_cache)
    raw_values.update(incident_automated)

    # Withdrawal delay from DeFiLlama yields metadata
    delay_automated = _automate_vault_withdrawal_delay(entity, static, matched_pools)
    raw_values.update(delay_automated)

    # --- Phase 3C: Documentation quality scoring ---
    try:
        docs_scores = score_vault_docs(slug)
        if docs_scores:
            for comp_id, live_score in docs_scores.items():
                static_score = static.get(comp_id, 0)
                raw_values[comp_id] = max(live_score, static_score)
    except Exception as e:
        logger.debug(f"VSRI docs scoring failed for {slug}: {e}")

    result = score_entity(VSRI_V01_DEFINITION, raw_values)
    result["entity_slug"] = slug
    result["entity_name"] = entity["name"]
    result["raw_values"] = raw_values

    return result


def store_vault_score(result: dict) -> None:
    """Store a vault score in the generic_index_scores table."""
    slug = result["entity_slug"]
    raw_for_storage = {k: v for k, v in result["raw_values"].items() if not k.startswith("_")}
    raw_canonical = json.dumps(raw_for_storage, sort_keys=True, default=str)
    inputs_hash = "0x" + hashlib.sha256(raw_canonical.encode()).hexdigest()

    execute("""
        INSERT INTO generic_index_scores
            (index_id, entity_slug, entity_name, overall_score,
             category_scores, component_scores, raw_values,
             formula_version, inputs_hash, confidence, confidence_tag,
             component_coverage, components_populated, components_total, missing_categories)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (index_id, entity_slug, scored_date)
        DO UPDATE SET
            entity_name = EXCLUDED.entity_name,
            overall_score = EXCLUDED.overall_score,
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
        "vsri", slug, result["entity_name"], result["overall_score"],
        json.dumps(result["category_scores"]),
        json.dumps(result["component_scores"]),
        json.dumps(raw_for_storage, default=str),
        result["version"], inputs_hash,
        result.get("confidence", "limited"),
        result.get("confidence_tag"),
        result.get("component_coverage"),
        result.get("components_populated"),
        result.get("components_total"),
        json.dumps(result.get("missing_categories") or []),
    ))


def run_vsri_scoring() -> list[dict]:
    """Score all vault entities. Called from worker."""
    all_pools = fetch_yield_pools()
    time.sleep(1)

    # Pre-fetch DeFiLlama hacks data (cached 24h, shared across all entities)
    hacks_cache = []
    try:
        from app.collectors.defillama import fetch_defillama_hacks
        hacks_cache = fetch_defillama_hacks()
    except Exception as e:
        logger.warning(f"VSRI hacks pre-fetch failed: {e}")

    results = []
    for entity in VAULT_ENTITIES:
        try:
            result = score_vault(entity, all_pools, hacks_cache=hacks_cache)
            if result:
                store_vault_score(result)
                results.append(result)
                logger.info(
                    f"  {result['entity_name']}: {result['overall_score']} "
                    f"({result['components_available']}/{result['components_total']} components)"
                )
        except Exception as e:
            logger.warning(f"VSRI scoring failed for {entity['slug']}: {e}")

    # Attest VSRI scores
    try:
        from app.state_attestation import attest_state
        if results:
            attest_state("vsri_components", [
                {"slug": r["entity_slug"], "score": r["overall_score"]}
                for r in results
            ])
    except Exception as e:
        logger.warning(f"VSRI attestation failed: {e}")

    return results
