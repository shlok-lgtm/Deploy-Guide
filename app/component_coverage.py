"""
Component Coverage Analysis
=============================
Classifies every component in each index definition as LIVE, STATIC,
DERIVED, or EMPTY based on how the collector populates it.

Used by /api/{index_id}/coverage and ops dashboards.
"""

# Per-index component classification.
# Keys are component_id from the index definition.
# Values are one of: "live", "static", "derived", "empty"

COVERAGE = {
    "lsti": {
        # Peg Stability — CoinGecko
        "eth_peg_deviation": "live",
        "peg_volatility_7d": "live",
        "peg_volatility_30d": "live",
        "dex_cex_spread": "live",
        "exchange_price_variance": "live",
        # Liquidity — CoinGecko + DeFiLlama
        "market_cap": "live",
        "dex_pool_depth": "live",
        "volume_cap_ratio": "live",
        "slippage_1m": "empty",
        "cross_chain_liquidity": "live",
        # Validator/Operator — Rated.network
        "validator_count": "live",
        "operator_diversity_hhi": "live",
        "slashing_history": "empty",
        "attestation_rate": "live",
        "slashing_insurance": "static",
        # Distribution — needs Etherscan integration
        "top_holder_concentration": "empty",
        "holder_gini": "empty",
        "defi_protocol_share": "empty",
        "exchange_concentration": "empty",
        # Smart Contract — static config
        "audit_status": "static",
        "upgradeability_risk": "static",
        "admin_key_risk": "static",
        "withdrawal_queue_impl": "static",
        "exploit_history_lst": "static",
        # Network/Withdrawal — needs protocol API integration
        "withdrawal_queue_length": "empty",
        "avg_withdrawal_time": "empty",
        "withdrawal_success_rate": "empty",
        "beacon_chain_dependency": "static",
        "mev_exposure": "static",
    },
    "bri": {
        # Security Architecture — static
        "verification_mechanism": "static",
        "guardian_count": "static",
        "guardian_diversity": "static",
        "bridge_upgrade_mechanism": "static",
        "bridge_timelock": "static",
        "bridge_audit_count": "static",
        # Operational History — DeFiLlama + static
        "total_value_transferred": "live",
        "uptime_pct": "static",
        "message_success_rate": "static",
        "incident_history": "static",
        "time_since_incident_days": "static",
        # Liquidity & Throughput — DeFiLlama
        "bridge_tvl": "live",
        "daily_volume": "live",
        "volume_tvl_ratio": "derived",
        "supported_chains": "live",
        "token_coverage": "static",
        # Smart Contract Risk — static
        "bridge_formal_verification": "static",
        "bug_bounty_size": "static",
        "contract_age_days": "static",
        "bridge_dependency_risk": "static",
        "code_complexity": "static",
        # Decentralization — static
        "operator_geographic_diversity": "static",
        "validator_rotation": "static",
        "bridge_governance_mechanism": "static",
        "token_holder_concentration": "empty",
        # Economic Security — static
        "cost_to_attack": "static",
        "slashing_mechanism": "static",
        "bridge_insurance": "static",
        "restaking_security": "static",
    },
    "dohi": {
        # Governance Activity — Snapshot
        "proposal_frequency_90d": "live",
        "voter_participation_rate": "live",
        "quorum_achievement_rate": "live",
        "proposal_pass_rate": "live",
        "delegate_count": "live",
        # Governance Concentration — Snapshot
        "top10_voter_share": "live",
        "delegate_concentration_hhi": "empty",
        "voting_power_gini": "live",
        "min_coalition_pct": "empty",
        # Operational Continuity — static
        "active_contributor_count": "static",
        "key_personnel_diversity": "static",
        "legal_entity_status": "static",
        "multisig_config": "static",
        # Treasury Management — DeFiLlama + static
        "treasury_size_usd": "live",
        "treasury_runway_months": "static",
        "treasury_diversification": "live",
        "treasury_growth_trend": "live",
        # Security Posture — static
        "dao_timelock_hours": "static",
        "emergency_capability": "static",
        "guardian_authority": "static",
        "dao_upgrade_mechanism": "static",
        "dao_audit_cadence": "static",
        # Transparency — static
        "public_reporting_frequency": "static",
        "financial_disclosure": "static",
        "compensation_transparency": "static",
        "meeting_cadence": "static",
    },
    "vsri": {
        # Strategy Transparency — static
        "strategy_description_avail": "static",
        "strategy_code_public": "static",
        "parameter_visibility": "static",
        "rebalance_logic_documented": "static",
        "risk_disclosure": "static",
        # Performance & Volatility — DeFiLlama
        "apy_7d": "live",
        "apy_30d": "live",
        "apy_volatility": "live",
        "max_drawdown": "empty",
        "il_exposure": "static",
        # Liquidity Risk — DeFiLlama + static
        "vault_tvl": "live",
        "withdrawal_delay": "static",
        "deposit_concentration": "static",
        "position_liquidity": "static",
        # Smart Contract Risk — static
        "vault_audit_status": "static",
        "vault_contract_age_days": "static",
        "vault_upgrade_mechanism": "static",
        "dependency_chain_depth": "static",
        "composability_risk": "static",
        # Underlying Asset Quality — CQI lookup
        "underlying_sii_score": "derived",
        "underlying_psi_score": "derived",
        "collateral_diversity": "static",
        "correlation_risk": "static",
        # Operational Risk — static
        "curator_track_record": "static",
        "rebalance_frequency": "static",
        "strategy_change_history": "static",
        "vault_incident_history": "static",
        "fee_transparency": "static",
    },
    "cxri": {
        # Reserve Proof Quality — static
        "por_method": "static",
        "por_frequency": "static",
        "por_recency_days": "static",
        "auditor_reputation": "static",
        "liabilities_included": "static",
        "negative_balance_detection": "static",
        # Reserve Composition — static (DeFiLlama planned)
        "reserve_asset_diversity": "live",
        "stablecoin_reserve_pct": "static",
        "native_token_pct": "static",
        "quality_asset_pct": "static",
        "unlabeled_asset_pct": "static",
        # Regulatory Status — static
        "license_count": "static",
        "mica_status": "static",
        "us_licensing": "static",
        "enforcement_history": "static",
        "jurisdiction_quality": "static",
        # Operational Track Record — CoinGecko + static
        "years_in_operation": "live",
        "withdrawal_freeze_count": "static",
        "security_breach_count": "static",
        "insurance_coverage": "static",
        "fund_segregation": "static",
        # Transparency — static
        "public_audit_reports": "static",
        "realtime_reserve_dashboard": "static",
        "api_availability": "static",
        "corporate_disclosure": "static",
        # On-Chain Signals — CoinGecko + needs Etherscan
        "known_wallet_balance": "live",
        "hot_cold_ratio": "empty",
        "large_withdrawal_zscore": "empty",
        "unusual_outflow_score": "empty",
    },
    "tti": {
        # Underlying Asset Quality — static
        "treasury_duration": "static",
        "credit_quality": "static",
        "yield_benchmark_spread": "static",
        "asset_concentration": "static",
        "sovereign_risk": "static",
        "maturity_profile": "static",
        "interest_rate_sensitivity": "static",
        "liquidity_of_underlying": "static",
        "currency_risk": "static",
        "reinvestment_risk": "static",
        # Reserve & Collateral Verification — static
        "attestation_frequency": "static",
        "attestation_recency_days": "static",
        "auditor_quality": "static",
        "reserve_coverage_ratio": "static",
        "collateral_segregation": "static",
        "custodian_quality": "static",
        "onchain_verification": "empty",
        "bankruptcy_remoteness": "static",
        "rehypothecation_risk": "static",
        "collateral_composition_disclosure": "static",
        # NAV & Pricing — CoinGecko + static
        "nav_deviation": "live",
        "nav_update_frequency": "static",
        "pricing_methodology": "static",
        "oracle_integration": "static",
        "nav_volatility_30d": "empty",
        "mark_to_market_accuracy": "static",
        "pricing_source_diversity": "static",
        "accrual_mechanism": "static",
        # Redemption & Liquidity — static + DeFiLlama
        "redemption_window": "static",
        "settlement_time_hours": "static",
        "min_redemption_amount": "static",
        "tti_tvl": "live",
        "secondary_market_liquidity": "static",
        "gate_mechanism": "static",
        "redemption_fee": "static",
        "instant_liquidity_pct": "static",
        "queue_depth": "static",
        "cross_chain_availability": "empty",
        # Smart Contract & Infrastructure — static
        "tti_contract_audit": "static",
        "tti_upgradeability": "static",
        "tti_admin_key_risk": "static",
        "access_control": "static",
        "compliance_module": "static",
        "oracle_dependency": "static",
        "tti_contract_age_days": "empty",
        "chain_infrastructure": "static",
        "tti_bug_bounty": "static",
        "minting_mechanism": "static",
        "emergency_mechanism": "static",
        "dependency_risk": "static",
        # Issuer & Counterparty Risk — static
        "issuer_regulatory_status": "static",
        "issuer_track_record": "static",
        "issuer_aum": "static",
        "counterparty_count": "static",
        "bank_partner_quality": "static",
        "insurance_coverage_tti": "static",
        "conflict_of_interest": "static",
        "operational_continuity_tti": "static",
        "key_person_risk": "static",
        # Regulatory & Compliance — static
        "securities_registration": "static",
        "investor_accreditation": "static",
        "kyc_aml_compliance": "static",
        "transfer_restrictions": "static",
        "tax_reporting": "static",
        "prospectus_availability": "static",
        "jurisdiction_risk_tti": "static",
        "regulatory_change_risk": "static",
        # Holder & Distribution — needs Etherscan
        "tti_holder_count": "empty",
        "tti_top10_concentration": "empty",
        "institutional_holder_pct": "static",
        "defi_integration_count": "empty",
        "geographic_distribution": "static",
        "holder_growth_rate": "empty",
        # Market & Trading — CoinGecko
        "tti_volume_24h": "live",
        "tti_market_cap": "live",
        "exchange_listing_count": "live",
    },
}


def get_coverage(index_id: str) -> dict | None:
    """Get component coverage breakdown for an index.

    Returns dict with total, live, static, derived, empty counts
    and per-category breakdown, or None if index_id not recognized.
    """
    classification = COVERAGE.get(index_id)
    if classification is None:
        return None

    # Load index definition for category info
    definitions = _load_definitions()
    defn = definitions.get(index_id)
    if not defn:
        return None

    # Count by status
    live = sum(1 for v in classification.values() if v == "live")
    static = sum(1 for v in classification.values() if v == "static")
    derived = sum(1 for v in classification.values() if v == "derived")
    empty = sum(1 for v in classification.values() if v == "empty")
    total = len(classification)

    # Per-category breakdown
    by_category = {}
    for comp_id, comp_def in defn.get("components", {}).items():
        cat = comp_def.get("category", "unknown")
        if cat not in by_category:
            by_category[cat] = {"total": 0, "populated": 0, "empty": 0}
        by_category[cat]["total"] += 1
        status = classification.get(comp_id, "empty")
        if status in ("live", "static", "derived"):
            by_category[cat]["populated"] += 1
        else:
            by_category[cat]["empty"] += 1

    # List empty components
    empty_list = [comp_id for comp_id, status in classification.items() if status == "empty"]

    coverage_pct = round((live + static + derived) / max(total, 1) * 100, 1)
    live_pct = round((live + derived) / max(total, 1) * 100, 1)

    return {
        "index_id": index_id,
        "total_components": total,
        "live": live,
        "static": static,
        "derived": derived,
        "empty": empty,
        "coverage_pct": coverage_pct,
        "live_coverage_pct": live_pct,
        "by_category": by_category,
        "empty_components": empty_list,
    }


def get_all_coverage() -> dict:
    """Get coverage summary across all indices."""
    results = {}
    totals = {"total": 0, "live": 0, "static": 0, "derived": 0, "empty": 0}

    for index_id in COVERAGE:
        cov = get_coverage(index_id)
        if cov:
            results[index_id] = cov
            totals["total"] += cov["total_components"]
            totals["live"] += cov["live"]
            totals["static"] += cov["static"]
            totals["derived"] += cov["derived"]
            totals["empty"] += cov["empty"]

    totals["coverage_pct"] = round(
        (totals["live"] + totals["static"] + totals["derived"]) / max(totals["total"], 1) * 100, 1
    )
    totals["live_coverage_pct"] = round(
        (totals["live"] + totals["derived"]) / max(totals["total"], 1) * 100, 1
    )

    return {
        "indices": results,
        "totals": totals,
    }


def _load_definitions() -> dict:
    """Load all index definitions lazily."""
    defs = {}
    try:
        from app.index_definitions.lsti_v01 import LSTI_V01_DEFINITION
        defs["lsti"] = LSTI_V01_DEFINITION
    except ImportError:
        pass
    try:
        from app.index_definitions.bri_v01 import BRI_V01_DEFINITION
        defs["bri"] = BRI_V01_DEFINITION
    except ImportError:
        pass
    try:
        from app.index_definitions.dohi_v01 import DOHI_V01_DEFINITION
        defs["dohi"] = DOHI_V01_DEFINITION
    except ImportError:
        pass
    try:
        from app.index_definitions.vsri_v01 import VSRI_V01_DEFINITION
        defs["vsri"] = VSRI_V01_DEFINITION
    except ImportError:
        pass
    try:
        from app.index_definitions.cxri_v01 import CXRI_V01_DEFINITION
        defs["cxri"] = CXRI_V01_DEFINITION
    except ImportError:
        pass
    try:
        from app.index_definitions.tti_v01 import TTI_V01_DEFINITION
        defs["tti"] = TTI_V01_DEFINITION
    except ImportError:
        pass
    return defs
