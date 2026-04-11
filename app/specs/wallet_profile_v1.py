"""
Wallet Risk Profile Schema v1.0.0
===================================
Defines the wallet profile as a reputation primitive — a cumulative behavioral
record that is queryable, hashable, and verifiable.
"""

WALLET_PROFILE_SCHEMA_V1 = {
    "version": "1.0.0",
    "description": "Wallet Risk Profile — reputation primitive. Cumulative behavioral record, queryable and verifiable.",
    "fields": {
        "address": "Wallet address (0x-prefixed)",
        "chain": "Primary chain (default: ethereum)",
        "profile_hash": "SHA-256 of the canonical profile JSON",
        "computed_at": "ISO timestamp",
        "current_state": {
            "risk_score": "Value-weighted SII (0-100)",
            "concentration_hhi": "Herfindahl index (0-10000)",
            "total_value_usd": "Total stablecoin value",
            "holdings_count": "Number of stablecoin positions",
            "dominant_asset": "Symbol of largest holding",
            "dominant_pct": "Percentage in dominant asset",
        },
        "behavioral_signals": {
            "days_tracked": "Days since first score computed",
            "score_stability_30d": "Std dev of risk score over trailing 30 days",
            "avg_score_30d": "Average risk score over trailing 30 days",
            "max_drawdown_90d": "Largest score drop in trailing 90 days",
            "diversification_trend": "improving | stable | deteriorating (based on HHI trend)",
        },
        "score_consistency": {
            "pct_days_high_score": "Percentage of tracked days at score >= 80",
            "best_score_ever": "Highest score recorded",
            "worst_score_ever": "Lowest score recorded",
        },
    },
}
