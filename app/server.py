"""
Basis Protocol - API Server
============================
Clean FastAPI server. Reads from database only. No data collection.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.config import STABLECOIN_REGISTRY, CORS_ORIGINS
from app.database import (
    init_pool, close_pool, health_check as db_health_check,
    fetch_one, fetch_all,
)
from app.scoring import (
    SII_V1_WEIGHTS, STRUCTURAL_SUBWEIGHTS, FORMULA_VERSION,
    score_to_grade, COMPONENT_NORMALIZATIONS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Basis Protocol API",
    description="Standardized risk surfaces for on-chain finance",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Lifecycle
# =============================================================================

@app.on_event("startup")
async def startup():
    init_pool()
    # Register governance intelligence routes
    try:
        from app.governance import register_gov_routes, apply_gov_migration
        apply_gov_migration()
        register_gov_routes(app)
        logger.info("Governance intelligence routes registered")
    except Exception as e:
        logger.warning(f"Governance module not available: {e}")
    # Register content engine routes
    try:
        from app.content_engine import register_content_routes
        register_content_routes(app)
        logger.info("Content engine routes registered")
    except Exception as e:
        logger.warning(f"Content engine not available: {e}")
    logger.info("Basis Protocol API started")


@app.on_event("shutdown")
async def shutdown():
    close_pool()
    logger.info("Basis Protocol API stopped")


# =============================================================================
# Root — landing page for browsers
# =============================================================================

@app.get("/")
async def root():
    """API info page."""
    return {
        "name": "Basis Protocol API",
        "product": "Stablecoin Integrity Index (SII)",
        "version": FORMULA_VERSION,
        "endpoints": {
            "health": "/api/health",
            "scores": "/api/scores",
            "detail": "/api/scores/{coin}",
            "history": "/api/scores/{coin}/history?days=90",
            "compare": "/api/compare?coins=usdc,usdt,dai",
            "methodology": "/api/methodology",
            "events": "/api/events",
        },
        "docs": "/docs",
    }


# =============================================================================
# 1. GET /api/health
# =============================================================================

@app.get("/api/health")
async def get_health():
    """System health check — is the database up, how many stablecoins scored."""
    db_status = db_health_check()
    
    scores_result = fetch_one("SELECT COUNT(*) as count FROM scores")
    scored_count = scores_result["count"] if scores_result else 0
    
    latest_result = fetch_one(
        "SELECT MAX(computed_at) as latest FROM scores"
    )
    latest_score = latest_result["latest"] if latest_result else None
    
    return {
        "status": "healthy" if db_status["status"] == "healthy" else "degraded",
        "database": db_status,
        "scores": {
            "stablecoins_scored": scored_count,
            "stablecoins_registered": len(STABLECOIN_REGISTRY),
            "last_computed": latest_score.isoformat() if latest_score else None,
        },
        "formula_version": FORMULA_VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# 2. GET /api/scores — All stablecoin scores (main rankings table)
# =============================================================================

@app.get("/api/scores")
async def get_scores():
    """Get current SII scores for all stablecoins."""
    rows = fetch_all("""
        SELECT s.*, st.name, st.symbol, st.issuer
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        ORDER BY s.overall_score DESC
    """)
    
    results = []
    for row in rows:
        results.append({
            "id": row["stablecoin_id"],
            "name": row["name"],
            "symbol": row["symbol"],
            "issuer": row["issuer"],
            "score": float(row["overall_score"]),
            "grade": row["grade"],
            "price": float(row["current_price"]) if row.get("current_price") else None,
            "market_cap": row.get("market_cap"),
            "volume_24h": row.get("volume_24h"),
            "daily_change": float(row["daily_change"]) if row.get("daily_change") else None,
            "weekly_change": float(row["weekly_change"]) if row.get("weekly_change") else None,
            "categories": {
                "peg": float(row["peg_score"]) if row.get("peg_score") else None,
                "liquidity": float(row["liquidity_score"]) if row.get("liquidity_score") else None,
                "flows": float(row["mint_burn_score"]) if row.get("mint_burn_score") else None,
                "distribution": float(row["distribution_score"]) if row.get("distribution_score") else None,
                "structural": float(row["structural_score"]) if row.get("structural_score") else None,
            },
            "structural_breakdown": {
                "reserves": float(row["reserves_score"]) if row.get("reserves_score") else None,
                "contract": float(row["contract_score"]) if row.get("contract_score") else None,
                "oracle": float(row["oracle_score"]) if row.get("oracle_score") else None,
                "governance": float(row["governance_score"]) if row.get("governance_score") else None,
                "network": float(row["network_score"]) if row.get("network_score") else None,
            },
            "component_count": row.get("component_count"),
            "formula_version": row.get("formula_version"),
            "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
        })
    
    return {
        "stablecoins": results,
        "count": len(results),
        "formula_version": FORMULA_VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# 3. GET /api/scores/{coin} — Detailed score for one stablecoin
# =============================================================================

@app.get("/api/scores/{coin}")
async def get_score_detail(coin: str):
    """Get detailed SII score breakdown for a specific stablecoin."""
    row = fetch_one("""
        SELECT s.*, st.name, st.symbol, st.issuer, st.attestation_config, st.regulatory_licenses
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE s.stablecoin_id = %s
    """, (coin,))
    
    if not row:
        # Check if the stablecoin exists but isn't scored yet
        exists = fetch_one("SELECT id FROM stablecoins WHERE id = %s", (coin,))
        if exists:
            raise HTTPException(status_code=404, detail=f"Stablecoin '{coin}' exists but has no scores yet")
        raise HTTPException(status_code=404, detail=f"Stablecoin '{coin}' not found")
    
    # Get latest component readings
    components = fetch_all("""
        SELECT component_id, category, raw_value, normalized_score, data_source, collected_at
        FROM component_readings
        WHERE stablecoin_id = %s
          AND collected_at > NOW() - INTERVAL '48 hours'
        ORDER BY category, component_id
    """, (coin,))
    
    return {
        "id": row["stablecoin_id"],
        "name": row["name"],
        "symbol": row["symbol"],
        "issuer": row["issuer"],
        "score": float(row["overall_score"]),
        "grade": row["grade"],
        "price": float(row["current_price"]) if row.get("current_price") else None,
        "market_cap": row.get("market_cap"),
        "volume_24h": row.get("volume_24h"),
        "categories": {
            "peg": {"score": float(row["peg_score"]) if row.get("peg_score") else None, "weight": SII_V1_WEIGHTS["peg_stability"]},
            "liquidity": {"score": float(row["liquidity_score"]) if row.get("liquidity_score") else None, "weight": SII_V1_WEIGHTS["liquidity_depth"]},
            "flows": {"score": float(row["mint_burn_score"]) if row.get("mint_burn_score") else None, "weight": SII_V1_WEIGHTS["mint_burn_dynamics"]},
            "distribution": {"score": float(row["distribution_score"]) if row.get("distribution_score") else None, "weight": SII_V1_WEIGHTS["holder_distribution"]},
            "structural": {"score": float(row["structural_score"]) if row.get("structural_score") else None, "weight": SII_V1_WEIGHTS["structural_risk_composite"]},
        },
        "structural_breakdown": {
            "reserves": {"score": float(row["reserves_score"]) if row.get("reserves_score") else None, "weight": STRUCTURAL_SUBWEIGHTS["reserves_collateral"]},
            "contract": {"score": float(row["contract_score"]) if row.get("contract_score") else None, "weight": STRUCTURAL_SUBWEIGHTS["smart_contract_risk"]},
            "oracle": {"score": float(row["oracle_score"]) if row.get("oracle_score") else None, "weight": STRUCTURAL_SUBWEIGHTS["oracle_integrity"]},
            "governance": {"score": float(row["governance_score"]) if row.get("governance_score") else None, "weight": STRUCTURAL_SUBWEIGHTS["governance_operations"]},
            "network": {"score": float(row["network_score"]) if row.get("network_score") else None, "weight": STRUCTURAL_SUBWEIGHTS["network_chain_risk"]},
        },
        "components": [
            {
                "id": c["component_id"],
                "category": c["category"],
                "raw_value": c["raw_value"],
                "normalized_score": round(c["normalized_score"], 2) if c["normalized_score"] else None,
                "data_source": c["data_source"],
                "collected_at": c["collected_at"].isoformat() if c["collected_at"] else None,
            }
            for c in components
        ],
        "attestation": row.get("attestation_config"),
        "regulatory_licenses": row.get("regulatory_licenses"),
        "component_count": row.get("component_count"),
        "formula_version": row.get("formula_version"),
        "daily_change": float(row["daily_change"]) if row.get("daily_change") else None,
        "weekly_change": float(row["weekly_change"]) if row.get("weekly_change") else None,
        "computed_at": row["computed_at"].isoformat() if row.get("computed_at") else None,
    }


# =============================================================================
# 4. GET /api/scores/{coin}/history — Historical scores
# =============================================================================

@app.get("/api/scores/{coin}/history")
async def get_score_history(
    coin: str,
    days: int = Query(default=90, ge=1, le=365),
):
    """Get historical SII scores for a stablecoin."""
    rows = fetch_all("""
        SELECT score_date, overall_score, grade, peg_score, liquidity_score,
               mint_burn_score, distribution_score, structural_score,
               daily_change, component_count
        FROM score_history
        WHERE stablecoin = %s
          AND score_date > CURRENT_DATE - INTERVAL '%s days'
        ORDER BY score_date ASC
    """, (coin, days))
    
    return {
        "stablecoin": coin,
        "days": days,
        "history": [
            {
                "date": str(row["score_date"]),
                "score": float(row["overall_score"]),
                "grade": row["grade"],
                "categories": {
                    "peg": float(row["peg_score"]) if row.get("peg_score") else None,
                    "liquidity": float(row["liquidity_score"]) if row.get("liquidity_score") else None,
                    "flows": float(row["mint_burn_score"]) if row.get("mint_burn_score") else None,
                    "distribution": float(row["distribution_score"]) if row.get("distribution_score") else None,
                    "structural": float(row["structural_score"]) if row.get("structural_score") else None,
                },
                "daily_change": float(row["daily_change"]) if row.get("daily_change") else None,
                "component_count": row.get("component_count"),
            }
            for row in rows
        ],
        "count": len(rows),
    }


# =============================================================================
# 5. GET /api/compare — Compare stablecoins side by side
# =============================================================================

@app.get("/api/compare")
async def compare_scores(
    coins: str = Query(description="Comma-separated stablecoin IDs", examples=["usdc,usdt,dai"]),
):
    """Compare SII scores for multiple stablecoins."""
    coin_list = [c.strip().lower() for c in coins.split(",") if c.strip()]
    
    if not coin_list:
        raise HTTPException(status_code=400, detail="Provide at least one stablecoin ID")
    if len(coin_list) > 10:
        raise HTTPException(status_code=400, detail="Maximum 10 stablecoins per comparison")
    
    placeholders = ",".join(["%s"] * len(coin_list))
    rows = fetch_all(f"""
        SELECT s.*, st.name, st.symbol, st.issuer
        FROM scores s
        JOIN stablecoins st ON st.id = s.stablecoin_id
        WHERE s.stablecoin_id IN ({placeholders})
        ORDER BY s.overall_score DESC
    """, tuple(coin_list))
    
    return {
        "comparison": [
            {
                "id": row["stablecoin_id"],
                "name": row["name"],
                "symbol": row["symbol"],
                "score": float(row["overall_score"]),
                "grade": row["grade"],
                "categories": {
                    "peg": float(row["peg_score"]) if row.get("peg_score") else None,
                    "liquidity": float(row["liquidity_score"]) if row.get("liquidity_score") else None,
                    "flows": float(row["mint_burn_score"]) if row.get("mint_burn_score") else None,
                    "distribution": float(row["distribution_score"]) if row.get("distribution_score") else None,
                    "structural": float(row["structural_score"]) if row.get("structural_score") else None,
                },
            }
            for row in rows
        ],
        "count": len(rows),
        "requested": coin_list,
    }


# =============================================================================
# 6. GET /api/methodology — Formula and weights
# =============================================================================

@app.get("/api/methodology")
async def get_methodology():
    """Get the SII methodology — formula, weights, and component specifications."""
    return {
        "version": FORMULA_VERSION,
        "formula": "SII = 0.30×Peg + 0.25×Liquidity + 0.15×MintBurn + 0.10×Distribution + 0.20×Structural",
        "structural_formula": "Structural = 0.30×Reserves + 0.20×SmartContract + 0.15×Oracle + 0.20×Governance + 0.15×Network",
        "weights": {
            "top_level": SII_V1_WEIGHTS,
            "structural": STRUCTURAL_SUBWEIGHTS,
        },
        "grade_scale": {
            "A+": "90-100", "A": "85-90", "A-": "80-85",
            "B+": "75-80", "B": "70-75", "B-": "65-70",
            "C+": "60-65", "C": "55-60", "C-": "50-55",
            "D": "45-50", "F": "0-45",
        },
        "components": {
            comp_id: {
                "category": spec["category"],
                "weight_in_category": spec["weight"],
            }
            for comp_id, spec in COMPONENT_NORMALIZATIONS.items()
        },
        "total_components": 102,
        "automated_components": 83,
        "data_sources": [
            "CoinGecko Pro", "DeFiLlama", "Etherscan", "Curve Finance",
            "Issuer attestation reports", "On-chain contract analysis",
        ],
    }


# =============================================================================
# 7. GET /api/config — Stablecoin registry
# =============================================================================

@app.get("/api/config")
async def get_config():
    """Get the stablecoin registry configuration."""
    return {
        "stablecoins": {
            sid: {
                "name": cfg["name"],
                "symbol": cfg["symbol"],
                "issuer": cfg["issuer"],
                "coingecko_id": cfg["coingecko_id"],
                "contract": cfg.get("contract"),
                "attestation": cfg.get("attestation"),
            }
            for sid, cfg in STABLECOIN_REGISTRY.items()
        },
        "count": len(STABLECOIN_REGISTRY),
    }


# =============================================================================
# 8. GET /api/events — Crisis events timeline
# =============================================================================

@app.get("/api/events")
async def get_events(
    limit: int = Query(default=50, ge=1, le=500),
):
    """Get crisis events and annotations timeline."""
    rows = fetch_all("""
        SELECT id, event_date, event_name, event_type, affected_stablecoins,
               description, severity
        FROM score_events
        ORDER BY event_date DESC
        LIMIT %s
    """, (limit,))
    
    return {
        "events": [
            {
                "date": str(row["event_date"]),
                "name": row["event_name"],
                "type": row["event_type"],
                "affected": row["affected_stablecoins"],
                "description": row["description"],
                "severity": row["severity"],
            }
            for row in rows
        ],
        "count": len(rows),
    }


# =============================================================================
# 9. GET /api/deviations/{coin} — Peg deviation history
# =============================================================================

@app.get("/api/deviations/{coin}")
async def get_deviations(
    coin: str,
    limit: int = Query(default=50, ge=1, le=500),
):
    """Get historical peg deviation events for a stablecoin."""
    # Map stablecoin ID to coingecko_id
    cfg = STABLECOIN_REGISTRY.get(coin)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Stablecoin '{coin}' not found")
    
    cg_id = cfg["coingecko_id"]
    
    rows = fetch_all("""
        SELECT event_start, event_end, duration_hours, max_deviation_pct,
               avg_deviation_pct, direction, recovery_complete,
               market_cap_at_start, volume_during_event
        FROM deviation_events
        WHERE coingecko_id = %s
        ORDER BY event_start DESC
        LIMIT %s
    """, (cg_id, limit))
    
    return {
        "stablecoin": coin,
        "coingecko_id": cg_id,
        "deviations": [
            {
                "start": row["event_start"].isoformat() if row["event_start"] else None,
                "end": row["event_end"].isoformat() if row["event_end"] else None,
                "duration_hours": row["duration_hours"],
                "max_deviation_pct": row["max_deviation_pct"],
                "avg_deviation_pct": row["avg_deviation_pct"],
                "direction": row["direction"],
                "recovered": row["recovery_complete"],
                "market_cap": row["market_cap_at_start"],
                "volume": row["volume_during_event"],
            }
            for row in rows
        ],
        "count": len(rows),
    }
