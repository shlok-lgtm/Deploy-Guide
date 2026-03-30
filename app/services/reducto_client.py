"""
Reducto API client.
PDF document parsing with schema-level extraction and confidence scoring.
Used for attestation PDFs (Grant Thornton, BDO, Deloitte reports).
Docs: https://docs.reducto.ai
"""
import os
import httpx
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

REDUCTO_BASE = "https://platform.reducto.ai"


def _get_key():
    return os.getenv("REDUCTO_API_KEY")


def _headers():
    return {
        "Authorization": f"Bearer {_get_key()}",
        "Content-Type": "application/json",
    }


# BRSS attestation schema — one schema for ALL issuers
BRSS_ATTESTATION_SCHEMA = {
    "type": "object",
    "properties": {
        "issuer_name": {"type": "string", "description": "Name of the stablecoin issuer"},
        "asset_name": {"type": "string", "description": "Name of the token (e.g. USDC, USDT)"},
        "attestation_date": {"type": "string", "description": "As-of date of the report"},
        "total_reserves_usd": {"type": "number", "description": "Total reserves in USD"},
        "total_supply": {"type": "number", "description": "Total circulating supply"},
        "reserve_composition": {
            "type": "object",
            "properties": {
                "cash_and_deposits_pct": {"type": "number"},
                "us_treasury_bills_pct": {"type": "number"},
                "us_treasury_bonds_pct": {"type": "number"},
                "reverse_repo_pct": {"type": "number"},
                "commercial_paper_pct": {"type": "number"},
                "money_market_funds_pct": {"type": "number"},
                "corporate_bonds_pct": {"type": "number"},
                "secured_loans_pct": {"type": "number"},
                "crypto_collateral_pct": {"type": "number"},
                "other_pct": {"type": "number"}
            }
        },
        "auditor_name": {"type": "string"},
        "report_type": {"type": "string"},
        "custodians": {"type": "array", "items": {"type": "string"}},
        "jurisdiction": {"type": "string"}
    }
}


# Schema for synthetic/derivative-backed stablecoins (e.g., Ethena USDe)
SYNTHETIC_ATTESTATION_SCHEMA = {
    "type": "object",
    "properties": {
        "issuer_name": {"type": "string", "description": "Name of the issuer or protocol"},
        "asset_name": {"type": "string", "description": "Name of the token"},
        "attestation_date": {"type": "string", "description": "As-of date of the report"},
        "total_supply": {"type": "number", "description": "Total tokens in circulation"},
        "backing_assets": {
            "type": "object",
            "description": "What backs the token",
            "properties": {
                "total_value_usd": {"type": "number"},
                "staked_eth_usd": {"type": "number"},
                "derivatives_notional_usd": {"type": "number"},
                "other_usd": {"type": "number"},
            }
        },
        "custodians": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "assets_held_usd": {"type": "number"},
                    "percentage": {"type": "number"},
                }
            },
            "description": "Custodians holding backing assets"
        },
        "open_interest": {"type": "number", "description": "Total short perpetual futures position notional"},
        "collateral_ratio": {"type": "number", "description": "Total backing value / total supply"},
        "funding_rate": {"type": "number", "description": "Current aggregate funding rate"},
        "report_type": {"type": "string"},
        "auditor_name": {"type": "string"},
    }
}

# Schema for RWA-tokenized assets (e.g., USDY, BUIDL)
RWA_ATTESTATION_SCHEMA = {
    "type": "object",
    "properties": {
        "issuer_name": {"type": "string"},
        "asset_name": {"type": "string"},
        "attestation_date": {"type": "string", "description": "As-of date of the report"},
        "nav_per_token": {"type": "number", "description": "Net asset value per token"},
        "total_supply": {"type": "number"},
        "total_assets_usd": {"type": "number", "description": "Total assets under management"},
        "underlying_holdings": {
            "type": "object",
            "properties": {
                "us_treasuries_pct": {"type": "number"},
                "bank_deposits_pct": {"type": "number"},
                "money_market_pct": {"type": "number"},
                "corporate_bonds_pct": {"type": "number"},
                "other_pct": {"type": "number"},
            }
        },
        "yield_rate": {"type": "number", "description": "Current yield or APY"},
        "weighted_avg_maturity_days": {"type": "number"},
        "auditor_name": {"type": "string"},
        "report_type": {"type": "string"},
    }
}

# System prompts per disclosure type
SYSTEM_PROMPTS = {
    "fiat-reserve": "This is a stablecoin reserve attestation report from an accounting firm. Extract reserve composition, total reserves, supply, and auditor details.",
    "synthetic-derivative": "This is a custodian attestation or transparency report for a synthetic/derivative-backed stablecoin. Extract custodian holdings, open interest, collateral ratios, and backing asset composition. Do NOT look for traditional reserve categories like Treasury Bills — this is backed by derivatives positions.",
    "rwa-tokenized": "This is a fund report or NAV attestation for a tokenized real-world asset. Extract NAV per token, total AUM, underlying holdings composition, yield, and maturity.",
    "overcollateralized": "This is a report about an overcollateralized stablecoin. Extract collateral ratio, total collateral value, vault information, and liquidation parameters.",
    "algorithmic": "This is a report about an algorithmic or hybrid stablecoin. Extract collateral ratio, protocol-owned liquidity, AMO balances, and mechanism parameters.",
}

# Map disclosure_type to schema
SCHEMAS_BY_TYPE = {
    "fiat-reserve": BRSS_ATTESTATION_SCHEMA,
    "synthetic-derivative": SYNTHETIC_ATTESTATION_SCHEMA,
    "rwa-tokenized": RWA_ATTESTATION_SCHEMA,
}


# Custodian attestation PDF schema — narrow, for proof-of-custody documents
CUSTODIAN_ATTESTATION_SCHEMA = {
    "type": "object",
    "properties": {
        "custodian_name": {"type": "string", "description": "Name of the custodian (e.g., Copper, Ceffu, Cobo, Fireblocks)"},
        "attestation_date": {"type": "string", "description": "As-of date or report date"},
        "assets_under_custody_usd": {"type": "number", "description": "Total value of assets held by this custodian in USD"},
        "asset_breakdown": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "asset_type": {"type": "string", "description": "e.g., ETH, stETH, BTC, USDT"},
                    "amount": {"type": "number"},
                    "value_usd": {"type": "number"},
                }
            },
            "description": "Breakdown of assets held by type"
        },
        "attestor_name": {"type": "string", "description": "Firm that performed the attestation (if any)"},
        "report_type": {"type": "string", "description": "e.g., custody attestation, proof of reserves, SOC report"},
        "wallet_addresses": {
            "type": "array",
            "items": {"type": "string"},
            "description": "On-chain wallet addresses listed in the report (if any)"
        },
    }
}

# Source-type-specific schemas — used when a source_url has a known type
SOURCE_TYPE_SCHEMAS = {
    "custodian_pdf": {
        "schema": CUSTODIAN_ATTESTATION_SCHEMA,
        "system_prompt": "This is a custodian attestation report. It proves that a specific custodian holds certain assets on behalf of a stablecoin issuer. Extract the custodian name, assets under custody, asset breakdown by type, attestation date, and any wallet addresses listed.",
    },
    "dashboard": {
        "schema": None,  # Will use get_schema_for_type(disclosure_type)
        "system_prompt": None,  # Will use SYSTEM_PROMPTS[disclosure_type]
    },
    "attestation_page": {
        "schema": None,
        "system_prompt": None,
    },
}


def get_schema_for_type(disclosure_type: str) -> tuple[dict, str]:
    """Return (schema, system_prompt) for a given disclosure type."""
    schema = SCHEMAS_BY_TYPE.get(disclosure_type, BRSS_ATTESTATION_SCHEMA)
    prompt = SYSTEM_PROMPTS.get(disclosure_type, SYSTEM_PROMPTS["fiat-reserve"])
    return schema, prompt


async def parse_pdf(pdf_url: str, schema: dict = None, disclosure_type: str = None) -> dict:
    """
    Parse a PDF and extract structured data matching schema.
    If disclosure_type is provided and no explicit schema, uses the type-specific schema.
    """
    if not _get_key():
        logger.warning("REDUCTO_API_KEY not set, skipping PDF parse")
        return {"error": "no_api_key"}

    if schema is None:
        if disclosure_type and disclosure_type in SCHEMAS_BY_TYPE:
            schema, system_prompt = get_schema_for_type(disclosure_type)
        else:
            schema = BRSS_ATTESTATION_SCHEMA
            system_prompt = SYSTEM_PROMPTS["fiat-reserve"]
    else:
        system_prompt = SYSTEM_PROMPTS.get(disclosure_type, SYSTEM_PROMPTS["fiat-reserve"])

    async with httpx.AsyncClient(timeout=180) as client:
        try:
            resp = await client.post(
                f"{REDUCTO_BASE}/extract",
                headers=_headers(),
                json={
                    "input": pdf_url,
                    "instructions": {
                        "schema": schema,
                        "system_prompt": system_prompt,
                    },
                    "settings": {
                        "citations": {"enabled": True},
                    },
                }
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Reducto parse failed for {pdf_url}: {e}")
            return {"error": str(e)}


async def parse_to_markdown(pdf_url: str) -> dict:
    """
    Parse a PDF into markdown chunks without schema extraction.
    Uses Reducto's /parse endpoint.
    Useful when you don't know the document structure.
    """
    if not _get_key():
        return {"error": "no_api_key"}

    async with httpx.AsyncClient(timeout=180) as client:
        try:
            resp = await client.post(
                f"{REDUCTO_BASE}/parse",
                headers=_headers(),
                json={"input": pdf_url}
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Reducto markdown parse failed for {pdf_url}: {e}")
            return {"error": str(e)}
