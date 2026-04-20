"""
Populate the incident_snapshots row for slug = rseth-2026-04-18.

This is a one-off freeze of the Q4 forum-reply data package from
audits/lsti_rseth_audit_2026-04-20.md. Values are captured from the
live /api/lsti/scores/{slug} endpoint when available; for operators
running this in environments without live API access, a fallback value
set is used that matches the audit's documented static values.

Usage:
    python scripts/populate_incident_rseth_2026_04_18.py

Idempotent — re-running updates captured_at and components_json.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timezone

import requests

# Project path setup so we can import app.database without installing the package
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)

from app.database import execute  # noqa: E402

SLUG = "rseth-2026-04-18"
EVENT_DATE = date(2026, 4, 18)
TITLE = "Unbacked rsETH Minting via Kelp DAO LayerZero Bridge"
SUMMARY = (
    "116,500 rsETH (~$292M) minted without ETH backing, deposited as "
    "Aave V3 collateral, ~$196M WETH borrowed."
)

# The Q4 forum-reply package as revised in audit v1.1 (2026-04-20).
# Order matters — renders top-to-bottom in the incident page table.
#
#   1. exploit_history_lst         — static, updated 100 -> 10 per audit.
#   2. eth_price_ratio             — NEW in v1.1. Cross-peer comparable when
#                                    paired with the Design column.
#   3. peg_volatility_7d           — design-agnostic price-noise signal.
#   4. market_cap                  — objective size anchor.
#   5. top_holder_concentration    — objective distribution signal.
#
# Removed in v1.1 (see audits/lsti_rseth_audit_2026-04-20.md §V1.1):
#   - dex_pool_depth      (collector-to-storage wiring gap)
#   - eth_peg_deviation   (not cross-peer comparable: rebasing vs reward-bearing)
Q4_COMPONENTS = [
    "exploit_history_lst",
    "eth_price_ratio",
    "peg_volatility_7d",
    "market_cap",
    "top_holder_concentration",
]

COMPONENT_META = {
    "exploit_history_lst": {
        "label": "Exploit History",
        "category": "smart_contract",
        "unit": "score",
        "source": "static, updated 2026-04-20",
        "note": "Static floor lowered 100 → 10 per audit.",
    },
    "eth_price_ratio": {
        "label": "ETH Price Ratio",
        "category": "peg_stability",
        "unit": "ETH",
        "source": "CoinGecko",
        "note": (
            "Cross-peer comparable. Reward-bearing LSTs (rETH, rsETH) "
            "appreciate above 1.0 by design; rebasing LSTs (stETH, eETH) "
            "target 1.0. See audit v1.1 for methodology."
        ),
    },
    "peg_volatility_7d": {
        "label": "7d Peg Volatility",
        "category": "peg_stability",
        "unit": "%",
        "source": "CoinGecko",
        "note": "Live at capture time.",
    },
    "market_cap": {
        "label": "Market Cap",
        "category": "liquidity",
        "unit": "USD",
        "source": "CoinGecko",
        "note": "Live at capture time.",
    },
    "top_holder_concentration": {
        "label": "Top 10 Holder Share",
        "category": "distribution",
        "unit": "%",
        "source": "Etherscan",
        "note": "Live at capture time (24h cache).",
    },
}

# Design type — Rebasing vs Reward-bearing. Baked here, not fetched live:
# this is a structural property of each LST that does not change without a
# protocol upgrade. See audit v1.1 §Finding 2.
#   Rebasing        — token balance adjusts; price targets 1.0 ETH.
#   Reward-bearing  — token balance is static; exchange rate appreciates > 1.0 ETH.
PEERS = [
    {"slug": "kelp-rseth",       "name": "Kelp rsETH",       "symbol": "rsETH", "coingecko_id": "kelp-dao-restaked-eth", "design": "reward-bearing"},
    {"slug": "lido-steth",       "name": "Lido stETH",       "symbol": "stETH", "coingecko_id": "staked-ether",          "design": "rebasing"},
    {"slug": "rocket-pool-reth", "name": "Rocket Pool rETH", "symbol": "rETH",  "coingecko_id": "rocket-pool-eth",       "design": "reward-bearing"},
    {"slug": "etherfi-eeth",     "name": "EtherFi eETH",     "symbol": "eETH",  "coingecko_id": "ether-fi-staked-eth",   "design": "rebasing"},
]

# Fallback values if the hub API is unreachable. These mirror the
# static_config values documented in audits/lsti_rseth_audit_2026-04-20.md
# for exploit_history_lst (the only component whose value we know precisely
# without a live query) and use None otherwise so the page renders
# "—" instead of a fabricated number.
FALLBACK_VALUES = {
    "kelp-rseth":       {"exploit_history_lst": 10,  "market_cap": None, "top_holder_concentration": None, "peg_volatility_7d": None, "eth_price_ratio": None},
    "lido-steth":       {"exploit_history_lst": 100, "market_cap": None, "top_holder_concentration": None, "peg_volatility_7d": None, "eth_price_ratio": None},
    "rocket-pool-reth": {"exploit_history_lst": 100, "market_cap": None, "top_holder_concentration": None, "peg_volatility_7d": None, "eth_price_ratio": None},
    "etherfi-eeth":     {"exploit_history_lst": 100, "market_cap": None, "top_holder_concentration": None, "peg_volatility_7d": None, "eth_price_ratio": None},
}


COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
CG_BASE = (
    "https://pro-api.coingecko.com/api/v3"
    if COINGECKO_API_KEY
    else "https://api.coingecko.com/api/v3"
)


def _cg_headers() -> dict:
    h = {"Accept": "application/json"}
    if COINGECKO_API_KEY:
        h["x-cg-pro-api-key"] = COINGECKO_API_KEY
    return h


def fetch_eth_price_ratio(coingecko_id: str) -> float | None:
    """Fetch the current LST/ETH price ratio from CoinGecko.

    Returns the raw ETH-denominated price (e.g. 0.994 for stETH, 1.158 for
    rETH). None if the call fails — the snapshot then records null and the
    page renders an em-dash for that cell rather than fabricating a number.
    """
    try:
        resp = requests.get(
            f"{CG_BASE}/simple/price",
            params={"ids": coingecko_id, "vs_currencies": "eth"},
            headers=_cg_headers(),
            timeout=10,
        )
        if resp.status_code != 200:
            return None
        data = resp.json()
        val = (data.get(coingecko_id) or {}).get("eth")
        return float(val) if val is not None else None
    except Exception:
        return None


def fetch_live_components(api_base: str, slug: str) -> dict | None:
    """Fetch the hub-side Q4 components. eth_price_ratio is sourced from
    CoinGecko directly below; everything else reads from the hub's
    raw_values for the LSTI scoring row."""
    url = f"{api_base.rstrip('/')}/api/lsti/scores/{slug}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return None
        data = resp.json()
        raw = data.get("raw_values") or {}
        # Only pull hub-sourced components — eth_price_ratio is filled in
        # by the direct CoinGecko call in build_row().
        return {c: raw.get(c) for c in Q4_COMPONENTS if c != "eth_price_ratio"}
    except Exception:
        return None


def build_row(api_base: str | None) -> dict:
    components = {}
    for peer in PEERS:
        values = None
        if api_base:
            values = fetch_live_components(api_base, peer["slug"])
        if values is None:
            values = dict(FALLBACK_VALUES[peer["slug"]])
        # Always attempt the CoinGecko ETH-ratio fetch regardless of hub
        # reachability. Stays null on failure.
        values["eth_price_ratio"] = fetch_eth_price_ratio(peer["coingecko_id"])
        components[peer["slug"]] = {
            "name": peer["name"],
            "symbol": peer["symbol"],
            "design": peer["design"],
            "values": values,
        }
    return {
        "peers": components,
        "component_order": Q4_COMPONENTS,
        "component_meta": COMPONENT_META,
    }


def main() -> None:
    api_base = os.environ.get("BASIS_API_BASE")  # e.g. http://localhost:5000
    components_json = build_row(api_base)
    metadata = {
        "audit_path": "audits/lsti_rseth_audit_2026-04-20.md",
        "q4_components": Q4_COMPONENTS,
        "source_of_truth": "api.lsti.scores.raw_values" if api_base else "fallback",
        "captured_iso": datetime.now(timezone.utc).isoformat(),
    }

    execute(
        """
        INSERT INTO incident_snapshots (
            slug, event_date, title, summary, captured_at,
            components_json, metadata_json
        ) VALUES (%s, %s, %s, %s, NOW(), %s, %s)
        ON CONFLICT (slug) DO UPDATE SET
            event_date = EXCLUDED.event_date,
            title = EXCLUDED.title,
            summary = EXCLUDED.summary,
            captured_at = NOW(),
            components_json = EXCLUDED.components_json,
            metadata_json = EXCLUDED.metadata_json,
            updated_at = NOW()
        """,
        (
            SLUG,
            EVENT_DATE,
            TITLE,
            SUMMARY,
            json.dumps(components_json, default=str),
            json.dumps(metadata, default=str),
        ),
    )
    print(f"Wrote incident_snapshots row for slug={SLUG}")


if __name__ == "__main__":
    main()
