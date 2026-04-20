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
import time
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


_MAX_ATTEMPTS = 3
_BACKOFF_SECS = (2, 4, 8)  # waits between attempts; index = attempts already made
_PER_PEER_SLEEP = 1.5  # honored between successful peer fetches, mirrors
                       # app/collectors/lst_collector.py:109


def _cg_wait_from_retry_after(resp: "requests.Response", fallback: float) -> float:
    """Honor the Retry-After header if present and numeric; else fallback."""
    ra = resp.headers.get("Retry-After")
    if not ra:
        return fallback
    try:
        return max(float(ra), fallback)
    except (TypeError, ValueError):
        return fallback


def fetch_eth_price_ratio(coingecko_id: str) -> float:
    """Fetch the current LST/ETH price ratio from CoinGecko.

    Returns the raw ETH-denominated price (e.g. 0.994 for stETH, 1.158 for
    rETH) as a float.

    On transient failure (HTTP 429 or 5xx) retries up to 3 attempts with
    exponential backoff (2s, 4s, 8s), honoring Retry-After if the server
    sent one. On final failure — or on any 200 response that does not
    include the coingecko_id — raises RuntimeError so the caller aborts
    the DB write instead of committing a silent null.

    Voice: loud. Each attempt prints a single status line so the operator
    running this in a terminal can see what's happening.
    """
    url = f"{CG_BASE}/simple/price"
    params = {"ids": coingecko_id, "vs_currencies": "eth"}

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(
                url, params=params, headers=_cg_headers(), timeout=10
            )
        except requests.RequestException as e:
            wait = _BACKOFF_SECS[min(attempt - 1, len(_BACKOFF_SECS) - 1)]
            print(
                f"  [attempt {attempt}/{_MAX_ATTEMPTS}] {coingecko_id}: "
                f"network error ({e}); waiting {wait}s before retry"
            )
            if attempt == _MAX_ATTEMPTS:
                raise RuntimeError(
                    f"CoinGecko fetch failed for {coingecko_id} after "
                    f"{_MAX_ATTEMPTS} attempts (last error: {e})"
                ) from e
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            data = resp.json()
            val = (data.get(coingecko_id) or {}).get("eth")
            if val is None:
                # 200 but empty payload — treat as a hard failure so the
                # operator knows CG accepted the id but returned nothing.
                raise RuntimeError(
                    f"CoinGecko returned 200 but no eth price for "
                    f"{coingecko_id}; response: {data!r}"
                )
            print(f"  [attempt {attempt}/{_MAX_ATTEMPTS}] {coingecko_id}: OK ({val:.4f})")
            return float(val)

        # Retryable status
        if resp.status_code in (429, 500, 502, 503, 504):
            wait = _cg_wait_from_retry_after(
                resp, _BACKOFF_SECS[min(attempt - 1, len(_BACKOFF_SECS) - 1)]
            )
            print(
                f"  [attempt {attempt}/{_MAX_ATTEMPTS}] {coingecko_id}: "
                f"HTTP {resp.status_code}; waiting {wait:g}s before retry"
            )
            if attempt == _MAX_ATTEMPTS:
                raise RuntimeError(
                    f"CoinGecko fetch failed for {coingecko_id} after "
                    f"{_MAX_ATTEMPTS} attempts (last status: {resp.status_code})"
                )
            time.sleep(wait)
            continue

        # Non-retryable error — fail immediately so the operator doesn't
        # wait out a full backoff cycle on a 404/401.
        raise RuntimeError(
            f"CoinGecko fetch failed for {coingecko_id}: "
            f"HTTP {resp.status_code} (non-retryable)"
        )

    # Unreachable — loop always returns or raises.
    raise RuntimeError(f"CoinGecko fetch loop exited unexpectedly for {coingecko_id}")


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
    """Assemble the per-peer snapshot. Raises RuntimeError if the CoinGecko
    fetch fails for any peer after 3 attempts — the caller is expected to
    abort the DB write rather than commit a row with silent nulls."""
    components = {}
    for idx, peer in enumerate(PEERS):
        print(f"Fetching {peer['slug']} ({peer['symbol']}):")
        values = None
        if api_base:
            values = fetch_live_components(api_base, peer["slug"])
        if values is None:
            values = dict(FALLBACK_VALUES[peer["slug"]])
        # eth_price_ratio is always fetched directly from CoinGecko.
        # Raises on final failure — propagates to main().
        values["eth_price_ratio"] = fetch_eth_price_ratio(peer["coingecko_id"])
        components[peer["slug"]] = {
            "name": peer["name"],
            "symbol": peer["symbol"],
            "design": peer["design"],
            "values": values,
        }
        # Throttle between peers to stay well under CoinGecko's public
        # rate ceiling. Skip after the last peer.
        if idx < len(PEERS) - 1:
            time.sleep(_PER_PEER_SLEEP)
    return {
        "peers": components,
        "component_order": Q4_COMPONENTS,
        "component_meta": COMPONENT_META,
    }


def main() -> None:
    api_base = os.environ.get("BASIS_API_BASE")  # e.g. http://localhost:5000
    try:
        components_json = build_row(api_base)
    except RuntimeError as e:
        print(
            "\nAborting DB write — CoinGecko fetch failed for one or more "
            "peers. No silent nulls. Re-run when rate limit clears.\n"
            f"Reason: {e}",
            file=sys.stderr,
        )
        sys.exit(1)
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
