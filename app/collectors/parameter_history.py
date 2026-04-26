"""
Protocol Parameter Change History Collector (Pipeline 9)
==========================================================
Captures on-chain governance parameter changes across scored protocols
with concurrent SII/PSI score state at time of each change.

Uses Alchemy RPC (eth_call) or Etherscan eth_call fallback for contract reads.
Runs in both fast cycle (change detection only) and slow cycle (full snapshots).
Never raises — all errors logged and skipped.
"""

import hashlib
import json
import logging
import os
import time
from datetime import date, datetime, timezone, timedelta

import httpx

from app.database import fetch_all, fetch_one, execute
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# RPC helpers
# ---------------------------------------------------------------------------

def _get_rpc_url(chain: str = "ethereum") -> str:
    alchemy_key = os.environ.get("ALCHEMY_API_KEY", "")
    if not alchemy_key:
        return ""
    chain_map = {
        "ethereum": "eth-mainnet",
        "arbitrum": "arb-mainnet",
        "base": "base-mainnet",
    }
    network = chain_map.get(chain, "eth-mainnet")
    return f"https://{network}.g.alchemy.com/v2/{alchemy_key}"


def _eth_call_sync(contract: str, data: str, chain: str = "ethereum") -> str:
    """Execute eth_call via Alchemy RPC (sync). Falls back to Etherscan."""
    rpc_url = _get_rpc_url(chain)
    if rpc_url:
        try:
            resp = httpx.post(
                rpc_url,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_call",
                    "params": [{"to": contract, "data": data}, "latest"],
                },
                timeout=15,
            )
            result = resp.json().get("result", "0x")
            if result and result != "0x":
                return result
        except Exception as e:
            logger.debug(f"RPC eth_call failed for {contract}: {e}")

    # Etherscan fallback (ethereum only)
    if chain == "ethereum":
        api_key = os.environ.get("ETHERSCAN_API_KEY", "")
        if api_key:
            try:
                resp = httpx.get(
                    "https://api.etherscan.io/v2/api",
                    params={
                        "module": "proxy",
                        "action": "eth_call",
                        "to": contract,
                        "data": data,
                        "tag": "latest",
                        "apikey": api_key,
                    },
                    timeout=15,
                )
                result = resp.json().get("result", "0x")
                if result and result != "0x":
                    return result
            except Exception as e:
                logger.debug(f"Etherscan eth_call fallback failed: {e}")

    return "0x"


def _encode_address_param(address: str) -> str:
    """ABI-encode an address as a 32-byte padded hex param."""
    return address.lower().replace("0x", "").zfill(64)


def _decode_uint256(hex_str: str, offset: int = 0) -> int:
    """Decode a uint256 from a hex response at a 32-byte word offset."""
    start = 2 + (offset * 64)  # skip 0x prefix
    end = start + 64
    if len(hex_str) < end:
        return 0
    return int(hex_str[start:end], 16)


# ---------------------------------------------------------------------------
# Protocol Parameter Registry
# ---------------------------------------------------------------------------

# Function selectors (first 4 bytes of keccak256 of function signature)
# getReserveData(address) = 0x35ea6a75
# getAssetInfoByAddress(address) = 0xc2be3a6c
# ilks(bytes32) = 0xd9638d36

AAVE_GET_RESERVE_DATA = "0x35ea6a75"
COMPOUND_GET_ASSET_INFO = "0xc2be3a6c"

WATCHED_ASSETS = {
    "usdc": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "dai": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    "weth": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "wbtc": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
}

# Each param spec: (parameter_key, parameter_name, field_index_in_return, value_unit, normalization_factor)
AAVE_PARAM_SPECS = [
    ("ltv", "LTV", 1, "percent", 100),
    ("liquidation_threshold", "Liquidation Threshold", 2, "percent", 100),
    ("liquidation_bonus", "Liquidation Bonus", 3, "percent", 100),
    ("supply_cap", "Supply Cap", 11, "token_units", 1),
    ("borrow_cap", "Borrow Cap", 12, "token_units", 1),
]

COMPOUND_ASSETS = {
    "weth": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "wbtc": "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
}

COMPOUND_PARAM_SPECS = [
    ("borrow_collateral_factor", "Borrow Collateral Factor", 1, "percent", 10**18),
    ("liquidate_collateral_factor", "Liquidate Collateral Factor", 2, "percent", 10**18),
]


def _build_protocol_parameter_registry() -> dict:
    """Build the full parameter registry. Returns dict of protocol_slug -> list of param dicts."""
    registry = {}

    # --- Aave V3 Pool ---
    aave_pool = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"
    aave_params = []
    for asset_symbol, asset_addr in WATCHED_ASSETS.items():
        for key, name, field_idx, unit, norm in AAVE_PARAM_SPECS:
            aave_params.append({
                "contract_address": aave_pool,
                "chain": "ethereum",
                "parameter_key": f"aave_{asset_symbol}_{key}",
                "parameter_name": f"{asset_symbol.upper()} {name}",
                "function_selector": AAVE_GET_RESERVE_DATA,
                "asset_address": asset_addr,
                "asset_symbol": asset_symbol.upper(),
                "field_index": field_idx,
                "value_unit": unit,
                "normalization_factor": norm,
            })
    registry["aave"] = aave_params

    # --- Compound V3 Comet USDC ---
    compound_comet = "0xc3d688B66703497DAA19211EEdff47f25384cdc3"
    compound_params = []
    for asset_symbol, asset_addr in COMPOUND_ASSETS.items():
        for key, name, field_idx, unit, norm in COMPOUND_PARAM_SPECS:
            compound_params.append({
                "contract_address": compound_comet,
                "chain": "ethereum",
                "parameter_key": f"compound_{asset_symbol}_{key}",
                "parameter_name": f"{asset_symbol.upper()} {name}",
                "function_selector": COMPOUND_GET_ASSET_INFO,
                "asset_address": asset_addr,
                "asset_symbol": asset_symbol.upper(),
                "field_index": field_idx,
                "value_unit": unit,
                "normalization_factor": norm,
            })
    registry["compound-finance"] = compound_params

    # --- Morpho Blue ---
    # Morpho uses market(bytes32) which requires market IDs. Placeholder for
    # known markets — will need market ID discovery for full coverage.
    # Skipping for initial launch; add when market IDs are catalogued.

    return registry


PROTOCOL_PARAMETER_REGISTRY = _build_protocol_parameter_registry()


# ---------------------------------------------------------------------------
# On-chain parameter reading
# ---------------------------------------------------------------------------

def _read_parameter_value(param_spec: dict) -> tuple[str | None, int | None]:
    """
    Read a single parameter value from on-chain.
    Returns (raw_hex_value, decoded_int) or (None, None) on failure.
    """
    contract = param_spec["contract_address"]
    chain = param_spec["chain"]
    selector = param_spec["function_selector"]
    asset_addr = param_spec.get("asset_address")

    # Build calldata
    if asset_addr:
        calldata = selector + _encode_address_param(asset_addr)
    else:
        calldata = selector

    result = _eth_call_sync(contract, calldata, chain)
    if not result or result == "0x":
        return None, None

    field_idx = param_spec["field_index"]
    raw_value = _decode_uint256(result, field_idx)
    return str(raw_value), raw_value


# ---------------------------------------------------------------------------
# Score context
# ---------------------------------------------------------------------------

def _get_concurrent_scores(protocol_slug: str, asset_symbol: str | None) -> dict:
    """Fetch concurrent SII/PSI scores and compute change context."""
    context = {
        "concurrent_sii_score": None,
        "concurrent_psi_score": None,
        "hours_since_last_sii_change": None,
        "sii_trend_7d": None,
        "change_context": "unknown",
    }

    # PSI score
    try:
        psi_row = fetch_one(
            """SELECT overall_score FROM psi_scores
               WHERE protocol_slug = %s
               ORDER BY computed_at DESC LIMIT 1""",
            (protocol_slug,),
        )
        if psi_row:
            context["concurrent_psi_score"] = float(psi_row["overall_score"])
    except Exception:
        pass

    if not asset_symbol:
        return context

    # SII score
    try:
        sii_row = fetch_one(
            """SELECT overall_score FROM scores
               WHERE stablecoin_id = LOWER(%s)
               ORDER BY scored_at DESC LIMIT 1""",
            (asset_symbol,),
        )
        if sii_row:
            context["concurrent_sii_score"] = float(sii_row["overall_score"])
    except Exception:
        pass

    # 7-day SII trend
    try:
        trend_rows = fetch_all(
            """SELECT overall_score, score_date FROM score_history
               WHERE stablecoin = LOWER(%s)
                 AND score_date > CURRENT_DATE - INTERVAL '7 days'
               ORDER BY score_date ASC""",
            (asset_symbol,),
        )
        if trend_rows and len(trend_rows) >= 2:
            first = float(trend_rows[0]["overall_score"])
            last = float(trend_rows[-1]["overall_score"])
            context["sii_trend_7d"] = round(last - first, 2)
    except Exception:
        pass

    # Hours since last SII change (>1 point move)
    try:
        last_move = fetch_one(
            """SELECT score_date FROM score_history
               WHERE stablecoin = LOWER(%s)
                 AND ABS(daily_change) > 1
               ORDER BY score_date DESC LIMIT 1""",
            (asset_symbol,),
        )
        if last_move and last_move.get("score_date"):
            move_date = last_move["score_date"]
            if hasattr(move_date, "isoformat"):
                delta = datetime.now(timezone.utc) - datetime.combine(
                    move_date, datetime.min.time(), tzinfo=timezone.utc
                )
                context["hours_since_last_sii_change"] = round(delta.total_seconds() / 3600, 2)
    except Exception:
        pass

    # Determine change context
    trend = context.get("sii_trend_7d")
    if trend is not None:
        if trend < -1:
            context["change_context"] = "reactive"
        elif trend >= 0:
            context["change_context"] = "proactive"

    return context


# ---------------------------------------------------------------------------
# Change handling
# ---------------------------------------------------------------------------

def _handle_parameter_change(
    protocol_slug: str,
    protocol_id: int,
    param_spec: dict,
    old_value: float,
    old_raw: str,
    new_value: float,
    new_raw: str,
    results: dict,
):
    """Record a parameter change with concurrent score context."""
    now = datetime.now(timezone.utc)
    change_magnitude = round(abs(new_value - old_value), 4)
    change_direction = "increase" if new_value > old_value else "decrease" if new_value < old_value else "unchanged"

    # Get concurrent score state
    asset_symbol = param_spec.get("asset_symbol")
    score_ctx = _get_concurrent_scores(protocol_slug, asset_symbol)

    content_data = (
        f"{protocol_slug}{param_spec['parameter_key']}"
        f"{param_spec.get('asset_address', '')}{new_raw}{now.isoformat()}"
    )
    content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

    execute(
        """INSERT INTO protocol_parameter_changes
            (protocol_slug, protocol_id, parameter_name, parameter_key,
             asset_address, asset_symbol, contract_address, chain,
             previous_value, previous_value_raw, new_value, new_value_raw,
             value_unit, change_magnitude, change_direction,
             changed_at, concurrent_sii_score, concurrent_psi_score,
             hours_since_last_sii_change, sii_trend_7d, change_context,
             content_hash, attested_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                   %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
        (
            protocol_slug, protocol_id,
            param_spec["parameter_name"], param_spec["parameter_key"],
            param_spec.get("asset_address"), asset_symbol,
            param_spec["contract_address"], param_spec["chain"],
            old_value, old_raw, new_value, new_raw,
            param_spec["value_unit"], change_magnitude, change_direction,
            now,
            score_ctx["concurrent_sii_score"], score_ctx["concurrent_psi_score"],
            score_ctx["hours_since_last_sii_change"], score_ctx["sii_trend_7d"],
            score_ctx["change_context"],
            content_hash,
        ),
    )

    try:
        from app.state_attestation import attest_state
        attest_state("protocol_parameter_changes", [{
            "protocol_slug": protocol_slug,
            "parameter_key": param_spec["parameter_key"],
            "new_value": new_value,
            "change_context": score_ctx["change_context"],
            "changed_at": now.isoformat(),
        }], str(protocol_id))
    except Exception:
        pass

    logger.info(
        f"PARAMETER CHANGE: {protocol_slug} {param_spec['parameter_name']} "
        f"{old_value} -> {new_value} ({change_direction}, "
        f"context={score_ctx['change_context']})"
    )
    results["changes_detected"] += 1


# ---------------------------------------------------------------------------
# Snapshot storage
# ---------------------------------------------------------------------------

def _store_daily_snapshot(protocol_slug: str, protocol_id: int, results: dict):
    """Store a daily point-in-time snapshot of all parameter values."""
    today = date.today()

    existing = fetch_one(
        """SELECT id FROM protocol_parameter_snapshots
           WHERE protocol_slug = %s AND snapshot_date = %s""",
        (protocol_slug, today),
    )
    if existing:
        return

    params = fetch_all(
        """SELECT parameter_key, current_value, value_unit, asset_symbol
           FROM protocol_parameters WHERE protocol_slug = %s""",
        (protocol_slug,),
    )
    if not params:
        return

    param_dict = {}
    for p in params:
        param_dict[p["parameter_key"]] = {
            "value": float(p["current_value"]) if p.get("current_value") is not None else None,
            "unit": p.get("value_unit"),
            "asset_symbol": p.get("asset_symbol"),
        }

    content_data = f"{protocol_slug}{today.isoformat()}{json.dumps(param_dict, sort_keys=True)}"
    content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

    execute(
        """INSERT INTO protocol_parameter_snapshots
            (protocol_slug, protocol_id, snapshot_date, parameters,
             parameter_count, content_hash, attested_at)
           VALUES (%s, %s, %s, %s, %s, %s, NOW())
           ON CONFLICT (protocol_slug, snapshot_date) DO NOTHING""",
        (
            protocol_slug, protocol_id, today,
            json.dumps(param_dict), len(param_dict), content_hash,
        ),
    )

    try:
        from app.state_attestation import attest_state
        attest_state("protocol_parameter_snapshots", [{
            "protocol_slug": protocol_slug,
            "snapshot_date": today.isoformat(),
            "parameter_count": len(param_dict),
        }], str(protocol_id))
    except Exception:
        pass

    results["snapshots_stored"] += 1


# ---------------------------------------------------------------------------
# Core check (used by both fast and slow cycle)
# ---------------------------------------------------------------------------

def check_parameter_changes() -> dict:
    """
    Check all watched parameters for changes. Used in fast cycle.
    Does NOT store daily snapshots — that's done in the slow cycle.
    Returns summary dict.
    """
    results = {
        "protocols_checked": 0,
        "parameters_checked": 0,
        "changes_detected": 0,
        "errors": [],
    }

    for protocol_slug, param_specs in PROTOCOL_PARAMETER_REGISTRY.items():
        try:
            protocol_id = 0

            for spec in param_specs:
                try:
                    raw_str, raw_int = _read_parameter_value(spec)
                    if raw_int is None:
                        continue

                    norm = spec["normalization_factor"]
                    normalized = raw_int / norm if norm else raw_int
                    results["parameters_checked"] += 1

                    # Look up stored value
                    stored = fetch_one(
                        """SELECT current_value, current_value_raw
                           FROM protocol_parameters
                           WHERE protocol_slug = %s AND parameter_key = %s
                             AND COALESCE(asset_address, '') = COALESCE(%s, '')
                             AND chain = %s""",
                        (protocol_slug, spec["parameter_key"],
                         spec.get("asset_address"), spec["chain"]),
                    )

                    if not stored:
                        # First capture
                        execute(
                            """INSERT INTO protocol_parameters
                                (protocol_slug, protocol_id, parameter_name, parameter_key,
                                 asset_address, asset_symbol, contract_address, chain,
                                 current_value, current_value_raw, value_unit)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                               ON CONFLICT (protocol_slug, parameter_key, asset_address, chain)
                               DO NOTHING""",
                            (
                                protocol_slug, protocol_id,
                                spec["parameter_name"], spec["parameter_key"],
                                spec.get("asset_address"), spec.get("asset_symbol"),
                                spec["contract_address"], spec["chain"],
                                normalized, raw_str, spec["value_unit"],
                            ),
                        )
                        continue

                    stored_value = float(stored["current_value"]) if stored.get("current_value") is not None else None
                    if stored_value is not None and abs(normalized - stored_value) > 1e-8:
                        # Change detected
                        _handle_parameter_change(
                            protocol_slug, protocol_id, spec,
                            stored_value, stored.get("current_value_raw", ""),
                            normalized, raw_str,
                            results,
                        )
                        # Update current value
                        execute(
                            """UPDATE protocol_parameters
                               SET current_value = %s, current_value_raw = %s,
                                   last_updated_at = NOW()
                               WHERE protocol_slug = %s AND parameter_key = %s
                                 AND COALESCE(asset_address, '') = COALESCE(%s, '')
                                 AND chain = %s""",
                            (
                                normalized, raw_str,
                                protocol_slug, spec["parameter_key"],
                                spec.get("asset_address"), spec["chain"],
                            ),
                        )

                    time.sleep(0.3)  # Rate limit RPC calls

                except Exception as e:
                    logger.debug(f"Parameter read failed: {protocol_slug}/{spec['parameter_key']}: {e}")

            results["protocols_checked"] += 1

        except Exception as e:
            results["errors"].append(f"{protocol_slug}: {e}")
            logger.error(f"Parameter history failed for {protocol_slug}: {e}")

    return results


# ---------------------------------------------------------------------------
# Main collector (slow cycle — full run with snapshots)
# ---------------------------------------------------------------------------

async def collect_parameter_history() -> dict:
    """
    Full parameter history collection: check for changes + store daily snapshots.
    Returns summary dict.
    """
    results = check_parameter_changes()
    results["snapshots_stored"] = 0

    # Store daily snapshots for each protocol
    for protocol_slug in PROTOCOL_PARAMETER_REGISTRY:
        try:
            _store_daily_snapshot(protocol_slug, 0, results)
        except Exception as e:
            logger.debug(f"Parameter snapshot failed for {protocol_slug}: {e}")

    logger.info(
        f"Parameter history: protocols={results['protocols_checked']} "
        f"params={results['parameters_checked']} "
        f"changes={results['changes_detected']} "
        f"snapshots={results['snapshots_stored']} "
        f"errors={len(results['errors'])}"
    )
    return results
