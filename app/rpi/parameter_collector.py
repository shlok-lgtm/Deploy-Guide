"""
RPI Parameter Change Tracker
==============================
Monitors on-chain parameter changes for DeFi protocols via Etherscan.
Tracks admin/governance contract interactions that modify risk parameters.
"""

import logging
import os
import time
from datetime import datetime, timezone

import requests

from app.database import execute, fetch_all, fetch_one

logger = logging.getLogger(__name__)

ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY", "")
ETHERSCAN_BASE = "https://api.etherscan.io/v2/api"

# Protocol admin/governance contract addresses and function signatures
# that correspond to risk parameter changes.
PROTOCOL_CONFIGS = {
    "aave": {
        "contracts": [
            {
                "address": "0x8689b8add004a9fd2320031b7d3f5af4dced0e43",  # PoolConfigurator V3
                "name": "PoolConfigurator",
                "functions": [
                    "setReserveBorrowing",
                    "configureReserveAsCollateral",
                    "setReserveFactor",
                    "setBorrowCap",
                    "setSupplyCap",
                    "setDebtCeiling",
                    "setLiquidationProtocolFee",
                    "setEModeCategory",
                    "setReserveFreeze",
                    "setReservePause",
                    "updateBridgeProtocolFee",
                    "updateFlashloanPremiumTotal",
                ],
            },
        ],
    },
    "compound-finance": {
        "contracts": [
            {
                "address": "0xc3d688B66703497DAA19211EEdff47f25384cdc3",  # cUSDCv3 Comet
                "name": "CometConfiguration",
                "functions": [
                    "setFactory",
                    "setGovernor",
                    "setPauseGuardian",
                    "setBaseTokenPriceFeed",
                    "setExtensionDelegate",
                    "updateAssetBorrowCollateralFactor",
                    "updateAssetLiquidateCollateralFactor",
                    "updateAssetLiquidationFactor",
                    "updateAssetSupplyCap",
                ],
            },
        ],
    },
    "uniswap": {
        "contracts": [
            {
                "address": "0x1a9C8182C09F50C8318d769245beA52c32BE35BC",  # Uniswap Governance
                "name": "GovernorBravo",
                "functions": [
                    "_setProposalThreshold",
                    "_setVotingDelay",
                    "_setVotingPeriod",
                ],
            },
        ],
    },
    "sky": {
        "contracts": [
            {
                "address": "0x135954d155898D42C90D2a57824C690e0c7BEf1B",  # MCD_JUG (stability fees)
                "name": "Jug",
                "functions": [
                    "drip",
                    "file",
                ],
            },
        ],
    },
    "curve-finance": {
        "contracts": [
            {
                "address": "0x5F890841f657d90E081bAbdB532A05996Af79Fe6",  # Curve DAO Ownership
                "name": "CurveOwnership",
                "functions": [
                    "commit_set_admins",
                    "apply_set_admins",
                    "set_killed",
                ],
            },
        ],
    },
}


def fetch_contract_txns(contract_address: str, start_block: int = 0) -> list[dict]:
    """Fetch recent transactions to a contract from Etherscan."""
    if not ETHERSCAN_API_KEY:
        logger.debug("No ETHERSCAN_API_KEY — skipping parameter tracking")
        return []

    time.sleep(0.25)  # rate limit
    try:
        resp = requests.get(
            ETHERSCAN_BASE,
            params={
                "module": "account",
                "action": "txlist",
                "address": contract_address,
                "startblock": start_block,
                "endblock": 99999999,
                "page": 1,
                "offset": 100,
                "sort": "desc",
                "apikey": ETHERSCAN_API_KEY,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == "1":
                return data.get("result", [])
    except Exception as e:
        logger.warning(f"Etherscan fetch failed for {contract_address}: {e}")
    return []


def _match_function(input_data: str, function_names: list[str]) -> str | None:
    """Check if a tx input matches any of the tracked function signatures."""
    if not input_data or len(input_data) < 10:
        return None
    # Function selector is first 4 bytes (8 hex chars) after 0x
    selector = input_data[:10]
    # We don't have the full ABI, so we match by function name heuristic:
    # Check if any known function selectors match.
    # For a production system, we'd precompute keccak256 selectors.
    # For now, we log all successful txns to the governance contracts
    # as potential parameter changes.
    return "unknown_function"


def collect_parameter_changes():
    """Collect on-chain parameter changes for all configured protocols."""
    total_stored = 0

    for slug, config in PROTOCOL_CONFIGS.items():
        for contract_cfg in config["contracts"]:
            address = contract_cfg["address"]
            name = contract_cfg["name"]
            functions = contract_cfg["functions"]

            # Get the latest block we've seen for this contract
            last_row = fetch_one("""
                SELECT MAX(block_number) AS latest_block
                FROM parameter_changes
                WHERE protocol_slug = %s AND contract_address = %s
            """, (slug, address.lower()))
            start_block = (last_row["latest_block"] or 0) + 1 if last_row else 0

            txns = fetch_contract_txns(address, start_block)
            logger.info(f"RPI params: {slug}/{name} — {len(txns)} txns since block {start_block}")

            for tx in txns:
                # Only count successful transactions
                if tx.get("isError") == "1" or tx.get("txreceipt_status") == "0":
                    continue

                # Only count transactions TO the governance contract (not from)
                if tx.get("to", "").lower() != address.lower():
                    continue

                input_data = tx.get("input", "")
                # Filter: only transactions with non-trivial input (function calls)
                if not input_data or input_data == "0x" or len(input_data) < 10:
                    continue

                tx_hash = tx.get("hash", "")
                block_num = int(tx.get("blockNumber", 0))
                ts = int(tx.get("timeStamp", 0))
                detected_at = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None

                try:
                    execute("""
                        INSERT INTO parameter_changes
                            (protocol_slug, tx_hash, block_number, parameter_type,
                             function_signature, contract_address, chain, detected_at)
                        VALUES (%s, %s, %s, %s, %s, %s, 'ethereum', %s)
                        ON CONFLICT (protocol_slug, tx_hash) DO NOTHING
                    """, (
                        slug, tx_hash, block_num, name,
                        input_data[:10], address.lower(), detected_at,
                    ))
                    total_stored += 1
                except Exception as e:
                    logger.debug(f"Failed to store param change {tx_hash}: {e}")

    logger.info(f"RPI parameter collector: {total_stored} changes stored")
    return total_stored


def get_parameter_velocity(protocol_slug: str, days: int = 30) -> int:
    """Get count of parameter changes in the last N days for a protocol."""
    row = fetch_one("""
        SELECT COUNT(*) AS cnt
        FROM parameter_changes
        WHERE protocol_slug = %s
          AND detected_at >= NOW() - INTERVAL '%s days'
    """ % ('%s', days), (protocol_slug,))
    return row["cnt"] if row else 0


def get_parameter_recency(protocol_slug: str) -> int | None:
    """Get days since the most recent parameter change."""
    row = fetch_one("""
        SELECT EXTRACT(DAY FROM NOW() - MAX(detected_at))::INT AS days_since
        FROM parameter_changes
        WHERE protocol_slug = %s
    """, (protocol_slug,))
    return row["days_since"] if row and row["days_since"] is not None else None
