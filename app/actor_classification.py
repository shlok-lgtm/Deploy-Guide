"""
Actor Classification — Primitive #21
=====================================
Deterministic, rule-based classifier that assigns every wallet in the entity
graph an actor type: autonomous_agent, human, contract_vault, or unknown.

No ML.  Same transaction history → same classification.
Computation attestation preserved via classification_hash.

Features are derived from data already in wallet_graph.wallet_edges and
wallet_graph.wallets — no new data collection required.
"""

import asyncio
import hashlib
import json
import logging
import math
from datetime import datetime, timezone

from app.database import fetch_all, fetch_one, execute

logger = logging.getLogger(__name__)

METHODOLOGY_VERSION = "ACL-v1.0"
VALID_ACTOR_TYPES = {"autonomous_agent", "human", "contract_vault", "unknown"}

# Minimum transactions (edges) in the last 90 days to attempt classification
MIN_TX_COUNT = 20
# Thresholds for actor_type from agent_probability
AGENT_THRESHOLD = 0.75
HUMAN_THRESHOLD = 0.25

# Feature weights (sum to 1.0)
FEATURE_WEIGHTS = {
    "cadence_regularity": 0.25,
    "active_hours_entropy": 0.20,
    "counterparty_concentration": 0.20,
    "max_idle_gap_hours": 0.15,
    "value_regularity": 0.20,
}


# ---------------------------------------------------------------------------
# Feature extraction — all from wallet_graph.wallet_edges
# ---------------------------------------------------------------------------

def _extract_features(address: str) -> dict | None:
    """Extract classification features from edge history for one wallet.

    Returns dict of feature_name → raw_value, or None if insufficient data.
    """
    addr = address.lower()

    # Fetch recent edges involving this wallet (last 90 days)
    rows = fetch_all(
        """
        SELECT from_address, to_address, transfer_count, total_value_usd,
               first_transfer_at, last_transfer_at
        FROM wallet_graph.wallet_edges
        WHERE (LOWER(from_address) = %s OR LOWER(to_address) = %s)
          AND last_transfer_at > NOW() - INTERVAL '90 days'
        """,
        (addr, addr),
    )

    if not rows:
        return None

    total_transfers = sum(r["transfer_count"] or 1 for r in rows)
    if total_transfers < MIN_TX_COUNT:
        return None

    # --- Feature 1: Cadence regularity (CV of inter-edge time gaps) ---
    timestamps = []
    for r in rows:
        if r["first_transfer_at"]:
            timestamps.append(r["first_transfer_at"])
        if r["last_transfer_at"]:
            timestamps.append(r["last_transfer_at"])
    timestamps.sort()

    cadence_regularity = 0.5  # default (neutral)
    if len(timestamps) >= 3:
        gaps = [
            (timestamps[i + 1] - timestamps[i]).total_seconds()
            for i in range(len(timestamps) - 1)
            if (timestamps[i + 1] - timestamps[i]).total_seconds() > 0
        ]
        if gaps:
            mean_gap = sum(gaps) / len(gaps)
            if mean_gap > 0:
                std_gap = math.sqrt(sum((g - mean_gap) ** 2 for g in gaps) / len(gaps))
                cv = std_gap / mean_gap
                # Low CV = regular (agent-like). CV < 0.3 → 1.0, CV > 2.0 → 0.0
                cadence_regularity = max(0.0, min(1.0, 1.0 - (cv - 0.3) / 1.7))

    # --- Feature 2: Active hours entropy ---
    hour_counts = [0] * 24
    for r in rows:
        for ts in [r["first_transfer_at"], r["last_transfer_at"]]:
            if ts:
                hour_counts[ts.hour] += 1
    total_hour_hits = sum(hour_counts)
    hours_entropy = 0.0
    if total_hour_hits > 0:
        for c in hour_counts:
            if c > 0:
                p = c / total_hour_hits
                hours_entropy -= p * math.log2(p)
        # Max entropy for 24 bins = log2(24) ≈ 4.585
        # Agents → high entropy (uniform). Humans → low entropy (clustered).
        max_entropy = math.log2(24)
        hours_entropy_norm = hours_entropy / max_entropy  # 0 to 1, 1 = agent-like

    # --- Feature 3: Counterparty concentration ---
    counterparties = set()
    for r in rows:
        other = r["to_address"] if r["from_address"].lower() == addr else r["from_address"]
        counterparties.add(other.lower())
    unique_counterparties = len(counterparties)
    # Low diversity (few counterparties, many txns) = agent-like (repetitive patterns)
    diversity_ratio = unique_counterparties / max(total_transfers, 1)
    # Invert: low diversity → high agent score
    counterparty_concentration = max(0.0, min(1.0, 1.0 - diversity_ratio))

    # --- Feature 4: Max idle gap (hours) ---
    max_idle_hours = 0.0
    if len(timestamps) >= 2:
        max_gap_seconds = max(
            (timestamps[i + 1] - timestamps[i]).total_seconds()
            for i in range(len(timestamps) - 1)
        )
        max_idle_hours = max_gap_seconds / 3600
    # Short idle = agent-like. <4h → 1.0, >72h → 0.0
    idle_score = max(0.0, min(1.0, 1.0 - (max_idle_hours - 4) / 68))

    # --- Feature 5: Value regularity (round-number transactions) ---
    round_count = 0
    value_count = 0
    for r in rows:
        val = float(r["total_value_usd"] or 0)
        if val > 0:
            value_count += 1
            # Check if value is a round number (divisible by 100, or exact integer)
            if val >= 100 and (val % 100 < 0.01 or val % 100 > 99.99):
                round_count += 1
            elif val == int(val):
                round_count += 1
    value_regularity = round_count / max(value_count, 1)

    return {
        "cadence_regularity": round(cadence_regularity, 4),
        "active_hours_entropy": round(hours_entropy_norm, 4),
        "counterparty_concentration": round(counterparty_concentration, 4),
        "max_idle_gap_hours": round(idle_score, 4),
        "value_regularity": round(value_regularity, 4),
        "_tx_count": total_transfers,
        "_unique_counterparties": unique_counterparties,
        "_max_idle_hours_raw": round(max_idle_hours, 1),
    }


# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------

def _compute_agent_probability(features: dict) -> float:
    """Weighted average of feature scores → agent_probability [0, 1]."""
    total = 0.0
    weight_sum = 0.0
    for fname, w in FEATURE_WEIGHTS.items():
        val = features.get(fname)
        if val is not None:
            total += val * w
            weight_sum += w
    if weight_sum == 0:
        return 0.5
    return round(total / weight_sum, 4)


def _determine_type(prob: float, is_contract: bool) -> str:
    if is_contract:
        return "contract_vault"
    if prob >= AGENT_THRESHOLD:
        return "autonomous_agent"
    if prob <= HUMAN_THRESHOLD:
        return "human"
    return "unknown"


def _determine_confidence(prob: float, tx_count: int) -> str:
    if tx_count >= 50 and (prob >= 0.85 or prob <= 0.15):
        return "high"
    if tx_count >= 30 and (prob >= AGENT_THRESHOLD or prob <= HUMAN_THRESHOLD):
        return "medium"
    return "low"


def _classification_hash(features: dict, methodology: str) -> str:
    """SHA-256 of canonical feature vector + methodology for attestation."""
    # Strip internal keys
    public_features = {k: v for k, v in features.items() if not k.startswith("_")}
    canonical = json.dumps(
        {"features": public_features, "methodology": methodology},
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_wallet(address: str) -> dict | None:
    """Classify one wallet. Upserts actor_classifications, updates wallet_profiles,
    logs to history if type changed.

    Returns classification dict or None if insufficient data.
    """
    addr = address.lower()

    # Check is_contract from wallets table
    wallet_row = fetch_one(
        "SELECT is_contract FROM wallet_graph.wallets WHERE LOWER(address) = %s LIMIT 1",
        (addr,),
    )
    is_contract = bool(wallet_row and wallet_row.get("is_contract"))

    # Contract vaults get classified directly
    if is_contract:
        features = {"_contract": True, "_tx_count": 0}
        prob = 0.5
        actor_type = "contract_vault"
        confidence = "high"
        chash = _classification_hash(features, METHODOLOGY_VERSION)
    else:
        features = _extract_features(addr)
        if features is None:
            return None

        prob = _compute_agent_probability(features)
        actor_type = _determine_type(prob, False)
        confidence = _determine_confidence(prob, features.get("_tx_count", 0))
        chash = _classification_hash(features, METHODOLOGY_VERSION)

    tx_count = features.get("_tx_count", 0)

    # Check previous classification for history tracking
    prev = fetch_one(
        "SELECT actor_type FROM wallet_graph.actor_classifications WHERE wallet_address = %s",
        (addr,),
    )
    previous_type = prev["actor_type"] if prev else None

    # Upsert classification
    execute(
        """
        INSERT INTO wallet_graph.actor_classifications
            (wallet_address, actor_type, agent_probability, confidence,
             feature_vector, tx_count_basis, methodology_version,
             classification_hash, classified_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (wallet_address) DO UPDATE SET
            actor_type = EXCLUDED.actor_type,
            agent_probability = EXCLUDED.agent_probability,
            confidence = EXCLUDED.confidence,
            feature_vector = EXCLUDED.feature_vector,
            tx_count_basis = EXCLUDED.tx_count_basis,
            methodology_version = EXCLUDED.methodology_version,
            classification_hash = EXCLUDED.classification_hash,
            classified_at = NOW(),
            updated_at = NOW()
        """,
        (
            addr, actor_type, prob, confidence,
            json.dumps(features), tx_count, METHODOLOGY_VERSION,
            chash,
        ),
    )

    # Update wallet_profiles denormalized columns
    execute(
        """
        UPDATE wallet_graph.wallet_profiles
        SET actor_type = %s, agent_probability = %s
        WHERE LOWER(address) = %s
        """,
        (actor_type, prob, addr),
    )

    # Log to history if type changed
    if previous_type is not None and previous_type != actor_type:
        execute(
            """
            INSERT INTO wallet_graph.actor_classification_history
                (wallet_address, actor_type, agent_probability, previous_type, methodology_version)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (addr, actor_type, prob, previous_type, METHODOLOGY_VERSION),
        )
        logger.info(f"Reclassified {addr[:12]}…: {previous_type} → {actor_type} (p={prob:.2f})")

    return {
        "wallet_address": addr,
        "actor_type": actor_type,
        "agent_probability": prob,
        "confidence": confidence,
        "tx_count_basis": tx_count,
        "classification_hash": chash,
    }


def classify_all_active(limit: int = 2000) -> dict:
    """Classify all wallets with sufficient edge history.

    Targets wallets that either:
      - Have never been classified, OR
      - Were classified >24h ago (re-evaluation)

    Returns summary dict.
    """
    rows = fetch_all(
        """
        WITH addr_transfer_totals AS (
            SELECT addr, SUM(transfer_count) AS total_transfers
            FROM (
                SELECT LOWER(from_address) AS addr, transfer_count
                FROM wallet_graph.wallet_edges
                WHERE last_transfer_at > NOW() - INTERVAL '90 days'
                UNION ALL
                SELECT LOWER(to_address) AS addr, transfer_count
                FROM wallet_graph.wallet_edges
                WHERE last_transfer_at > NOW() - INTERVAL '90 days'
            ) edges_both_sides
            GROUP BY addr
            HAVING SUM(transfer_count) >= %s
        )
        SELECT w.address
        FROM wallet_graph.wallets w
        INNER JOIN addr_transfer_totals a ON LOWER(w.address) = a.addr
        LEFT JOIN wallet_graph.actor_classifications ac
          ON ac.wallet_address = LOWER(w.address)
        WHERE ac.wallet_address IS NULL
           OR ac.classified_at < NOW() - INTERVAL '24 hours'
        ORDER BY ac.classified_at NULLS FIRST, a.total_transfers DESC
        LIMIT %s
        """,
        (MIN_TX_COUNT, limit),
    )

    classified = 0
    skipped = 0
    reclassified = 0
    by_type = {"autonomous_agent": 0, "human": 0, "contract_vault": 0, "unknown": 0}

    for r in rows:
        try:
            result = classify_wallet(r["address"])
            if result:
                classified += 1
                by_type[result["actor_type"]] = by_type.get(result["actor_type"], 0) + 1
            else:
                skipped += 1
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Classification error for {r['address'][:12]}…: {e}")
            skipped += 1

    # Count reclassifications from this run
    reclass_row = fetch_one(
        """
        SELECT COUNT(*) AS cnt FROM wallet_graph.actor_classification_history
        WHERE classified_at > NOW() - INTERVAL '5 minutes'
        """
    )
    reclassified = reclass_row["cnt"] if reclass_row else 0

    logger.info(
        f"Actor classification complete: {classified} classified, {skipped} skipped, "
        f"{reclassified} reclassified — "
        f"agents={by_type['autonomous_agent']}, humans={by_type['human']}, "
        f"vaults={by_type['contract_vault']}, unknown={by_type['unknown']}"
    )

    # Attest actor classifications
    try:
        from app.state_attestation import attest_state
        if classified > 0:
            attest_state("actors", [{"classified": classified, "reclassified": reclassified, "by_type": by_type}])
        else:
            attest_state("actors", [{"status": "ran_no_results", "results_count": 0}])
    except Exception as ae:
        logger.error(f"Actor attestation FAILED: {ae}")
        from app.worker import _record_cycle_error
        _record_cycle_error(
            error_type="actor_attestation_failure",
            error_message=str(ae)[:500],
            cycle_phase="actor_classification",
        )

    return {
        "classified": classified,
        "skipped": skipped,
        "reclassified": reclassified,
        "by_type": by_type,
        "methodology": METHODOLOGY_VERSION,
    }
