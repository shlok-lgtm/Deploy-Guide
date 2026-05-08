"""
Data Coherence Guards — tuned for high-volume collection
=========================================================
Validates incoming data against the previous snapshot before storage.
Prevents silent corruption from bad API responses.

Per-data-type configurable thresholds:
- Wallet balances: >95% drop flagged (whales exit pools legitimately)
- Exchange data: flag only trust score change or exchange disappears
- DEX pools: flag zero or negative values only
- Entity snapshots: flag null/empty returns, not market swings
- Mint/burn: no coherence check (events, not snapshots)
- Bridge flows: flag negative volumes only

Flagged data is STORED with a violation record — never rejected.
Violations go to coherence_violations table for ops review.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# =============================================================================
# Per-data-type thresholds — configurable
# =============================================================================

THRESHOLDS = {
    "liquidity_depth": {
        "max_volume_drop_pct": 98,      # DEX/CEX volume swings are normal
        "max_depth_drop_pct": 95,       # Liquidity can move fast
        "flag_zero_values": True,
        "flag_negative_values": True,
    },
    "yield_snapshots": {
        "max_tvl_drop_pct": 90,         # TVL crashes >90% are real incidents
        "flag_negative_apy": True,
        "flag_negative_tvl": True,
    },
    "exchange_snapshots": {
        "max_volume_drop_pct": 99,      # Exchange volume swings 50%+ daily — only flag near-zero
        "flag_trust_score_change": True, # Trust score changes are significant
        "flag_exchange_disappears": True,
    },
    "bridge_flows": {
        "flag_negative_volume": True,
        "flag_negative_tvl": True,
    },
    "peg_snapshots_5m": {
        "max_price_deviation_pct": 10,  # Stablecoins shouldn't deviate >10%
    },
    "entity_snapshots_hourly": {
        "flag_null_return": True,       # Flag if entity returns empty data
        "max_market_cap_drop_pct": 99,  # Only flag near-total disappearance
    },
    "contract_surveillance": {
        "flag_source_change": True,     # Any source code change is noteworthy
    },
    "wallet_balances": {
        "max_drop_pct": 95,            # Whale exits are real — only flag 95%+
        "flag_top100_drop_90pct": True, # Tighter for top-100 wallets
    },
}


class CoherenceViolation:
    """Represents a data coherence violation."""

    def __init__(
        self,
        data_type: str,
        entity_id: str,
        field_name: str,
        previous_value: Optional[float],
        incoming_value: Optional[float],
        violation_type: str,
        severity: str = "warning",
        details: Optional[str] = None,
    ):
        self.data_type = data_type
        self.entity_id = entity_id
        self.field_name = field_name
        self.previous_value = previous_value
        self.incoming_value = incoming_value
        self.violation_type = violation_type
        self.severity = severity
        self.details = details

    def to_dict(self) -> dict:
        return {
            "data_type": self.data_type,
            "entity_id": self.entity_id,
            "field_name": self.field_name,
            "previous_value": self.previous_value,
            "incoming_value": self.incoming_value,
            "violation_type": self.violation_type,
            "severity": self.severity,
            "details": self.details,
        }


def store_violation(violation: CoherenceViolation):
    """Store a violation in the coherence_violations table."""
    try:
        from app.database import execute
        execute(
            """INSERT INTO coherence_violations
               (data_type, entity_id, field_name, violation_type, severity,
                previous_value, incoming_value, details, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
            (
                violation.data_type, violation.entity_id,
                violation.field_name, violation.violation_type,
                violation.severity,
                violation.previous_value, violation.incoming_value,
                violation.details,
            ),
        )
    except Exception as e:
        # Fall back to old coherence_reports table if new table not yet migrated
        try:
            from app.database import execute as _exec
            _exec(
                """INSERT INTO coherence_reports
                   (check_name, status, details, created_at)
                   VALUES (%s, %s, %s, NOW())""",
                (
                    f"guard:{violation.data_type}:{violation.entity_id}",
                    violation.severity,
                    json.dumps(violation.to_dict()),
                ),
            )
        except Exception as e2:
            logger.warning(f"Could not store coherence violation: primary={e}, fallback={e2}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="data_layer_store_violation_failure",
                    error_message=f"primary={str(e)[:200]}, fallback={str(e2)[:200]}"[:500],
                    cycle_phase="coherence_guards",
                )
            except Exception:
                pass


# =============================================================================
# Check functions — generic, parameterized by threshold
# =============================================================================

def check_numeric_drop(
    data_type: str, entity_id: str, field_name: str,
    previous: Optional[float], incoming: Optional[float],
    max_drop_pct: float = 95.0,
) -> Optional[CoherenceViolation]:
    """Flag if a numeric value drops by more than max_drop_pct."""
    if previous is None or incoming is None or previous <= 0:
        return None
    drop_pct = ((previous - incoming) / previous) * 100
    if drop_pct > max_drop_pct:
        return CoherenceViolation(
            data_type=data_type, entity_id=entity_id, field_name=field_name,
            previous_value=previous, incoming_value=incoming,
            violation_type="extreme_drop", severity="critical",
            details=f"{field_name} dropped {drop_pct:.1f}% ({previous:.2f} → {incoming:.2f})",
        )
    return None


def check_negative(
    data_type: str, entity_id: str, field_name: str,
    value: Optional[float],
) -> Optional[CoherenceViolation]:
    """Reject negative values where not expected."""
    if value is not None and value < 0:
        return CoherenceViolation(
            data_type=data_type, entity_id=entity_id, field_name=field_name,
            previous_value=None, incoming_value=value,
            violation_type="negative_value", severity="critical",
            details=f"{field_name} is negative: {value}",
        )
    return None


def check_null_return(
    data_type: str, entity_id: str,
    data: dict,
) -> Optional[CoherenceViolation]:
    """Flag if an API returned empty/null data for a known entity."""
    if not data or all(v is None for v in data.values()):
        return CoherenceViolation(
            data_type=data_type, entity_id=entity_id, field_name="*",
            previous_value=None, incoming_value=None,
            violation_type="null_return", severity="warning",
            details=f"API returned empty/null data for {entity_id}",
        )
    return None


# =============================================================================
# DataCoherenceGuard — per-data-type validation
# =============================================================================

class DataCoherenceGuard:
    """
    Validate incoming data against previous snapshots.
    Per-data-type thresholds from THRESHOLDS config.
    """

    def __init__(self, data_type: str):
        self.data_type = data_type
        self.config = THRESHOLDS.get(data_type, {})
        self._violations: list[CoherenceViolation] = []

    def validate_liquidity(
        self, asset_id: str, venue: str, incoming: dict
    ) -> list[CoherenceViolation]:
        """Validate liquidity depth — tuned for high-volume DEX/CEX data."""
        violations = []
        cfg = self.config

        # Only flag negatives and zeros — volume/depth swings are normal
        if cfg.get("flag_negative_values", True):
            for field in ["volume_24h", "bid_depth_1pct", "ask_depth_1pct"]:
                v = check_negative(self.data_type, f"{asset_id}:{venue}", field, incoming.get(field))
                if v:
                    violations.append(v)

        if cfg.get("flag_zero_values", True):
            # Only check volume — depth can legitimately be zero for new pools
            vol = incoming.get("volume_24h")
            if vol is not None and vol == 0:
                # Check if previous had volume (zero replacement)
                try:
                    from app.database import fetch_one
                    prev = fetch_one(
                        """SELECT volume_24h FROM liquidity_depth
                           WHERE asset_id = %s AND venue = %s AND volume_24h > 1000000
                           ORDER BY snapshot_at DESC LIMIT 1""",
                        (asset_id, venue),
                    )
                    if prev and prev.get("volume_24h") and float(prev["volume_24h"]) > 1_000_000:
                        violations.append(CoherenceViolation(
                            data_type=self.data_type, entity_id=f"{asset_id}:{venue}",
                            field_name="volume_24h",
                            previous_value=float(prev["volume_24h"]),
                            incoming_value=0,
                            violation_type="zero_replacement",
                            severity="warning",
                            details=f"High-volume venue went to zero ({float(prev['volume_24h']):,.0f} → 0)",
                        ))
                except Exception as e:
                    logger.warning(f"[coherence_guards] liquidity venue check failed for {asset_id}:{venue}: {e}")
                    try:
                        from app.worker import _record_cycle_error
                        _record_cycle_error(
                            error_type="data_layer_validate_liquidity_check_failure",
                            error_message=str(e)[:500],
                            cycle_phase="coherence_guards",
                        )
                    except Exception:
                        pass

        self._violations.extend(violations)
        return violations

    def validate_yield(
        self, pool_id: str, incoming: dict
    ) -> list[CoherenceViolation]:
        """Validate yield snapshot — flag negatives and catastrophic TVL drops."""
        violations = []
        cfg = self.config

        if cfg.get("flag_negative_tvl", True):
            v = check_negative(self.data_type, pool_id, "tvl_usd", incoming.get("tvl_usd"))
            if v:
                violations.append(v)

        if cfg.get("flag_negative_apy", True):
            apy = incoming.get("apy")
            if apy is not None and apy < -100:  # Small negative APY is possible (IL)
                violations.append(CoherenceViolation(
                    data_type=self.data_type, entity_id=pool_id,
                    field_name="apy", previous_value=None, incoming_value=apy,
                    violation_type="extreme_negative_apy", severity="warning",
                    details=f"APY is extremely negative: {apy:.1f}%",
                ))

        # Check for catastrophic TVL drop (>90%)
        max_tvl_drop = cfg.get("max_tvl_drop_pct", 90)
        try:
            from app.database import fetch_one
            prev = fetch_one(
                """SELECT tvl_usd FROM yield_snapshots
                   WHERE pool_id = %s AND tvl_usd > 0
                   ORDER BY snapshot_at DESC LIMIT 1""",
                (pool_id,),
            )
            if prev and prev.get("tvl_usd"):
                v = check_numeric_drop(
                    self.data_type, pool_id, "tvl_usd",
                    float(prev["tvl_usd"]), incoming.get("tvl_usd"),
                    max_drop_pct=max_tvl_drop,
                )
                if v:
                    violations.append(v)
        except Exception as e:
            logger.warning(f"[coherence_guards] yield TVL drop check failed for {pool_id}: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="data_layer_validate_yield_check_failure",
                    error_message=str(e)[:500],
                    cycle_phase="coherence_guards",
                )
            except Exception:
                pass

        self._violations.extend(violations)
        return violations

    def validate_exchange(
        self, exchange_id: str, incoming: dict
    ) -> list[CoherenceViolation]:
        """Validate exchange — flag trust score changes and disappearances."""
        violations = []
        cfg = self.config

        if cfg.get("flag_trust_score_change", True):
            try:
                from app.database import fetch_one
                prev = fetch_one(
                    """SELECT trust_score FROM exchange_snapshots
                       WHERE exchange_id = %s AND trust_score IS NOT NULL
                       ORDER BY snapshot_at DESC LIMIT 1""",
                    (exchange_id,),
                )
                if prev and prev.get("trust_score") is not None:
                    old_ts = int(prev["trust_score"])
                    new_ts = incoming.get("trust_score")
                    if new_ts is not None and abs(int(new_ts) - old_ts) >= 2:
                        violations.append(CoherenceViolation(
                            data_type=self.data_type, entity_id=exchange_id,
                            field_name="trust_score",
                            previous_value=old_ts, incoming_value=int(new_ts),
                            violation_type="trust_score_change",
                            severity="warning",
                            details=f"Trust score changed: {old_ts} → {new_ts}",
                        ))
            except Exception as e:
                logger.warning(f"[coherence_guards] exchange trust check failed for {exchange_id}: {e}")
                try:
                    from app.worker import _record_cycle_error
                    _record_cycle_error(
                        error_type="data_layer_validate_exchange_check_failure",
                        error_message=str(e)[:500],
                        cycle_phase="coherence_guards",
                    )
                except Exception:
                    pass

        self._violations.extend(violations)
        return violations

    def validate_bridge_flow(
        self, bridge_id: str, source_chain: str, dest_chain: str, incoming: dict
    ) -> list[CoherenceViolation]:
        """Validate bridge flow — flag negatives only."""
        violations = []
        key = f"{bridge_id}:{source_chain}->{dest_chain}"

        v = check_negative(self.data_type, key, "volume_usd", incoming.get("volume_usd"))
        if v:
            violations.append(v)
        v = check_negative(self.data_type, key, "tvl_usd", incoming.get("tvl_usd"))
        if v:
            violations.append(v)

        self._violations.extend(violations)
        return violations

    def validate_entity_snapshot(
        self, entity_id: str, incoming: dict
    ) -> list[CoherenceViolation]:
        """Validate entity snapshot — flag null returns, not market swings."""
        violations = []

        v = check_null_return(self.data_type, entity_id, incoming)
        if v:
            violations.append(v)

        self._violations.extend(violations)
        return violations

    def validate_price(
        self, entity_id: str, incoming_price: float
    ) -> list[CoherenceViolation]:
        """Validate stablecoin price — flag >10% deviation from $1.00."""
        violations = []
        max_dev = self.config.get("max_price_deviation_pct", 10)

        deviation_pct = abs(incoming_price - 1.0) * 100
        if deviation_pct > max_dev:
            violations.append(CoherenceViolation(
                data_type=self.data_type, entity_id=entity_id,
                field_name="price",
                previous_value=1.0, incoming_value=incoming_price,
                violation_type="price_deviation",
                severity="critical" if deviation_pct > 50 else "warning",
                details=f"Stablecoin price deviated {deviation_pct:.1f}% from $1.00 (${incoming_price:.4f})",
            ))

        self._violations.extend(violations)
        return violations

    def get_violations(self) -> list[CoherenceViolation]:
        return list(self._violations)

    def store_all_violations(self):
        for v in self._violations:
            store_violation(v)
        count = len(self._violations)
        if count > 0:
            logger.warning(f"Coherence guard [{self.data_type}]: {count} violations stored")
        self._violations.clear()


# =============================================================================
# Ops dashboard — coherence summary
# =============================================================================

def get_coherence_summary(hours: int = 24) -> dict:
    """
    Coherence summary for ops dashboard.
    Returns: flags by data type, flag rate, top flagged entities, oldest unreviewed.
    """
    from app.database import fetch_all, fetch_one

    # Total flags in window
    by_type = fetch_all(
        """SELECT data_type, violation_type, severity, COUNT(*) as cnt
           FROM coherence_violations
           WHERE created_at >= NOW() - INTERVAL '%s hours'
           GROUP BY data_type, violation_type, severity
           ORDER BY cnt DESC""",
        (hours,),
    )

    # Total records per data type (approximate from recent hourly rollups)
    total_records = fetch_all(
        """SELECT provider, SUM(total_calls) as total
           FROM api_usage_hourly
           WHERE hour >= NOW() - INTERVAL '%s hours'
           GROUP BY provider""",
        (hours,),
    )

    # Top 10 most flagged entities
    top_entities = fetch_all(
        """SELECT entity_id, data_type, COUNT(*) as flag_count
           FROM coherence_violations
           WHERE created_at >= NOW() - INTERVAL '%s hours'
           GROUP BY entity_id, data_type
           ORDER BY flag_count DESC
           LIMIT 10""",
        (hours,),
    )

    # Oldest unreviewed flag
    oldest = fetch_one(
        """SELECT id, data_type, entity_id, violation_type, severity, details, created_at
           FROM coherence_violations
           WHERE reviewed = FALSE
           ORDER BY created_at ASC
           LIMIT 1"""
    )

    # Unreviewed count
    unreviewed = fetch_one(
        "SELECT COUNT(*) as cnt FROM coherence_violations WHERE reviewed = FALSE"
    )

    # Total flags
    total_flags = fetch_one(
        """SELECT COUNT(*) as cnt FROM coherence_violations
           WHERE created_at >= NOW() - INTERVAL '%s hours'""",
        (hours,),
    )

    return {
        "window_hours": hours,
        "total_flags": total_flags["cnt"] if total_flags else 0,
        "unreviewed_count": unreviewed["cnt"] if unreviewed else 0,
        "by_type": [dict(r) for r in by_type] if by_type else [],
        "top_flagged_entities": [dict(r) for r in top_entities] if top_entities else [],
        "oldest_unreviewed": dict(oldest) if oldest else None,
        "api_usage_context": [dict(r) for r in total_records] if total_records else [],
    }
