"""
Component 2b: Observation builder.

Replaces the empty-Signal stub from S2a with real observations computed
from production data. Pulls pre-event / event-window / post-event ranges
per covered index, produces value + trend observations per measure,
flags z-score anomalies (strict 14-point baseline minimum), flags peer
divergences when peer_set is non-empty.

Public entry point:
    def build_signal(entity, event_date, peer_set, coverage) -> Signal

Design notes:
  - Windows are computed once per analysis. With event_date present:
    pre_event = [event_date - 30d, event_date - 1d]
    event_window = [event_date, event_date + 7d]
    post_event = [event_date + 8d, today]
    Without event_date: only baseline = [today - 30d, today].
  - Per source we issue ONE query covering history_start (= earliest
    window start - 30 days) through today, then slice in Python for
    each window. The 30-day prefix gives the rolling baseline for
    z-score anomaly detection.
  - DB queries and this module's public API are synchronous, matching
    app/engine/coverage.py and the rest of the engine pipeline. Callers
    that need event-loop-friendly behavior wrap with asyncio.to_thread
    at the call site; build_stub_analysis stays sync to keep the
    analyze_router unchanged from S2a (per prompt scope).
  - All failure modes degrade gracefully: missing window data → fewer
    observations, not exceptions. Insufficient anomaly history → no
    anomaly flag, value observation still emitted. Peers without
    coverage on a measure → no peer_divergence_magnitude on that
    observation, value observation still emitted. Measure not in
    MEASURE_UNITS → unit="unknown" + warning logged.

S2c will replace the stub Interpretation in app/engine/analysis.py with
LLM-generated content; this module's output (Signal) is the primary
input to that prompt.
"""

from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from typing import Optional

from app.database import fetch_all
from app.engine.schemas import (
    CoverageResponse,
    EntityCoverage,
    EventWindow,
    Observation,
    Signal,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────
# MEASURE_UNITS — measure name → unit string
#
# Resolved at observation construction. Unknown measures fall through to
# "unknown" with a logged warning so operator can extend the dict over
# time. The same measure name on different indexes (e.g., overall_score
# on PSI vs SII) shares the unit; downstream consumers distinguish via
# index_id on the Observation.
# ─────────────────────────────────────────────────────────────────

MEASURE_UNITS: dict[str, str] = {
    # LSTI components
    "peg_volatility_7d": "pct",
    "peg_volatility_30d": "pct",
    "eth_peg_deviation": "pct",
    "eth_price_ratio": "ratio",
    "market_cap": "usd",
    "holder_gini": "ratio_0_1",
    "exchange_concentration": "ratio_0_1",
    "top_holder_concentration": "ratio_0_1",
    "volume_cap_ratio": "ratio",
    "defi_protocol_share": "ratio_0_1",
    "admin_key_risk": "score_0_100",
    "upgradeability_risk": "score_0_100",
    "withdrawal_queue_impl": "score_0_100",
    "slashing_insurance": "score_0_100",
    "exploit_history_lst": "score_0_100",
    "beacon_chain_dependency": "score_0_100",
    "mev_exposure": "score_0_100",
    "audit_status": "count",

    # PSI category + component scores
    "overall_score": "score_0_100",
    "balance_sheet": "score_0_100",
    "revenue": "score_0_100",
    "liquidity": "score_0_100",
    "security": "score_0_100",
    "token_health": "score_0_100",

    # PSI raw fundamentals
    "tvl": "usd",
    "fees_24h": "usd",
    "revenue_24h": "usd",
    "token_price": "usd",
    "token_mcap": "usd",
    "token_volume": "usd",
    "chain_count": "count",

    # BRI components
    "decentralization": "score_0_100",
    "economic_security": "score_0_100",
    "operational_history": "score_0_100",
    "smart_contract_risk": "score_0_100",
    "liquidity_throughput": "score_0_100",
    "security_architecture": "score_0_100",
    "bridge_insurance": "score_0_100",
    "restaking_security": "score_0_100",
    "bridge_formal_verification": "score_0_100",
    "bridge_timelock": "count",
    "bridge_audit_count": "count",
    "uptime_pct": "pct",
    "message_success_rate": "pct",
    "cost_to_attack": "usd",

    # SII categories (from scores / score_history columns)
    "peg_score": "score_0_100",
    "liquidity_score": "score_0_100",
    "mint_burn_score": "score_0_100",
    "distribution_score": "score_0_100",
    "structural_score": "score_0_100",
    "reserves_score": "score_0_100",
    "contract_score": "score_0_100",
    "oracle_score": "score_0_100",
    "governance_score": "score_0_100",
    "network_score": "score_0_100",

    # Measures discovered during S2b verification — added in S2c.
    # The "score" measure is ambiguous (different indexes use it for
    # different things). Disambiguate it by index in a future cleanup;
    # for v1 we keep a generic mapping so analyses don't 'unknown'-warn
    # on it constantly.
    "collateral_diversity": "score_0_100",
    "compensation_transparency": "score_0_100",
    "concentration_top3": "ratio_0_1",
    "governance": "score_0_100",
    "has_stablecoin_exposure": "boolean",
    "meeting_cadence": "count",
    "score": "score_0_100",  # FIXME: ambiguous — disambiguate by index in a future PR
    "unique_tokens": "count",
}


# Track measure names we've already warned about so logs don't spam.
_warned_unknown_measures: set[str] = set()


def _unit_for(measure: str) -> str:
    unit = MEASURE_UNITS.get(measure)
    if unit is not None:
        return unit
    if measure not in _warned_unknown_measures:
        logger.warning(
            "observation_builder: measure %r not in MEASURE_UNITS; "
            "defaulting to 'unknown'. Add it to MEASURE_UNITS in "
            "app/engine/observation_builder.py to silence this.",
            measure,
        )
        _warned_unknown_measures.add(measure)
    return "unknown"


# ─────────────────────────────────────────────────────────────────
# Window computation
# ─────────────────────────────────────────────────────────────────

def compute_windows(
    event_date: Optional[date],
    today: Optional[date] = None,
) -> dict[EventWindow, tuple[date, date]]:
    """Return the date ranges (inclusive on both ends) for each window.

    With event_date:
      pre_event    = [event_date - 30d, event_date - 1d]
      event_window = [event_date, event_date + 7d]
      post_event   = [event_date + 8d, today]   (may be empty if event is recent)

    Without event_date:
      baseline     = [today - 30d, today]

    Empty ranges (start > end) are returned as-is; callers skip windows
    with no data.
    """
    today = today or date.today()
    if event_date is None:
        return {"baseline": (today - timedelta(days=30), today)}
    return {
        "pre_event": (event_date - timedelta(days=30), event_date - timedelta(days=1)),
        "event_window": (event_date, event_date + timedelta(days=7)),
        "post_event": (event_date + timedelta(days=8), today),
    }


# ─────────────────────────────────────────────────────────────────
# Per-source data fetch
#
# Each fetch function returns dict[measure_name, list[(date, float)]],
# sorted by date ascending. Measures with no usable points are absent.
# ─────────────────────────────────────────────────────────────────

def _coerce_numeric(value: object) -> Optional[float]:
    """Best-effort cast to float; returns None if not coercible or NaN."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _fetch_generic_index_scores_sync(
    index_id: str,
    entity_slug: str,
    start: date,
    end: date,
) -> dict[str, list[tuple[date, float]]]:
    rows = fetch_all(
        """
        SELECT scored_date, overall_score, category_scores, component_scores, raw_values
        FROM generic_index_scores
        WHERE index_id = %s AND entity_slug = %s
          AND scored_date >= %s AND scored_date <= %s
        ORDER BY scored_date
        """,
        (index_id, entity_slug, start, end),
    )
    series: dict[str, list[tuple[date, float]]] = {}
    for r in rows:
        d = r["scored_date"]
        # explicit overall_score column
        ov = _coerce_numeric(r.get("overall_score"))
        if ov is not None:
            series.setdefault("overall_score", []).append((d, ov))
        # category, component, and raw_values JSONB — psycopg2 decodes these to dict
        for col in ("category_scores", "component_scores", "raw_values"):
            payload = r.get(col)
            if not isinstance(payload, dict):
                continue
            for measure, raw in payload.items():
                v = _coerce_numeric(raw)
                if v is None:
                    continue
                series.setdefault(measure, []).append((d, v))
    return series


def _fetch_historical_protocol_data_sync(
    protocol_slug: str,
    start: date,
    end: date,
) -> dict[str, list[tuple[date, float]]]:
    rows = fetch_all(
        """
        SELECT record_date, tvl, fees_24h, revenue_24h, token_price,
               token_mcap, token_volume, chain_count
        FROM historical_protocol_data
        WHERE protocol_slug = %s
          AND record_date >= %s AND record_date <= %s
        ORDER BY record_date
        """,
        (protocol_slug, start, end),
    )
    series: dict[str, list[tuple[date, float]]] = {}
    measure_cols = ("tvl", "fees_24h", "revenue_24h", "token_price",
                    "token_mcap", "token_volume", "chain_count")
    for r in rows:
        d = r["record_date"]
        for col in measure_cols:
            v = _coerce_numeric(r.get(col))
            if v is None:
                continue
            series.setdefault(col, []).append((d, v))
    return series


def _fetch_sii_history_sync(
    stablecoin: str,
    start: date,
    end: date,
) -> dict[str, list[tuple[date, float]]]:
    rows = fetch_all(
        """
        SELECT score_date, overall_score, peg_score, liquidity_score,
               mint_burn_score, distribution_score, structural_score,
               reserves_score, contract_score, oracle_score,
               governance_score, network_score
        FROM score_history
        WHERE stablecoin = %s
          AND score_date >= %s AND score_date <= %s
        ORDER BY score_date
        """,
        (stablecoin, start, end),
    )
    series: dict[str, list[tuple[date, float]]] = {}
    measure_cols = (
        "overall_score", "peg_score", "liquidity_score", "mint_burn_score",
        "distribution_score", "structural_score", "reserves_score",
        "contract_score", "oracle_score", "governance_score", "network_score",
    )
    for r in rows:
        d = r["score_date"]
        for col in measure_cols:
            v = _coerce_numeric(r.get(col))
            if v is None:
                continue
            series.setdefault(col, []).append((d, v))
    return series


def _fetch(
    entity_coverage: EntityCoverage,
    entity_slug: str,
    start: date,
    end: date,
) -> dict[str, list[tuple[date, float]]]:
    """Dispatch to the right source based on EntityCoverage.data_source."""
    src = entity_coverage.data_source
    if src == "generic_index_scores":
        return _fetch_generic_index_scores_sync(
            entity_coverage.index_id, entity_slug, start, end
        )
    if src == "historical_protocol_data":
        return _fetch_historical_protocol_data_sync(entity_slug, start, end)
    if src == "scores+score_history":
        return _fetch_sii_history_sync(entity_slug, start, end)
    logger.warning(
        "observation_builder: unknown data_source %r for index_id=%s; "
        "no observations will be produced for this index",
        src, entity_coverage.index_id,
    )
    return {}


# ─────────────────────────────────────────────────────────────────
# Statistics — z-score, linear regression
# ─────────────────────────────────────────────────────────────────

ANOMALY_HISTORY_MIN = 14   # strict per spec — fewer = no anomaly flag
ANOMALY_Z_THRESHOLD = 2.0
TREND_MIN_POINTS = 3


def _z_score(value: float, history: list[float]) -> Optional[float]:
    """Z-score of `value` against `history`. Returns None if history is too
    short (< ANOMALY_HISTORY_MIN points) or has zero variance."""
    if len(history) < ANOMALY_HISTORY_MIN:
        return None
    mean = sum(history) / len(history)
    var = sum((v - mean) ** 2 for v in history) / len(history)
    if var <= 0:
        return None
    std = math.sqrt(var)
    return (value - mean) / std


def _linear_regression_slope(
    points: list[tuple[date, float]]
) -> Optional[float]:
    """Slope of best-fit line through `points` (date, value), expressed as
    per-day change in value. Returns None if fewer than TREND_MIN_POINTS
    or if x-axis variance is zero."""
    if len(points) < TREND_MIN_POINTS:
        return None
    x0 = points[0][0]
    xs = [(d - x0).days for d, _ in points]
    ys = [v for _, v in points]
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    den = sum((x - x_mean) ** 2 for x in xs)
    if den == 0:
        return None
    return num / den


# ─────────────────────────────────────────────────────────────────
# Peer aggregation
# ─────────────────────────────────────────────────────────────────

def _peer_average(
    peer_data: dict[str, dict[str, list[tuple[date, float]]]],
    measure: str,
    window_start: date,
    window_end: date,
) -> tuple[Optional[float], list[str]]:
    """Average the latest in-window value across peers that have coverage
    on `measure`. Returns (peer_avg, [peer slugs that contributed])."""
    values: list[float] = []
    contributors: list[str] = []
    for peer_slug, series_by_measure in peer_data.items():
        series = series_by_measure.get(measure)
        if not series:
            continue
        in_window = [(d, v) for d, v in series if window_start <= d <= window_end]
        if not in_window:
            continue
        values.append(in_window[-1][1])
        contributors.append(peer_slug)
    if not values:
        return None, []
    return sum(values) / len(values), contributors


# ─────────────────────────────────────────────────────────────────
# Per-(index, window) observation construction
# ─────────────────────────────────────────────────────────────────

def _build_window_observations(
    *,
    index_id: str,
    entity_slug: str,
    window: EventWindow,
    window_start: date,
    window_end: date,
    series_by_measure: dict[str, list[tuple[date, float]]],
    peer_data: dict[str, dict[str, list[tuple[date, float]]]],
) -> list[Observation]:
    """For one (index, window), produce value + optional trend observations
    per measure with anomaly + peer-divergence flags applied to the value.

    series_by_measure contains the FULL series including the 30-day prefix
    used as anomaly baseline. We slice it to the window for the value/trend
    observations and use the prefix for the z-score baseline.
    """
    out: list[Observation] = []
    for measure, full_series in series_by_measure.items():
        window_points = [(d, v) for d, v in full_series if window_start <= d <= window_end]
        if not window_points:
            continue

        latest_date, latest_value = window_points[-1]
        unit = _unit_for(measure)

        # Anomaly: z-score against the 30 days strictly prior to latest_date
        prior_start = latest_date - timedelta(days=30)
        prior = [v for d, v in full_series if prior_start <= d < latest_date]
        z = _z_score(latest_value, prior)
        is_anomaly = z is not None and abs(z) > ANOMALY_Z_THRESHOLD

        # Peer divergence
        peer_avg, peer_slugs = _peer_average(
            peer_data, measure, window_start, window_end
        )
        peer_div = (latest_value - peer_avg) if peer_avg is not None else None

        out.append(
            Observation(
                index_id=index_id,
                entity_slug=entity_slug,
                measure=measure,
                window=window,
                kind="value",
                metric_value=latest_value,
                reference_value=peer_avg,
                unit=unit,
                at_date=latest_date,
                window_start=window_points[0][0],
                window_end=latest_date,
                is_anomaly=is_anomaly,
                anomaly_z_score=z,
                peer_divergence_magnitude=peer_div,
                peer_slugs_compared=peer_slugs,
            )
        )

        # Trend (slope per day) when we have enough points in the window
        slope = _linear_regression_slope(window_points)
        if slope is not None:
            out.append(
                Observation(
                    index_id=index_id,
                    entity_slug=entity_slug,
                    measure=measure,
                    window=window,
                    kind="trend",
                    metric_value=slope,
                    reference_value=window_points[0][1],
                    unit=unit,
                    at_date=window_points[-1][0],
                    window_start=window_points[0][0],
                    window_end=window_points[-1][0],
                    is_anomaly=False,
                    anomaly_z_score=None,
                    peer_divergence_magnitude=None,
                    peer_slugs_compared=[],
                )
            )
    return out


# ─────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────

def build_signal(
    entity: str,
    event_date: Optional[date],
    peer_set: list[str],
    coverage: CoverageResponse,
    today: Optional[date] = None,
) -> Signal:
    """Pull data, compute observations, return a populated Signal.

    Per Step 0 §1, when event_date is None only `baseline` is populated;
    when event_date is set, baseline stays empty and the three event
    windows are populated. The Signal model_validator on AnalysisCreate
    enforces this invariant — we honor it here by only ever filling one
    side.
    """
    today = today or date.today()
    windows = compute_windows(event_date, today=today)

    # Single fetch range per index covers all windows + the 30-day prefix
    # used as anomaly baseline. start and end are clamped to a sensible
    # joint range across the window dict.
    earliest_window_start = min(start for start, _ in windows.values())
    latest_window_end = max(end for _, end in windows.values())
    fetch_start = earliest_window_start - timedelta(days=30)
    fetch_end = max(latest_window_end, today)

    baseline_obs: list[Observation] = []
    pre_obs: list[Observation] = []
    event_obs: list[Observation] = []
    post_obs: list[Observation] = []

    for entity_coverage in coverage.matched_entities:
        # Primary entity series (full range including 30d prefix)
        entity_series = _fetch(
            entity_coverage, coverage.identifier, fetch_start, fetch_end
        )
        if not entity_series:
            continue

        # Peer series — same source/index, each peer fetched independently.
        # Empty when peer_set is empty (peer-div flag stays cleared).
        peer_data: dict[str, dict[str, list[tuple[date, float]]]] = {}
        for peer_slug in peer_set:
            peer_series = _fetch(
                entity_coverage, peer_slug, fetch_start, fetch_end
            )
            if peer_series:
                peer_data[peer_slug] = peer_series

        for window_name, (w_start, w_end) in windows.items():
            if w_start > w_end:
                # Empty range (e.g., post_event when event is too recent)
                continue
            window_obs = _build_window_observations(
                index_id=entity_coverage.index_id,
                entity_slug=coverage.identifier,
                window=window_name,
                window_start=w_start,
                window_end=w_end,
                series_by_measure=entity_series,
                peer_data=peer_data,
            )
            if window_name == "baseline":
                baseline_obs.extend(window_obs)
            elif window_name == "pre_event":
                pre_obs.extend(window_obs)
            elif window_name == "event_window":
                event_obs.extend(window_obs)
            elif window_name == "post_event":
                post_obs.extend(window_obs)

    return Signal(
        baseline=baseline_obs,
        pre_event=pre_obs,
        event_window=event_obs,
        post_event=post_obs,
    )
