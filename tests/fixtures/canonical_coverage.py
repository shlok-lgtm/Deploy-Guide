"""
Canonical CoverageResponse fixtures — the contract for Component 1.

These fixtures are extracted from production on FIXTURE_CAPTURE_DATE via
docs/analytic_engine_coverage_extraction.sql and pinned here as the shared
test contract for Stage 2 parallel sessions (P1/P2/P3).

Contract rules:
  - P1's real endpoint output MUST match these fixtures (after datetime
    normalization) for the six canonical entities.
  - P2 and P3 import these fixtures and never call the live coverage endpoint
    during their mocked testing.
  - Refresh cadence is monthly, first business day, operator-initiated.
    See docs/analytic_engine_step_0_v0.2.md §4 for the refresh policy.
  - Dates are expressed as offsets from FIXTURE_CAPTURE_DATE to make the
    fixtures stable if we rebase the capture date. Absolute dates are used
    only for earliest_record values that predate FIXTURE_CAPTURE_DATE by
    more than a year (i.e., historical_protocol_data origins).

Known caveats from the 2026-04-24 extraction:
  - USDC is 2 days stale in SII (days_since_last_record=2). Other stablecoins
    computed on FIXTURE_CAPTURE_DATE. See Step 0 doc §11.1 for the standing
    follow-up. The fixture reflects the observed state; it is not a test
    bug.
  - adjacent_indexes_not_covering values were populated manually from
    Query 1 + Query 2 + Query 3 outputs. Query 5 of the extraction SQL is
    retired (see SQL file and Step 0 doc §11.2).
"""

from datetime import date, datetime, timedelta
from typing import Optional

from app.engine.schemas import CoverageResponse, EntityCoverage, RelatedEntity

# ─────────────────────────────────────────────────────────────────
# Capture anchors — the one place to change when refreshing fixtures
# ─────────────────────────────────────────────────────────────────

FIXTURE_CAPTURE_DATE = date(2026, 4, 24)
FIXTURE_CAPTURE_DATETIME = datetime(2026, 4, 24, 12, 0, 0)


def _off(days: int) -> date:
    """Date expressed as offset from FIXTURE_CAPTURE_DATE (days<=0 for past)."""
    return FIXTURE_CAPTURE_DATE + timedelta(days=days)


# Universe of adjacent indexes present in the Basis registry as of capture.
# Populated manually from Q1+Q2+Q3 outputs because extraction Query 5 is
# retired. When the registry grows, rebuild this list on next fixture refresh.
_FULL_INDEX_UNIVERSE = {
    "bri",
    "bridge_monitor",
    "cxri",
    "dex_pool_data",
    "dohi",
    "exchange_health",
    "lsti",
    "psi",
    "sii",
    "tti",
    "vsri",
    "web_research_bridge",
    "web_research_exchange",
    "web_research_protocol",
}


def _complement(covering: set[str]) -> list[str]:
    """Return sorted adjacent-indexes-not-covering given the covering set."""
    return sorted(_FULL_INDEX_UNIVERSE - covering)


# ─────────────────────────────────────────────────────────────────
# Fixture 1 — Drift (partial-live; 3 matched entities)
# ─────────────────────────────────────────────────────────────────

DRIFT_COVERAGE = CoverageResponse(
    identifier="drift",
    matched_entities=[
        EntityCoverage(
            index_id="dex_pool_data",
            entity_slug="drift",
            entity_name="drift",
            coverage_type="live",
            live=True,
            density="multiple_daily",
            earliest_record=_off(-12),
            latest_record=_off(0),
            unique_days=5,
            days_since_last_record=0,
            coverage_window_days=12,
            data_source="generic_index_scores",
            available_endpoints=[],
        ),
        EntityCoverage(
            index_id="web_research_protocol",
            entity_slug="drift",
            entity_name="drift",
            coverage_type="sparse",
            live=False,
            density="single",
            earliest_record=_off(-11),
            latest_record=_off(-11),
            unique_days=1,
            days_since_last_record=11,
            coverage_window_days=0,
            data_source="generic_index_scores",
            available_endpoints=[],
        ),
        EntityCoverage(
            index_id="psi",
            entity_slug="drift",
            entity_name="drift",
            coverage_type="backfilled",
            live=False,
            density="daily",
            earliest_record=date(2021, 12, 4),  # absolute: predates FIXTURE_CAPTURE_DATE by >4y
            latest_record=_off(-21),
            unique_days=1582,
            days_since_last_record=21,
            coverage_window_days=1581,
            data_source="historical_protocol_data",
            available_endpoints=[
                "/api/psi/scores/drift/at/{date}",
                "/api/psi/scores/drift/range",
            ],
        ),
    ],
    related_entities=[],
    adjacent_indexes_not_covering=_complement({"dex_pool_data", "web_research_protocol", "psi"}),
    coverage_summary=(
        "Drift has live coverage in dex_pool_data (5 days, multiple_daily) and "
        "web_research_protocol (single data point, 11 days stale). PSI is "
        "backfilled via temporal reconstruction with 1582 days back to "
        "2021-12-04, last ingested 21 days ago. No live PSI tracking. No "
        "coverage in BRI, LSTI, SII, or other indexes."
    ),
    coverage_quality="partial-live",
    recommended_analysis_types=["retrospective_internal", "case_study", "internal_memo"],
    blocks_incident_page=True,
    blocks_reasons=[
        "PSI coverage is backfilled, not live; pre-event PSI claims require "
        "temporal reconstruction which is not a pinned-evidence artifact per "
        "V9.6 constitutional amendment. Live indexes (dex_pool_data, "
        "web_research_protocol) are sparse and not sufficient to support an "
        "incident page."
    ],
    data_snapshot_hash="sha256:drift_2026_04_24",
    computed_at=FIXTURE_CAPTURE_DATETIME,
)


# ─────────────────────────────────────────────────────────────────
# Fixture 2 — Kelp rsETH (partial-live; 1 matched entity)
# ─────────────────────────────────────────────────────────────────

KELP_RSETH_COVERAGE = CoverageResponse(
    identifier="kelp-rseth",
    matched_entities=[
        EntityCoverage(
            index_id="lsti",
            entity_slug="kelp-rseth",
            entity_name="Kelp rsETH",
            coverage_type="live",
            live=True,
            density="daily",
            earliest_record=_off(-367),
            latest_record=_off(0),
            unique_days=368,
            days_since_last_record=0,
            coverage_window_days=367,
            data_source="generic_index_scores",
            available_endpoints=["/api/lsti/scores/kelp-rseth"],
        ),
    ],
    related_entities=[],
    adjacent_indexes_not_covering=_complement({"lsti"}),
    coverage_summary=(
        "kelp-rseth has live LSTI coverage with 368 days of daily data back "
        "to 2025-04-22. No coverage in BRI, PSI, SII, or any other index."
    ),
    coverage_quality="partial-live",
    recommended_analysis_types=[
        "incident_page",
        "retrospective_internal",
        "case_study",
        "internal_memo",
        "talking_points",
        "one_pager",
    ],
    blocks_incident_page=False,
    blocks_reasons=[],
    data_snapshot_hash="sha256:kelp_rseth_2026_04_24",
    computed_at=FIXTURE_CAPTURE_DATETIME,
)


# ─────────────────────────────────────────────────────────────────
# Fixture 3 — USDC (partial-live; 1 matched entity)
#
# Note: live=False at capture time because days_since_last_record=2. Other
# SII stablecoins computed on FIXTURE_CAPTURE_DATE. Collector skipped USDC
# in the most recent cycle. See Step 0 doc §11.1.
# ─────────────────────────────────────────────────────────────────

USDC_COVERAGE = CoverageResponse(
    identifier="usdc",
    matched_entities=[
        EntityCoverage(
            index_id="sii",
            entity_slug="usdc",
            entity_name=None,
            coverage_type="live",
            live=False,
            density="daily",
            earliest_record=_off(-73),
            latest_record=_off(-2),
            unique_days=72,
            days_since_last_record=2,
            coverage_window_days=71,
            data_source="scores+score_history",
            available_endpoints=["/api/sii/scores/usdc"],
        ),
    ],
    related_entities=[],
    adjacent_indexes_not_covering=_complement({"sii"}),
    coverage_summary=(
        "usdc has SII coverage with 72 days of daily data since 2026-02-10. "
        "Most recent computation is 2 days stale (2026-04-22) — other "
        "stablecoins computed on 2026-04-24; collector appears to have "
        "skipped usdc in most recent run. No coverage in other indexes."
    ),
    coverage_quality="partial-live",
    recommended_analysis_types=[
        "incident_page",
        "retrospective_internal",
        "case_study",
        "internal_memo",
        "talking_points",
        "one_pager",
    ],
    blocks_incident_page=False,
    blocks_reasons=[],
    data_snapshot_hash="sha256:usdc_2026_04_24",
    computed_at=FIXTURE_CAPTURE_DATETIME,
)


# ─────────────────────────────────────────────────────────────────
# Fixture 4 — Jupiter Perpetual Exchange (partial-live; 3 matched entities)
# Peer of Drift via PSI; shape mirrors DRIFT_COVERAGE with different dates.
# ─────────────────────────────────────────────────────────────────

JUPITER_PERP_COVERAGE = CoverageResponse(
    identifier="jupiter-perpetual-exchange",
    matched_entities=[
        EntityCoverage(
            index_id="dex_pool_data",
            entity_slug="jupiter-perpetual-exchange",
            entity_name="jupiter-perpetual-exchange",
            coverage_type="live",
            live=True,
            density="multiple_daily",
            earliest_record=_off(-12),
            latest_record=_off(0),
            unique_days=5,
            days_since_last_record=0,
            coverage_window_days=12,
            data_source="generic_index_scores",
            available_endpoints=[],
        ),
        EntityCoverage(
            index_id="web_research_protocol",
            entity_slug="jupiter-perpetual-exchange",
            entity_name="jupiter-perpetual-exchange",
            coverage_type="sparse",
            live=False,
            density="single",
            earliest_record=_off(-11),
            latest_record=_off(-11),
            unique_days=1,
            days_since_last_record=11,
            coverage_window_days=0,
            data_source="generic_index_scores",
            available_endpoints=[],
        ),
        EntityCoverage(
            index_id="psi",
            entity_slug="jupiter-perpetual-exchange",
            entity_name="jupiter-perpetual-exchange",
            coverage_type="backfilled",
            live=False,
            density="daily",
            earliest_record=date(2024, 1, 29),  # absolute
            latest_record=_off(-21),
            unique_days=796,
            days_since_last_record=21,
            coverage_window_days=795,
            data_source="historical_protocol_data",
            available_endpoints=[
                "/api/psi/scores/jupiter-perpetual-exchange/at/{date}",
                "/api/psi/scores/jupiter-perpetual-exchange/range",
            ],
        ),
    ],
    related_entities=[],
    adjacent_indexes_not_covering=_complement({"dex_pool_data", "web_research_protocol", "psi"}),
    coverage_summary=(
        "jupiter-perpetual-exchange has live coverage in dex_pool_data "
        "(5 days, multiple_daily) and web_research_protocol (single data "
        "point, 11 days stale). PSI is backfilled via temporal "
        "reconstruction with 796 days back to 2024-01-29, last ingested "
        "21 days ago. No live PSI tracking."
    ),
    coverage_quality="partial-live",
    recommended_analysis_types=["retrospective_internal", "case_study", "internal_memo"],
    blocks_incident_page=True,
    blocks_reasons=[
        "PSI coverage is backfilled, not live; pre-event PSI claims require "
        "temporal reconstruction which is not a pinned-evidence artifact per "
        "V9.6 constitutional amendment. Live indexes are sparse and not "
        "sufficient to support an incident page."
    ],
    data_snapshot_hash="sha256:jupiter_perpetual_exchange_2026_04_24",
    computed_at=FIXTURE_CAPTURE_DATETIME,
)


# ─────────────────────────────────────────────────────────────────
# Fixture 5 — LayerZero (partial-live; 2 matched entities)
# ─────────────────────────────────────────────────────────────────

LAYERZERO_COVERAGE = CoverageResponse(
    identifier="layerzero",
    matched_entities=[
        EntityCoverage(
            index_id="bri",
            entity_slug="layerzero",
            entity_name="LayerZero",
            coverage_type="live",
            live=True,
            density="daily",
            earliest_record=_off(-367),
            latest_record=_off(0),
            unique_days=368,
            days_since_last_record=0,
            coverage_window_days=367,
            data_source="generic_index_scores",
            available_endpoints=["/api/bri/scores/layerzero"],
        ),
        EntityCoverage(
            index_id="web_research_bridge",
            entity_slug="layerzero",
            entity_name="layerzero",
            coverage_type="sparse",
            live=False,
            density="single",
            earliest_record=_off(-12),
            latest_record=_off(-12),
            unique_days=1,
            days_since_last_record=12,
            coverage_window_days=0,
            data_source="generic_index_scores",
            available_endpoints=[],
        ),
    ],
    related_entities=[],
    adjacent_indexes_not_covering=_complement({"bri", "web_research_bridge"}),
    coverage_summary=(
        "layerzero has live BRI coverage with 368 days of daily data back to "
        "2025-04-22. web_research_bridge has a single data point from "
        "2026-04-12 (12 days stale). No other index coverage."
    ),
    coverage_quality="partial-live",
    recommended_analysis_types=[
        "incident_page",
        "retrospective_internal",
        "case_study",
        "internal_memo",
        "talking_points",
        "one_pager",
    ],
    blocks_incident_page=False,
    blocks_reasons=[],
    data_snapshot_hash="sha256:layerzero_2026_04_24",
    computed_at=FIXTURE_CAPTURE_DATETIME,
)


# ─────────────────────────────────────────────────────────────────
# Fixture 6 — Unknown entity (404)
#
# When Component 1 cannot match the identifier, the endpoint returns HTTP
# 404 rather than a CoverageResponse. The fixture encodes the "no match"
# shape by being None. Tests assert:
#   response = client.get("/api/engine/coverage/this-entity-does-not-exist-xyz")
#   assert response.status_code == 404
#   assert UNKNOWN_ENTITY_COVERAGE is None
# ─────────────────────────────────────────────────────────────────

UNKNOWN_ENTITY_COVERAGE: Optional[CoverageResponse] = None


# ─────────────────────────────────────────────────────────────────
# Registry — makes iteration in tests ergonomic
# ─────────────────────────────────────────────────────────────────

ALL_FIXTURES: dict[str, Optional[CoverageResponse]] = {
    "drift": DRIFT_COVERAGE,
    "kelp-rseth": KELP_RSETH_COVERAGE,
    "usdc": USDC_COVERAGE,
    "jupiter-perpetual-exchange": JUPITER_PERP_COVERAGE,
    "layerzero": LAYERZERO_COVERAGE,
    "this-entity-does-not-exist-xyz": UNKNOWN_ENTITY_COVERAGE,
}
