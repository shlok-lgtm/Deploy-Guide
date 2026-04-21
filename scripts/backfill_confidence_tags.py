"""
Backfill V7.3 confidence fields on every index score table.

One-shot: recomputes `confidence`, `confidence_tag`, `component_coverage`,
`components_populated`, `components_total`, `missing_categories` from already-
stored components, and writes them in place. Does NOT re-run scoring; no API
calls, no formula evaluation.

Scope:
    scores                (SII)
    psi_scores            (PSI)
    rpi_scores            (RPI — base components only)
    generic_index_scores  (LSTI, BRI, DOHI, VSRI, CXRI, TTI — keyed by index_id)

Idempotent. Safe to re-run. Also applies migration 084 if not already applied.
"""
from __future__ import annotations

import json
import logging
import os
import sys

# Allow running as a script from the repo root.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.database import execute, fetch_all, fetch_one, get_cursor  # noqa: E402
from app.scoring_engine import compute_confidence_tag  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("backfill_confidence_tags")


# --------------------------------------------------------------------------- #
# Step 0 — ensure migration 084 columns exist (idempotent)
# --------------------------------------------------------------------------- #

MIGRATION_084_SQL = """
ALTER TABLE scores
    ADD COLUMN IF NOT EXISTS confidence TEXT,
    ADD COLUMN IF NOT EXISTS confidence_tag TEXT,
    ADD COLUMN IF NOT EXISTS component_coverage NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS components_populated INTEGER,
    ADD COLUMN IF NOT EXISTS components_total INTEGER,
    ADD COLUMN IF NOT EXISTS missing_categories JSONB;

ALTER TABLE psi_scores
    ADD COLUMN IF NOT EXISTS confidence TEXT,
    ADD COLUMN IF NOT EXISTS confidence_tag TEXT,
    ADD COLUMN IF NOT EXISTS component_coverage NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS components_populated INTEGER,
    ADD COLUMN IF NOT EXISTS components_total INTEGER,
    ADD COLUMN IF NOT EXISTS missing_categories JSONB;

ALTER TABLE rpi_scores
    ADD COLUMN IF NOT EXISTS confidence TEXT,
    ADD COLUMN IF NOT EXISTS confidence_tag TEXT,
    ADD COLUMN IF NOT EXISTS component_coverage NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS components_populated INTEGER,
    ADD COLUMN IF NOT EXISTS components_total INTEGER,
    ADD COLUMN IF NOT EXISTS missing_categories JSONB;

ALTER TABLE generic_index_scores
    ADD COLUMN IF NOT EXISTS component_coverage NUMERIC(5,4),
    ADD COLUMN IF NOT EXISTS components_populated INTEGER,
    ADD COLUMN IF NOT EXISTS components_total INTEGER,
    ADD COLUMN IF NOT EXISTS missing_categories JSONB;
"""


def ensure_columns() -> None:
    logger.info("Ensuring migration 084 columns exist...")
    with get_cursor() as cur:
        for stmt in MIGRATION_084_SQL.split(";"):
            if stmt.strip():
                cur.execute(stmt)
    # Record migration name if the migrations table is present.
    try:
        execute(
            "INSERT INTO migrations (name) VALUES ('085_confidence_tag_universal') "
            "ON CONFLICT DO NOTHING"
        )
    except Exception as e:
        logger.debug("migrations table insert skipped: %s", e)


# --------------------------------------------------------------------------- #
# SII backfill
# --------------------------------------------------------------------------- #

SII_V1_CATS = [
    ("peg", "peg_score"),
    ("liquidity", "liquidity_score"),
    ("flows", "mint_burn_score"),
    ("distribution", "distribution_score"),
    ("structural", "structural_score"),
]


def backfill_sii() -> int:
    """For every row in `scores`, recompute confidence fields from component_readings."""
    from app.scoring import COMPONENT_NORMALIZATIONS

    sii_comp_ids = set(COMPONENT_NORMALIZATIONS.keys())
    components_total = len(sii_comp_ids)

    rows = fetch_all("SELECT stablecoin_id, peg_score, liquidity_score, mint_burn_score, "
                     "distribution_score, structural_score FROM scores")
    updated = 0
    for row in rows:
        coin = row["stablecoin_id"]
        # Count populated components for this stablecoin from component_readings —
        # only components that are part of the v1 definition AND have a non-null
        # normalized_score count toward the V7.3 ratio.
        comp_rows = fetch_all(
            """
            SELECT DISTINCT ON (component_id) component_id, normalized_score
            FROM component_readings
            WHERE stablecoin_id = %s
              AND collected_at > NOW() - INTERVAL '7 days'
            ORDER BY component_id, collected_at DESC
            """,
            (coin,),
        )
        populated = sum(
            1 for r in comp_rows
            if r["component_id"] in sii_comp_ids and r.get("normalized_score") is not None
        )
        coverage = round(populated / max(components_total, 1), 4)
        missing = [cat for cat, col in SII_V1_CATS if not row.get(col)]
        conf = compute_confidence_tag(5 - len(missing), 5, coverage, missing)

        execute(
            """
            UPDATE scores SET
                confidence = %s,
                confidence_tag = %s,
                component_coverage = %s,
                components_populated = %s,
                components_total = %s,
                missing_categories = %s::jsonb
            WHERE stablecoin_id = %s
            """,
            (
                conf["confidence"],
                conf["tag"],
                coverage,
                populated,
                components_total,
                json.dumps(missing),
                coin,
            ),
        )
        updated += 1
    logger.info("SII: backfilled %d row(s)", updated)
    return updated


# --------------------------------------------------------------------------- #
# PSI / RPI / Circle 7 — all have component_scores JSON, same pattern
# --------------------------------------------------------------------------- #

def _coverage_from_component_scores(comp_scores: dict, definition: dict) -> tuple:
    """Return (populated, total, coverage, missing_categories) from stored JSON."""
    if not isinstance(comp_scores, dict):
        comp_scores = {}
    total = len(definition["components"])
    populated = sum(
        1 for cid in comp_scores
        if cid in definition["components"] and comp_scores.get(cid) is not None
    )
    coverage = round(populated / max(total, 1), 4)
    # Missing categories: categories that have zero populated components.
    comp_to_cat = {cid: cdef["category"] for cid, cdef in definition["components"].items()}
    populated_cats = {comp_to_cat[cid] for cid in comp_scores if cid in comp_to_cat and comp_scores.get(cid) is not None}
    missing = sorted(set(definition["categories"].keys()) - populated_cats)
    return populated, total, coverage, missing


def backfill_psi() -> int:
    from app.index_definitions.psi_v01 import PSI_V01_DEFINITION

    rows = fetch_all(
        "SELECT id, component_scores FROM psi_scores "
        "WHERE components_populated IS NULL OR component_coverage IS NULL"
    )
    updated = 0
    for row in rows:
        cs = row.get("component_scores") or {}
        populated, total, coverage, missing = _coverage_from_component_scores(cs, PSI_V01_DEFINITION)
        conf = compute_confidence_tag(
            len(PSI_V01_DEFINITION["categories"]) - len(missing),
            len(PSI_V01_DEFINITION["categories"]),
            coverage, missing,
        )
        execute(
            """
            UPDATE psi_scores SET
                confidence = %s,
                confidence_tag = %s,
                component_coverage = %s,
                components_populated = %s,
                components_total = %s,
                missing_categories = %s::jsonb
            WHERE id = %s
            """,
            (
                conf["confidence"],
                conf["tag"],
                coverage,
                populated,
                total,
                json.dumps(missing),
                row["id"],
            ),
        )
        updated += 1
    logger.info("PSI: backfilled %d row(s)", updated)
    return updated


def backfill_rpi() -> int:
    from app.index_definitions.rpi_v2 import RPI_V2_DEFINITION

    rows = fetch_all(
        "SELECT id, component_scores FROM rpi_scores "
        "WHERE components_populated IS NULL OR component_coverage IS NULL"
    )
    updated = 0
    for row in rows:
        cs = row.get("component_scores") or {}
        populated, total, coverage, missing = _coverage_from_component_scores(cs, RPI_V2_DEFINITION)
        conf = compute_confidence_tag(
            len(RPI_V2_DEFINITION["categories"]) - len(missing),
            len(RPI_V2_DEFINITION["categories"]),
            coverage, missing,
        )
        execute(
            """
            UPDATE rpi_scores SET
                confidence = %s,
                confidence_tag = %s,
                component_coverage = %s,
                components_populated = %s,
                components_total = %s,
                missing_categories = %s::jsonb
            WHERE id = %s
            """,
            (
                conf["confidence"],
                conf["tag"],
                coverage,
                populated,
                total,
                json.dumps(missing),
                row["id"],
            ),
        )
        updated += 1
    logger.info("RPI: backfilled %d row(s)", updated)
    return updated


def backfill_circle7() -> int:
    from app.index_definitions.lsti_v01 import LSTI_V01_DEFINITION
    from app.index_definitions.bri_v01 import BRI_V01_DEFINITION
    from app.index_definitions.dohi_v01 import DOHI_V01_DEFINITION
    from app.index_definitions.vsri_v01 import VSRI_V01_DEFINITION
    from app.index_definitions.cxri_v01 import CXRI_V01_DEFINITION
    from app.index_definitions.tti_v01 import TTI_V01_DEFINITION

    definitions = {
        "lsti": LSTI_V01_DEFINITION,
        "bri": BRI_V01_DEFINITION,
        "dohi": DOHI_V01_DEFINITION,
        "vsri": VSRI_V01_DEFINITION,
        "cxri": CXRI_V01_DEFINITION,
        "tti": TTI_V01_DEFINITION,
    }

    total_updated = 0
    for index_id, defn in definitions.items():
        rows = fetch_all(
            """
            SELECT id, component_scores FROM generic_index_scores
            WHERE index_id = %s
              AND (components_populated IS NULL OR component_coverage IS NULL)
            """,
            (index_id,),
        )
        updated = 0
        for row in rows:
            cs = row.get("component_scores") or {}
            populated, total, coverage, missing = _coverage_from_component_scores(cs, defn)
            conf = compute_confidence_tag(
                len(defn["categories"]) - len(missing),
                len(defn["categories"]),
                coverage, missing,
            )
            execute(
                """
                UPDATE generic_index_scores SET
                    confidence = %s,
                    confidence_tag = %s,
                    component_coverage = %s,
                    components_populated = %s,
                    components_total = %s,
                    missing_categories = %s::jsonb
                WHERE id = %s
                """,
                (
                    conf["confidence"],
                    conf["tag"],
                    coverage,
                    populated,
                    total,
                    json.dumps(missing),
                    row["id"],
                ),
            )
            updated += 1
        logger.info("%s: backfilled %d row(s)", index_id.upper(), updated)
        total_updated += updated
    return total_updated


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main() -> int:
    if not os.environ.get("DATABASE_URL"):
        logger.error("DATABASE_URL not set; aborting")
        return 2

    ensure_columns()

    total = 0
    total += backfill_sii()
    total += backfill_psi()
    total += backfill_rpi()
    total += backfill_circle7()

    logger.info("Backfill complete: %d total row(s) updated across all indices", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
