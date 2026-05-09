"""
Regulatory Lens System
=======================
Each lens maps SII/PSI components to regulatory criteria.
Lenses are JSON configs loaded at startup, with database-backed
custom lenses via the lens_configs table. The apply_lens()
function classifies an entity against a regulatory framework.
"""

import hashlib
import json
import os
import logging

logger = logging.getLogger(__name__)

_LENS_DIR = os.path.dirname(__file__)
_LENS_CACHE: dict = {}


def _compute_content_hash(criteria: dict) -> str:
    """SHA-256 of canonical JSON for a lens criteria dict."""
    canonical = json.dumps(criteria, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _db_row_to_lens_config(row: dict) -> dict:
    """Convert a lens_configs DB row to the in-memory lens config format."""
    criteria = row["criteria"]
    if isinstance(criteria, str):
        criteria = json.loads(criteria)
    return {
        "lens_id": row["lens_id"],
        "lens_version": row["version"],
        "framework": criteria.get("framework", row["name"]),
        "description": row.get("description", ""),
        "classification": criteria.get("classification", {}),
        "_db_row": row,
    }


def load_lens(lens_id: str) -> dict | None:
    """Load a lens config by ID. Checks DB first, then JSON files."""
    if lens_id in _LENS_CACHE:
        return _LENS_CACHE[lens_id]

    # Try database first
    try:
        from app.database import fetch_one
        row = fetch_one(
            "SELECT * FROM lens_configs WHERE lens_id = %s", (lens_id,)
        )
        if row:
            config = _db_row_to_lens_config(row)
            _LENS_CACHE[lens_id] = config
            return config
    except Exception as e:
        logger.warning(f"lens_configs table lookup failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="lenses_load_lens_db_lookup_failure",
                error_message=str(e)[:500],
                cycle_phase="lenses_load_lens",
            )
        except Exception:
            pass

    # Fallback to JSON file
    path = os.path.join(_LENS_DIR, f"{lens_id}.json")
    if not os.path.exists(path):
        logger.warning(f"Lens not found: {lens_id}")
        return None

    with open(path) as f:
        config = json.load(f)
    _LENS_CACHE[lens_id] = config
    return config


def load_lens_from_db(lens_id: str) -> dict | None:
    """Load a lens config strictly from the database. Returns None if not found."""
    try:
        from app.database import fetch_one
        row = fetch_one(
            "SELECT * FROM lens_configs WHERE lens_id = %s", (lens_id,)
        )
        if row:
            return _db_row_to_lens_config(row)
    except Exception as e:
        logger.warning(f"lens_configs table lookup failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="lenses_load_lens_from_db_failure",
                error_message=str(e)[:500],
                cycle_phase="lenses_load_lens_from_db",
            )
        except Exception:
            pass
    return None


def list_lenses() -> list[dict]:
    """List all available lenses (DB + JSON files, deduplicated)."""
    seen = set()
    lenses = []

    # DB lenses first
    try:
        from app.database import fetch_all
        rows = fetch_all(
            "SELECT lens_id, name, version, author, description, content_hash, created_at "
            "FROM lens_configs ORDER BY lens_id"
        )
        for row in rows:
            seen.add(row["lens_id"])
            lenses.append({
                "lens_id": row["lens_id"],
                "name": row["name"],
                "version": row["version"],
                "author": row["author"],
                "description": row.get("description"),
                "content_hash": row.get("content_hash"),
                "source": "database",
            })
    except Exception as e:
        logger.warning(f"lens_configs table listing failed: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="lenses_list_lenses_db_listing_failure",
                error_message=str(e)[:500],
                cycle_phase="lenses_list_lenses",
            )
        except Exception:
            pass

    # JSON file lenses (only if not already in DB)
    for fname in sorted(os.listdir(_LENS_DIR)):
        if fname.endswith(".json"):
            config = load_lens(fname.replace(".json", ""))
            if config and config.get("lens_id") not in seen:
                seen.add(config.get("lens_id"))
                lenses.append({
                    "lens_id": config.get("lens_id"),
                    "name": config.get("framework"),
                    "version": config.get("lens_version"),
                    "author": "basis-protocol",
                    "description": config.get("description", ""),
                    "source": "builtin",
                })

    return lenses


def apply_lens(lens_config: dict, report_data: dict) -> dict:
    """
    Apply a regulatory lens to assembled report data.
    Returns classification result with per-criterion pass/fail.
    """
    lens_id = lens_config.get("lens_id", "unknown")
    framework = lens_config.get("framework", "Unknown Framework")
    classification = lens_config.get("classification", {})

    results = {}
    overall_pass = True

    for group_id, group in classification.items():
        criteria = group.get("criteria", [])
        all_required = group.get("all_required", True)
        criterion_results = []

        for criterion in criteria:
            passed = _evaluate_criterion(criterion, report_data)
            criterion_results.append({
                "name": criterion["name"],
                "passed": passed,
                "threshold": criterion.get("threshold"),
                "categories": criterion.get("sii_categories", []),
                "logic": criterion.get("logic"),
            })

        group_passed = all(c["passed"] for c in criterion_results) if all_required \
            else any(c["passed"] for c in criterion_results)
        if not group_passed:
            overall_pass = False

        results[group_id] = {
            "passed": group_passed,
            "all_required": all_required,
            "criteria": criterion_results,
        }

    return {
        "lens_id": lens_id,
        "lens_version": lens_config.get("lens_version"),
        "framework": framework,
        "classification": results,
        "overall_pass": overall_pass,
    }


def _evaluate_criterion(criterion: dict, report_data: dict) -> bool:
    """Evaluate a single criterion against report data."""
    logic = criterion.get("logic", "category_score_above")
    threshold = criterion.get("threshold", 0)
    categories = criterion.get("sii_categories", [])

    cat_scores = report_data.get("categories") or report_data.get("category_scores") or {}

    if logic == "category_score_above":
        for cat in categories:
            val = cat_scores.get(cat)
            if isinstance(val, dict):
                val = val.get("score")
            if val is None or float(val) < threshold:
                return False
        return True

    if logic == "sub_score_above":
        sub_cats = criterion.get("sub_categories", [])
        structural = report_data.get("structural_breakdown") or {}
        for sub in sub_cats:
            val = structural.get(sub)
            if isinstance(val, dict):
                val = val.get("score")
            if val is None or float(val) < threshold:
                return False
        return True

    return False
