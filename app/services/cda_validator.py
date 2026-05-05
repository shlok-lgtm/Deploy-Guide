"""
CDA Validation — Runs per-issuer verification rules against extracted data.
Called after each successful extraction to produce a validation result record.
"""
import json
import logging
from datetime import datetime, timezone

from app.database import fetch_one, execute

logger = logging.getLogger(__name__)


def validate_extraction(extraction_id: int, asset_symbol: str, structured_data: dict, disclosure_type: str) -> dict:
    """Run verification rules for this issuer type against extracted data.
    Stores result in cda_validation_results. Returns summary."""
    reg = fetch_one(
        "SELECT verification_rules FROM cda_issuer_registry WHERE UPPER(asset_symbol) = %s",
        (asset_symbol.upper(),)
    )
    rules_config = (reg or {}).get("verification_rules", [])
    if isinstance(rules_config, str):
        rules_config = json.loads(rules_config)

    results = []
    for rule_def in (rules_config or []):
        rule_name = rule_def.get("rule", "unknown")
        result = _run_rule(rule_name, structured_data, disclosure_type)
        result["rule"] = rule_name
        result["description"] = rule_def.get("description", "")
        result["severity"] = rule_def.get("severity", "warning")
        results.append(result)

    passed = sum(1 for r in results if r.get("passed"))
    failed = sum(1 for r in results if not r.get("passed") and r.get("applicable", True))
    total = len(results)

    if failed > 0:
        overall = "failed"
    elif passed == total and total > 0:
        overall = "valid"
    elif total == 0:
        overall = "not_applicable"
    else:
        overall = "warning"

    execute(
        """
        INSERT INTO cda_validation_results
            (extraction_id, asset_symbol, rules_applied, rules_passed, rules_failed, rules_total, overall_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (extraction_id, asset_symbol.upper(), json.dumps(results), passed, failed, total, overall),
    )

    logger.info(f"CDA validation {asset_symbol}: {passed}/{total} passed, status={overall}")
    return {"passed": passed, "failed": failed, "total": total, "status": overall, "results": results}


def _run_rule(rule_name: str, data: dict, disclosure_type: str) -> dict:
    """Execute a single validation rule."""
    def _val(v):
        if isinstance(v, dict) and "value" in v:
            return v["value"]
        return v

    if rule_name == "reserves_gte_supply":
        reserves = _val(data.get("total_reserves_usd"))
        supply = _val(data.get("total_supply"))
        if reserves is None or supply is None:
            return {"passed": False, "applicable": False, "message": "Missing reserves or supply data"}
        try:
            r, s = float(reserves), float(supply)
            passed = r >= s * 0.99
            return {"passed": passed, "expected": f">= {s}", "actual": str(r), "message": f"Reserves ${r:,.0f} vs Supply {s:,.0f}"}
        except (ValueError, TypeError):
            return {"passed": False, "applicable": False, "message": "Could not parse values"}

    elif rule_name == "collateral_ratio_gte_1":
        cr = _val(data.get("collateral_ratio"))
        if cr is None:
            backing = data.get("backing_assets", {})
            total_backing = _val(backing.get("total_value_usd")) if isinstance(backing, dict) else None
            supply = _val(data.get("total_supply"))
            if total_backing and supply:
                try:
                    cr = float(total_backing) / float(supply)
                except (ValueError, TypeError, ZeroDivisionError) as e:
                    logger.warning(f"cda_validator: _run_rule collateral_ratio compute failed: {e}")
                    try:
                        from app.worker import _record_cycle_error
                        _record_cycle_error(
                            error_type="services__run_rule_collateral_ratio_compute_failure",
                            error_message=str(e)[:500],
                            cycle_phase="cda_validator",
                        )
                    except Exception:
                        pass
        if cr is None:
            return {"passed": False, "applicable": False, "message": "No collateral ratio data"}
        try:
            cr = float(cr)
            passed = cr >= 0.99
            return {"passed": passed, "expected": ">= 1.0", "actual": f"{cr:.4f}", "message": f"Collateral ratio: {cr:.4f}"}
        except (ValueError, TypeError):
            return {"passed": False, "applicable": False, "message": "Could not parse collateral ratio"}

    elif rule_name == "custodians_present":
        custodians = data.get("custodians", [])
        has_custodians = isinstance(custodians, list) and len(custodians) > 0
        count = len(custodians) if isinstance(custodians, list) else 0
        return {"passed": has_custodians, "expected": "at least 1 custodian", "actual": str(count), "message": f"{count} custodians found"}

    elif rule_name == "nav_positive":
        nav = _val(data.get("nav_per_token"))
        if nav is None:
            return {"passed": False, "applicable": False, "message": "No NAV data"}
        try:
            n = float(nav)
            return {"passed": n > 0, "expected": "> 0", "actual": str(n), "message": f"NAV per token: {n}"}
        except (ValueError, TypeError):
            return {"passed": False, "applicable": False, "message": "Could not parse NAV"}

    return {"passed": False, "applicable": False, "message": f"Unknown rule: {rule_name}"}
