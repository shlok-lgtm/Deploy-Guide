"""
OpenSanctions Screening Collector (Pipeline 19)
==================================================
Daily screening of all scored entity issuers and known team wallet addresses
against the OpenSanctions consolidated dataset.

Free API, no key required for basic searches.
Runs daily in the slow cycle.  Never raises — all errors logged and skipped.
"""

import hashlib
import json
import logging
import time
from datetime import date, datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, execute

logger = logging.getLogger(__name__)

OPENSANCTIONS_BASE = "https://api.opensanctions.org"


def _screen_target(target_name: str, target_type: str) -> dict | None:
    """
    Screen a single target against OpenSanctions /match/default endpoint.
    Returns the match response or None on failure.
    """
    schema = "Company" if target_type in ("company", "organization") else "CryptoWallet"

    if schema == "CryptoWallet":
        properties = {"address": [target_name]}
    else:
        properties = {"name": [target_name]}

    payload = {
        "queries": {
            "q0": {
                "schema": schema,
                "properties": properties,
            }
        }
    }

    try:
        resp = httpx.post(
            f"{OPENSANCTIONS_BASE}/match/default",
            json=payload,
            headers={"Accept": "application/json"},
            timeout=30,
        )
        if resp.status_code == 429:
            logger.warning("OpenSanctions rate limited, backing off 60s")
            time.sleep(60)
            resp = httpx.post(
                f"{OPENSANCTIONS_BASE}/match/default",
                json=payload,
                headers={"Accept": "application/json"},
                timeout=30,
            )
        if resp.status_code != 200:
            logger.debug(f"OpenSanctions returned {resp.status_code} for {target_name}")
            return None
        return resp.json()
    except Exception as e:
        logger.debug(f"OpenSanctions request failed for {target_name}: {e}")
        return None


def run_sanctions_screening() -> dict:
    """
    Main collector: load active targets, screen each against OpenSanctions,
    store results.

    Returns summary: {targets_screened, matches_found, skipped_existing}.
    """
    today = date.today()

    # Load active targets
    targets = fetch_all(
        "SELECT * FROM sanctions_screen_targets WHERE active = TRUE"
    )
    if not targets:
        logger.info("Sanctions screening: no active targets")
        return {"targets_screened": 0, "matches_found": 0, "skipped_existing": 0}

    targets_screened = 0
    matches_found = 0
    skipped = 0

    for target in targets:
        try:
            target_name = target["target_name"]
            target_type = target.get("target_type", "company")

            # Daily gate per target
            already_screened = fetch_one(
                """SELECT id FROM sanctions_screening_results
                   WHERE screen_target = %s AND screened_at::date = %s
                   LIMIT 1""",
                (target_name, today),
            )
            if already_screened:
                skipped += 1
                continue

            # Screen against OpenSanctions
            response = _screen_target(target_name, target_type)
            time.sleep(1)  # Rate limit: 1 req/s

            is_match = False
            match_score = 0.0
            match_dataset = None
            match_entity_id = None
            match_details = None

            if response:
                # Parse results from q0 query
                q0_results = response.get("responses", {}).get("q0", {}).get("results", [])
                if not q0_results:
                    q0_results = response.get("results", [])

                for result in q0_results:
                    score = result.get("score", 0)
                    if score > 0.7:
                        is_match = True
                        match_score = max(match_score, score)
                        match_dataset = result.get("datasets", ["unknown"])[0] if result.get("datasets") else "unknown"
                        match_entity_id = result.get("id", "")
                        match_details = result

            # Compute content hash
            now = datetime.now(timezone.utc)
            content_data = f"{target_name}{today.isoformat()}{is_match}{match_score}"
            content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

            # Store result
            execute(
                """INSERT INTO sanctions_screening_results
                    (screened_at, entity_type, entity_id, entity_symbol,
                     screen_target, screen_target_type, is_match, match_score,
                     match_dataset, match_entity_id, match_details,
                     content_hash, attested_at)
                   VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                (
                    target.get("entity_type"),
                    target.get("entity_id"),
                    target.get("entity_symbol"),
                    target_name,
                    target_type,
                    is_match,
                    match_score,
                    match_dataset,
                    match_entity_id,
                    json.dumps(match_details, default=str) if match_details else None,
                    content_hash,
                ),
            )
            targets_screened += 1

            if is_match:
                matches_found += 1
                logger.warning(
                    f"SANCTIONS MATCH: {target.get('entity_symbol')} — "
                    f"{target_name} matched {match_dataset} (score={match_score:.2f})"
                )
            else:
                logger.debug(f"Sanctions clear: {target_name}")

        except Exception as e:
            logger.debug(f"Sanctions screening failed for {target.get('target_name')}: {e}")

    # Attest batch
    if targets_screened > 0:
        try:
            from app.state_attestation import attest_state
            attest_state("sanctions_screening", [{
                "date": today.isoformat(),
                "targets_screened": targets_screened,
                "matches_found": matches_found,
            }])
        except Exception as ae:
            logger.debug(f"Sanctions screening attestation failed: {ae}")

    summary = {
        "targets_screened": targets_screened,
        "matches_found": matches_found,
        "skipped_existing": skipped,
    }
    logger.info(
        f"Sanctions screening: screened={targets_screened} matches={matches_found} skipped={skipped}"
    )
    return summary
