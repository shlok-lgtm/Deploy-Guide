"""
CourtListener / SEC EDGAR Enforcement History Collector (Pipeline 20)
=======================================================================
Weekly query of CourtListener and SEC EDGAR for federal court records,
SEC enforcement actions, and CFTC orders mentioning scored entity issuers.

Free APIs, no key required.
Weekly-gated.  Never raises — all errors logged and skipped.
"""

import hashlib
import json
import logging
import time
from datetime import date, datetime, timezone

import httpx

from app.database import fetch_all, fetch_one, execute
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

COURTLISTENER_BASE = "https://www.courtlistener.com/api/rest/v3"
SEC_EDGAR_SEARCH = "https://efts.sec.gov/LATEST/search-index"

# Entity → search terms mapping
ENFORCEMENT_SEARCH_TERMS = {
    "usdt": ["Tether Limited", "iFinex", "Bitfinex"],
    "usdc": ["Circle Internet Financial", "Centre Consortium"],
    "fdusd": ["First Digital Labs", "First Digital Trust"],
    "pyusd": ["PayPal", "Paxos"],
    "usd1": ["World Liberty Financial"],
    "tusd": ["TrueUSD", "Archblock", "Techteryx"],
    "aave": ["Aave", "Avara"],
    "compound": ["Compound Labs", "Robert Leshner"],
    "maker": ["MakerDAO", "Maker Foundation", "Rune Christensen"],
}


def _search_courtlistener(search_term: str) -> list[dict]:
    """Search CourtListener for opinions/orders matching search term."""
    results = []
    try:
        resp = httpx.get(
            f"{COURTLISTENER_BASE}/search/",
            params={
                "q": f'"{search_term}"',
                "type": "o",
                "order_by": "score desc",
                "stat_Precedential": "on",
                "filed_after": "2020-01-01",
            },
            headers={
                "Accept": "application/json",
                "User-Agent": "BasisProtocol/1.0 (research)",
            },
            timeout=30,
        )
        if resp.status_code == 429:
            logger.warning("CourtListener rate limited, backing off 60s")
            time.sleep(60)
            return results
        if resp.status_code != 200:
            logger.debug(f"CourtListener returned {resp.status_code} for '{search_term}'")
            return results

        data = resp.json()
        for item in data.get("results", [])[:20]:
            results.append({
                "source": "courtlistener",
                "case_name": item.get("caseName", "")[:500],
                "case_date": item.get("dateFiled"),
                "court": item.get("court", ""),
                "docket_number": item.get("docketNumber", ""),
                "record_type": "opinion",
                "summary": (item.get("snippet") or "")[:500],
                "case_url": item.get("download_url", ""),
                "absolute_url": f"https://www.courtlistener.com{item.get('absolute_url', '')}",
            })
    except Exception as e:
        logger.debug(f"CourtListener search failed for '{search_term}': {e}")
    return results


def _search_sec_edgar(search_term: str) -> list[dict]:
    """Search SEC EDGAR full-text for enforcement actions."""
    results = []
    try:
        resp = httpx.get(
            SEC_EDGAR_SEARCH,
            params={
                "q": f'"{search_term}"',
                "dateRange": "custom",
                "startdt": "2020-01-01",
                "forms": "EA",
            },
            headers={
                "User-Agent": "BasisProtocol research@basisprotocol.xyz",
                "Accept": "application/json",
            },
            timeout=30,
        )
        if resp.status_code != 200:
            logger.debug(f"SEC EDGAR returned {resp.status_code} for '{search_term}'")
            return results

        data = resp.json()
        hits = data.get("hits", {}).get("hits", [])
        for hit in hits[:20]:
            source = hit.get("_source", {})
            results.append({
                "source": "sec_edgar",
                "case_name": source.get("file_description", source.get("display_names", [""])[0] if source.get("display_names") else "")[:500],
                "case_date": source.get("file_date"),
                "court": "SEC",
                "docket_number": source.get("file_num", "") or source.get("_id", ""),
                "record_type": "enforcement_action",
                "summary": (source.get("file_description", "") or "")[:500],
                "case_url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum={source.get('file_num', '')}",
                "absolute_url": f"https://efts.sec.gov/LATEST/search-index?q={search_term}",
            })
    except Exception as e:
        logger.debug(f"SEC EDGAR search failed for '{search_term}': {e}")
    return results


def collect_enforcement_records() -> dict:
    """
    Main collector: search CourtListener + SEC EDGAR for enforcement records.
    Weekly-gated per entity.

    Returns summary: {entities_scanned, new_records, skipped_recent}.
    """
    entities_scanned = 0
    new_records = 0
    skipped_recent = 0

    for symbol, search_terms in ENFORCEMENT_SEARCH_TERMS.items():
        try:
            # Weekly gate per entity
            last_scan = fetch_one(
                """SELECT MAX(discovered_at) AS latest FROM enforcement_records
                   WHERE entity_symbol = %s""",
                (symbol,),
            )
            if last_scan and last_scan.get("latest"):
                latest = last_scan["latest"]
                if latest.tzinfo is None:
                    latest = latest.replace(tzinfo=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
                if age_hours < 168:  # 7 days
                    skipped_recent += 1
                    continue

            entities_scanned += 1

            for term in search_terms:
                # Search CourtListener
                cl_results = _search_courtlistener(term)
                time.sleep(1)  # Rate limit

                # Search SEC EDGAR
                sec_results = _search_sec_edgar(term)
                time.sleep(1)  # Rate limit

                all_results = cl_results + sec_results

                for record in all_results:
                    try:
                        docket = record.get("docket_number", "")
                        source = record.get("source", "")

                        if not docket:
                            continue

                        # Dedup check
                        existing = fetch_one(
                            """SELECT id FROM enforcement_records
                               WHERE docket_number = %s AND record_source = %s""",
                            (docket, source),
                        )
                        if existing:
                            continue

                        # Compute content hash
                        case_date_str = record.get("case_date") or ""
                        content_data = f"{docket}{source}{case_date_str}"
                        content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

                        # Look up entity_id from stablecoins or psi_scores
                        entity_id = None
                        entity_type = "stablecoin_issuer"
                        row = fetch_one(
                            "SELECT id FROM stablecoins WHERE LOWER(symbol) = %s",
                            (symbol.lower(),),
                        )
                        if row:
                            entity_id = row["id"]
                        else:
                            row = fetch_one(
                                "SELECT id FROM psi_scores WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1",
                                (symbol,),
                            )
                            if row:
                                entity_id = row["id"]
                                entity_type = "protocol_team"

                        execute(
                            """INSERT INTO enforcement_records
                                (entity_type, entity_id, entity_symbol, search_term,
                                 record_source, case_name, case_date, court,
                                 docket_number, record_type, summary, case_url,
                                 absolute_url, content_hash, attested_at)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                               ON CONFLICT (docket_number, record_source) DO NOTHING""",
                            (
                                entity_type,
                                entity_id,
                                symbol,
                                term,
                                source,
                                record.get("case_name"),
                                record.get("case_date"),
                                record.get("court"),
                                docket,
                                record.get("record_type"),
                                record.get("summary"),
                                record.get("case_url"),
                                record.get("absolute_url"),
                                content_hash,
                            ),
                        )
                        new_records += 1
                        logger.warning(
                            f"ENFORCEMENT RECORD FOUND: {symbol} — {record.get('case_name', '')[:80]}"
                        )

                    except Exception as e:
                        logger.debug(f"Failed to store enforcement record: {e}")

            # Attest per entity
            if new_records > 0:
                try:
                    from app.state_attestation import attest_state
                    attest_state("enforcement_records", [{
                        "entity_symbol": symbol,
                        "new_records": new_records,
                        "scanned_at": datetime.now(timezone.utc).isoformat(),
                    }])
                except Exception as ae:
                    logger.debug(f"Enforcement attestation failed: {ae}")

            logger.info(f"ENFORCEMENT SCAN: {symbol} found {new_records} new records")

        except Exception as e:
            logger.debug(f"Enforcement scan failed for {symbol}: {e}")

    summary = {
        "entities_scanned": entities_scanned,
        "new_records": new_records,
        "skipped_recent": skipped_recent,
    }
    logger.info(
        f"Enforcement history: scanned={entities_scanned} new={new_records} skipped={skipped_recent}"
    )
    return summary
