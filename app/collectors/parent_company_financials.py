"""
SEC EDGAR XBRL Parent Company Health Collector (Pipeline 21)
===============================================================
For stablecoins whose issuers are subsidiaries of public companies,
pull quarterly financial health data from EDGAR XBRL API and store
as permanent attested state.

All SEC EDGAR APIs are free, no key required.
Weekly-gated internally.  Never raises — all errors logged and skipped.
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

SEC_EDGAR_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# Required User-Agent for SEC EDGAR (they block generic agents)
SEC_HEADERS = {
    "User-Agent": "BasisProtocol research@basisprotocol.xyz",
    "Accept": "application/json",
}

# XBRL fact concepts we extract
FACT_CONCEPTS = {
    "total_assets_usd": [
        "us-gaap/Assets",
    ],
    "total_liabilities_usd": [
        "us-gaap/Liabilities",
    ],
    "total_equity_usd": [
        "us-gaap/StockholdersEquity",
        "us-gaap/StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ],
    "revenue_usd": [
        "us-gaap/Revenues",
        "us-gaap/RevenueFromContractWithCustomerExcludingAssessedTax",
    ],
    "net_income_usd": [
        "us-gaap/NetIncomeLoss",
    ],
    "cash_and_equivalents_usd": [
        "us-gaap/CashAndCashEquivalentsAtCarryingValue",
        "us-gaap/CashCashEquivalentsAndShortTermInvestments",
    ],
}


def _fetch_company_facts(cik: str) -> dict | None:
    """Fetch XBRL company facts from SEC EDGAR."""
    # Zero-pad CIK to 10 digits
    padded_cik = cik.lstrip("0").zfill(10)
    url = SEC_EDGAR_FACTS_URL.format(cik=padded_cik)

    try:
        resp = httpx.get(url, headers=SEC_HEADERS, timeout=30)
        if resp.status_code != 200:
            logger.debug(f"SEC EDGAR returned {resp.status_code} for CIK {cik}")
            return None
        return resp.json()
    except Exception as e:
        logger.debug(f"SEC EDGAR fetch failed for CIK {cik}: {e}")
        return None


def _extract_fact_value(facts: dict, concept_paths: list[str]) -> list[dict]:
    """
    Extract fact values from EDGAR XBRL response.
    Returns list of {value, end_date, fiscal_year, fiscal_period, form} dicts.
    """
    results = []
    us_gaap = facts.get("facts", {}).get("us-gaap", {})

    for concept_path in concept_paths:
        concept_name = concept_path.split("/")[-1]
        concept_data = us_gaap.get(concept_name, {})
        units = concept_data.get("units", {})

        # Try USD first, then pure number
        values = units.get("USD", units.get("USD/shares", []))
        if not values:
            continue

        for entry in values:
            form = entry.get("form", "")
            if form not in ("10-K", "10-Q"):
                continue

            fp = entry.get("fp", "")
            fy = entry.get("fy")
            end_date = entry.get("end")
            val = entry.get("val")

            if val is not None and end_date:
                results.append({
                    "value": val,
                    "end_date": end_date,
                    "fiscal_year": fy,
                    "fiscal_period": fp,
                    "form": form,
                })

        if results:
            break  # Use first matching concept

    return results


def _infer_fiscal_period(fp: str, form: str) -> str:
    """Normalize fiscal period string."""
    if form == "10-K":
        return "FY"
    fp_upper = (fp or "").upper()
    if fp_upper in ("Q1", "Q2", "Q3", "Q4", "FY"):
        return fp_upper
    return fp or "Q0"


def collect_parent_financials() -> dict:
    """
    Main collector: load parent company registry, fetch EDGAR XBRL data,
    store quarterly financials.
    Weekly-gated internally.

    Returns summary: {companies_checked, quarters_stored, skipped_existing}.
    """
    # Load registry
    companies = fetch_all(
        "SELECT * FROM parent_company_registry WHERE active = TRUE"
    )
    if not companies:
        logger.info("Parent company financials: no active companies in registry")
        return {"companies_checked": 0, "quarters_stored": 0, "skipped_existing": 0}

    # Weekly gate: check last run
    last_run = fetch_one(
        "SELECT MAX(captured_at) AS latest FROM parent_company_financials"
    )
    if last_run and last_run.get("latest"):
        latest = last_run["latest"]
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
        if age_hours < 168:  # 7 days
            logger.info(f"Parent company financials: last ran {age_hours:.0f}h ago, skipping (weekly gate)")
            return {"companies_checked": 0, "quarters_stored": 0, "skipped_existing": 0}

    companies_checked = 0
    quarters_stored = 0
    skipped_existing = 0

    # Deduplicate by CIK
    seen_ciks = set()

    for company in companies:
        try:
            cik = company["sec_cik"]
            company_name = company["company_name"]

            if cik in seen_ciks:
                continue
            seen_ciks.add(cik)

            logger.info(f"Fetching EDGAR data for {company_name} (CIK {cik})")
            facts = _fetch_company_facts(cik)
            time.sleep(0.2)  # SEC rate limit: max 10 req/s

            if not facts:
                continue

            companies_checked += 1

            # Extract all financial metrics
            extracted = {}
            for metric, concepts in FACT_CONCEPTS.items():
                values = _extract_fact_value(facts, concepts)
                for v in values:
                    key = (v["fiscal_year"], v["fiscal_period"])
                    if key not in extracted:
                        extracted[key] = {
                            "fiscal_year": v["fiscal_year"],
                            "fiscal_period": _infer_fiscal_period(v["fiscal_period"], v["form"]),
                            "period_end_date": v["end_date"],
                        }
                    extracted[key][metric] = v["value"]

            # Get most recent 4 quarters
            sorted_quarters = sorted(
                extracted.values(),
                key=lambda x: x.get("period_end_date", ""),
                reverse=True,
            )[:4]

            for quarter in sorted_quarters:
                fy = quarter.get("fiscal_year")
                fp = quarter.get("fiscal_period")

                if not fy or not fp:
                    continue

                # Check if already stored
                existing = fetch_one(
                    """SELECT id FROM parent_company_financials
                       WHERE cik = %s AND fiscal_year = %s AND fiscal_period = %s""",
                    (cik, fy, fp),
                )
                if existing:
                    skipped_existing += 1
                    continue

                # Compute derived metrics
                total_assets = quarter.get("total_assets_usd")
                total_liabilities = quarter.get("total_liabilities_usd")
                total_equity = quarter.get("total_equity_usd")

                debt_to_equity = None
                if total_liabilities and total_equity and total_equity != 0:
                    debt_to_equity = round(total_liabilities / total_equity, 4)

                # Content hash
                content_data = f"{cik}{fy}{fp}{total_assets or 0}"
                content_hash = "0x" + hashlib.sha256(content_data.encode()).hexdigest()

                execute(
                    """INSERT INTO parent_company_financials
                        (cik, company_name, fiscal_period, fiscal_year, period_end_date,
                         total_assets_usd, total_liabilities_usd, total_equity_usd,
                         revenue_usd, net_income_usd, cash_and_equivalents_usd,
                         debt_to_equity, content_hash, attested_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (cik, fiscal_year, fiscal_period) DO NOTHING""",
                    (
                        cik,
                        company_name,
                        fp,
                        fy,
                        quarter.get("period_end_date"),
                        total_assets,
                        total_liabilities,
                        total_equity,
                        quarter.get("revenue_usd"),
                        quarter.get("net_income_usd"),
                        quarter.get("cash_and_equivalents_usd"),
                        debt_to_equity,
                        content_hash,
                    ),
                )
                quarters_stored += 1

        except Exception as e:
            logger.debug(f"Parent company financials failed for {company.get('company_name')}: {e}")

    # Attest batch
    if quarters_stored > 0:
        try:
            from app.state_attestation import attest_state
            attest_state("parent_company_financials", [{
                "companies_checked": companies_checked,
                "quarters_stored": quarters_stored,
                "date": date.today().isoformat(),
            }])
        except Exception as ae:
            logger.debug(f"Parent company financials attestation failed: {ae}")

    summary = {
        "companies_checked": companies_checked,
        "quarters_stored": quarters_stored,
        "skipped_existing": skipped_existing,
    }
    logger.info(
        f"Parent company financials: checked={companies_checked} stored={quarters_stored} skipped={skipped_existing}"
    )
    return summary
