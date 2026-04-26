"""
TTI Issuer Disclosure Collector
=================================
CDA-pattern extension for tokenized treasury product disclosures.
Scrapes issuer websites and attestation PDFs to automate 19 TTI components.

Phase 3A: Uses Firecrawl (JS page scraping) + Reducto (PDF parsing) via
existing clients in app/services/.

Data flow:
  1. Check daily gate (skip if collected today)
  2. Firecrawl scrape product page → markdown
  3. Search markdown for PDF links (attestations, prospectuses)
  4. If PDFs found → Reducto parse with TTI_DISCLOSURE_SCHEMA
  5. Extract from page markdown if no PDFs
  6. Store extraction in tti_disclosure_extractions table
  7. Map extractions → raw_values for scoring engine
"""

import json
import logging
import re
import time
from datetime import date, datetime, timezone

from app.database import execute, fetch_one
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

PDF_URL_PATTERN = re.compile(r'https?://[^\s\)\"\'<>]+\.pdf', re.IGNORECASE)

# =============================================================================
# Issuer disclosure registry
# =============================================================================

TTI_DISCLOSURE_URLS = {
    "ondo-ousg": [
        {"url": "https://ondo.finance/ousg", "type": "product_page"},
    ],
    "ondo-usdy": [
        {"url": "https://ondo.finance/usdy", "type": "product_page"},
    ],
    "blackrock-buidl": [
        {"url": "https://securitize.io/invest/blackrock", "type": "product_page"},
    ],
    "franklin-benji": [
        {"url": "https://www.franklintempleton.com/strategies/blockchain-ventures", "type": "product_page"},
    ],
    "mountain-usdm": [
        {"url": "https://mountainprotocol.com/", "type": "product_page"},
        {"url": "https://docs.mountainprotocol.com/", "type": "docs_page"},
    ],
}

# Tier mappings for scoring
BANK_TIERS = {
    "jpmorgan": 95, "jp morgan": 95, "goldman sachs": 95, "morgan stanley": 95,
    "bank of new york": 95, "bny mellon": 95, "state street": 90,
    "citibank": 90, "citi": 90, "hsbc": 85, "barclays": 85,
    "wells fargo": 85, "u.s. bank": 80, "pnc": 75,
    "signature bank": 60, "silvergate": 50, "cross river": 60,
    "customers bank": 60, "lead bank": 55,
}

CUSTODIAN_TIERS = {
    "bny mellon": 95, "bank of new york": 95, "state street": 90,
    "northern trust": 90, "jpmorgan": 90, "citibank": 85,
    "bitgo": 70, "anchorage": 70, "coinbase custody": 70, "coinbase prime": 70,
    "fireblocks": 65, "copper": 65, "hex trust": 60,
}

JURISDICTION_RISK = {
    "us": 60, "united states": 60,       # changing fast
    "uk": 70, "united kingdom": 70,
    "switzerland": 75, "swiss": 75,
    "singapore": 70,
    "cayman islands": 40, "cayman": 40,
    "bvi": 40, "british virgin islands": 40,
    "bermuda": 50,
    "germany": 75, "france": 70, "ireland": 70,
    "hong kong": 65,
}

JURISDICTION_SOVEREIGN = {
    "us": 95, "united states": 95,
    "uk": 90, "united kingdom": 90,
    "switzerland": 92, "swiss": 92,
    "singapore": 90,
    "cayman islands": 70, "cayman": 70,
    "bvi": 60, "british virgin islands": 60,
    "bermuda": 65,
    "germany": 90, "france": 85, "ireland": 85,
    "hong kong": 85,
}


# =============================================================================
# Daily gate (follows cda_collector._already_collected_today pattern)
# =============================================================================

def _already_collected_today(entity_slug: str) -> bool:
    """Check if we already have an extraction for this entity today."""
    row = fetch_one(
        """SELECT id FROM tti_disclosure_extractions
           WHERE entity_slug = %s AND extracted_at::date = %s
           LIMIT 1""",
        (entity_slug, date.today().isoformat()),
    )
    return row is not None


def _store_extraction(entity_slug: str, entity_name: str, source_url: str,
                      source_type: str, structured_data: dict,
                      extraction_method: str, confidence: float):
    """Store an extraction in the tti_disclosure_extractions table."""
    # Ensure simple unique index exists (expression index doesn't work with ON CONFLICT)
    try:
        execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_tti_disc_entity_source
            ON tti_disclosure_extractions(entity_slug, source_url)
        """)
    except Exception:
        pass

    execute("""
        INSERT INTO tti_disclosure_extractions
            (entity_slug, entity_name, source_url, source_type,
             structured_data, extraction_method, confidence, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (entity_slug, source_url)
        DO UPDATE SET
            structured_data = EXCLUDED.structured_data,
            extraction_method = EXCLUDED.extraction_method,
            confidence = EXCLUDED.confidence,
            extracted_at = EXCLUDED.extracted_at
    """, (
        entity_slug, entity_name, source_url, source_type,
        json.dumps(structured_data), extraction_method, confidence,
    ))


# =============================================================================
# Page scraping — extract structured data from markdown
# =============================================================================

def _extract_from_markdown(markdown: str) -> dict:
    """Extract TTI disclosure fields from page markdown using regex patterns.

    This is the fallback when no PDF is available — many issuer facts
    are stated on product pages as plain text.
    """
    data = {}
    text = markdown.lower() if markdown else ""

    # Regulatory registrations
    reg_patterns = [
        r'(?:registered|regulated)\s+(?:with|by|under)\s+(?:the\s+)?([A-Z][A-Za-z\s]+)',
        r'(?:sec|finra|cftc|fca|finma|mas|esma)\s+(?:registered|regulated|licensed)',
        r'reg(?:ulation)?\s+(?:d|s|a\+)\b',
    ]
    registrations = []
    for pat in reg_patterns:
        matches = re.findall(pat, text, re.IGNORECASE)
        registrations.extend(matches)
    if registrations:
        data["regulatory_registrations"] = list(set(str(r).strip() for r in registrations[:10]))

    # Custodians
    for name in CUSTODIAN_TIERS:
        if name.lower() in text:
            data.setdefault("custodians", []).append(name.title())

    # Banking partners
    for name in BANK_TIERS:
        if name.lower() in text:
            data.setdefault("banking_partners", []).append(name.title())

    # KYC/AML
    if any(kw in text for kw in ["kyc", "know your customer", "aml", "anti-money laundering"]):
        data["kyc_aml_required"] = True

    # Accredited investors
    if any(kw in text for kw in ["accredited investor", "qualified purchaser", "reg d"]):
        data["accredited_only"] = True

    # Prospectus
    if any(kw in text for kw in ["prospectus", "offering memorandum", "offering circular", "ppm"]):
        data["prospectus_available"] = True

    # Transfer restrictions
    if any(kw in text for kw in ["transfer restrict", "non-transferable", "whitelist",
                                   "transfer agent", "restricted token"]):
        data["transfer_restrictions_described"] = True

    # Tax reporting
    if any(kw in text for kw in ["tax report", "1099", "k-1", "tax statement", "tax document"]):
        data["tax_reporting_provided"] = True

    # Conflict of interest
    if any(kw in text for kw in ["conflict of interest", "conflicts policy", "material conflict"]):
        data["conflict_of_interest_disclosed"] = True

    # Business continuity
    if any(kw in text for kw in ["business continuity", "disaster recovery", "operational resilience"]):
        data["business_continuity_plan"] = True

    # Collateral segregation
    if any(kw in text for kw in ["segregat", "ring-fenced", "bankruptcy remote",
                                   "special purpose vehicle", "spv"]):
        data["collateral_segregation_disclosed"] = True

    # Rehypothecation
    if any(kw in text for kw in ["rehypothecation", "re-hypothecation", "no rehypothecation",
                                   "assets shall not be"]):
        data["rehypothecation_prohibited"] = True

    # Securities registration
    if any(kw in text for kw in ["securities act", "sec registered", "regulation d", "reg d",
                                   "regulation s", "reg s", "regulation a"]):
        data["securities_registered"] = True
        # Try to find exemption type
        for exemption in ["reg d 506(c)", "reg d 506(b)", "regulation d", "reg s",
                          "regulation s", "reg a+", "regulation a+"]:
            if exemption in text:
                data["exemption_type"] = exemption.upper()
                break

    # Jurisdiction
    for j_name, _ in JURISDICTION_RISK.items():
        if j_name in text and len(j_name) > 3:  # skip short matches
            data["jurisdiction"] = j_name.title()
            break

    # Settlement time
    settle_match = re.search(r'(?:settlement|redemption)\s+(?:within|in|time[:\s]+)\s*(\d+)\s*(?:hour|hr|business day|day)', text)
    if settle_match:
        hours = int(settle_match.group(1))
        if "day" in settle_match.group(0).lower():
            hours *= 24
        data["settlement_time_hours"] = hours

    # Attestation frequency
    for freq in ["monthly", "quarterly", "semi-annual", "annually", "daily"]:
        if freq in text and any(kw in text for kw in ["attestation", "audit", "report", "nav"]):
            data["attestation_frequency"] = freq
            break

    # Auditor name
    for auditor in ["deloitte", "kpmg", "pwc", "ey", "ernst & young", "grant thornton",
                     "bdo", "withum", "armanino", "the network firm"]:
        if auditor in text:
            data["auditor_name"] = auditor.title()
            break

    # Named officers (look for "CEO", "CTO", etc. patterns)
    officer_matches = re.findall(
        r'(?:ceo|cto|cfo|coo|chief|founder|president|director)[:\s,]+([A-Z][a-z]+ [A-Z][a-z]+)',
        markdown or "", re.IGNORECASE,
    )
    if officer_matches:
        data["named_officers"] = list(set(officer_matches[:10]))

    return data


def _try_scrape_page(url: str, entity_slug: str = "") -> str | None:
    """Scrape a page using Parallel Extract (primary) or Firecrawl (fallback).

    Parallel Extract is preferred because it does objective-guided extraction —
    much better at pulling disclosure facts from complex JS-rendered product pages.
    """
    # Try Parallel Extract first (objective-guided, handles JS pages)
    try:
        import asyncio
        from app.services import parallel_client

        async def _extract():
            return await parallel_client.extract(
                url,
                objective=(
                    "Extract issuer disclosure information: regulatory registrations, "
                    "custodians, banking partners, KYC/AML requirements, accreditation, "
                    "prospectus availability, attestation details, redemption terms, "
                    "transfer restrictions, tax reporting, jurisdiction, named officers, "
                    "settlement time, business continuity, conflict of interest disclosures"
                ),
            )

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, _extract())
                result = future.result(timeout=130)
        else:
            result = asyncio.run(_extract())

        if result and "error" not in result:
            results_list = result.get("results", [])
            if results_list:
                content = results_list[0].get("full_content", "")
                if content and len(content) > 100:
                    logger.info(f"TTI Parallel Extract OK for {entity_slug}: {len(content)} chars")
                    return content
    except Exception as e:
        logger.debug(f"Parallel Extract failed for {url}: {e}")

    # Fallback to Firecrawl (JS-rendered page scraping)
    try:
        from app.services.firecrawl_client import scrape_js_page
        result = scrape_js_page(url, wait_ms=3000)
        if result and hasattr(result, 'markdown') and result.markdown:
            return result.markdown
        if isinstance(result, dict) and result.get("markdown"):
            return result["markdown"]
    except Exception as e:
        logger.debug(f"Firecrawl scrape failed for {url}: {e}")

    # Last resort: plain requests
    try:
        import requests
        _t0 = time.monotonic()
        _status = None
        try:
            resp = requests.get(url, timeout=15, allow_redirects=True)
            _status = resp.status_code
        except Exception:
            _status = 0
            raise
        finally:
            try:
                from urllib.parse import urlparse as _urlparse
                _provider = _urlparse(url).netloc or "unknown"
                track_api_call(provider=_provider, endpoint="GET", caller="services.tti_disclosure_collector", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
            except Exception:
                pass
        if resp.status_code == 200:
            text = re.sub(r'<[^>]+>', ' ', resp.text)
            return re.sub(r'\s+', ' ', text).strip()
    except Exception as e:
        logger.debug(f"Requests fallback also failed for {url}: {e}")

    return None


def _try_parallel_task_research(entity_slug: str, entity_name: str, issuer: str) -> dict | None:
    """Use Parallel Task deep research to find issuer disclosure facts.

    This is the most powerful tool — it searches the web and synthesizes
    structured data. Used when page scraping yields sparse results.
    Follows the exact pattern from cda_collector._step_parallel_task.
    """
    try:
        import asyncio
        from app.services import parallel_client

        research_q = (
            f"Research the tokenized treasury product {entity_name} issued by {issuer}. "
            f"Find: regulatory registrations (SEC, FINRA, state), custodians, banking partners, "
            f"KYC/AML requirements, whether accredited-only, prospectus/offering memo availability, "
            f"attestation frequency and auditor, settlement time, jurisdiction, named officers/directors, "
            f"business continuity plan, conflict of interest disclosures, transfer restrictions."
        )
        research_fields = {
            "regulatory_registrations": "List of regulatory registrations (SEC RIA, FINRA, state MTLs)",
            "custodians": "Names of custodians holding the underlying assets",
            "banking_partners": "Banking partners used by the issuer",
            "kyc_aml_required": "Whether KYC/AML verification is required (yes/no)",
            "accredited_only": "Whether limited to accredited investors (yes/no)",
            "prospectus_available": "Whether a prospectus or offering memo is publicly available (yes/no)",
            "auditor_name": "Name of the auditing/attestation firm",
            "attestation_frequency": "How often attestations are published (monthly/quarterly/annually)",
            "jurisdiction": "Primary legal jurisdiction of the issuer",
            "settlement_time_hours": "Redemption settlement time in hours",
            "named_officers": "Named officers, CEO, CTO, directors",
            "securities_registered": "Whether registered as a security or exempt (Reg D, Reg S, etc.)",
        }

        async def _run():
            return await parallel_client.task(
                question=research_q,
                fields=research_fields,
                processor="base",  # $0.005 per call — cost-effective
            )

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, _run())
                result = future.result(timeout=330)
        else:
            result = asyncio.run(_run())

        if result and "error" not in result:
            fields = result.get("fields", result.get("data", {}))
            if fields and isinstance(fields, dict):
                # Convert Parallel Task structured output to our extraction format
                data = {}
                if fields.get("regulatory_registrations"):
                    regs = fields["regulatory_registrations"]
                    data["regulatory_registrations"] = (
                        [r.strip() for r in regs.split(",")] if isinstance(regs, str)
                        else regs
                    )
                if fields.get("custodians"):
                    custs = fields["custodians"]
                    data["custodians"] = (
                        [c.strip() for c in custs.split(",")] if isinstance(custs, str)
                        else custs
                    )
                if fields.get("banking_partners"):
                    banks = fields["banking_partners"]
                    data["banking_partners"] = (
                        [b.strip() for b in banks.split(",")] if isinstance(banks, str)
                        else banks
                    )
                if _is_yes(fields.get("kyc_aml_required")):
                    data["kyc_aml_required"] = True
                if _is_yes(fields.get("accredited_only")):
                    data["accredited_only"] = True
                if _is_yes(fields.get("prospectus_available")):
                    data["prospectus_available"] = True
                if _is_yes(fields.get("securities_registered")):
                    data["securities_registered"] = True
                    exemption = fields.get("securities_registered", "")
                    for ex in ["reg d", "reg s", "reg a"]:
                        if ex in exemption.lower():
                            data["exemption_type"] = ex.upper()
                            break
                if fields.get("auditor_name"):
                    data["auditor_name"] = fields["auditor_name"]
                if fields.get("attestation_frequency"):
                    data["attestation_frequency"] = fields["attestation_frequency"]
                if fields.get("jurisdiction"):
                    data["jurisdiction"] = fields["jurisdiction"]
                if fields.get("named_officers"):
                    officers = fields["named_officers"]
                    data["named_officers"] = (
                        [o.strip() for o in officers.split(",")] if isinstance(officers, str)
                        else officers
                    )
                settle = fields.get("settlement_time_hours")
                if settle:
                    try:
                        data["settlement_time_hours"] = int(re.sub(r'[^\d]', '', str(settle)))
                    except (ValueError, TypeError):
                        pass

                logger.info(f"TTI Parallel Task OK for {entity_slug}: {len(data)} fields")
                return data
    except Exception as e:
        logger.warning(f"Parallel Task research failed for {entity_slug}: {e}")

    return None


def _is_yes(value) -> bool:
    """Check if a Parallel Task field value indicates 'yes'."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in ("yes", "true", "required", "mandatory")
    return False


def _try_parse_pdf(entity_slug: str, pdf_url: str) -> dict | None:
    """Try to parse a PDF with Reducto using TTI_DISCLOSURE_SCHEMA."""
    try:
        import asyncio
        from app.services.reducto_client import parse_pdf, TTI_DISCLOSURE_SCHEMA, TTI_DISCLOSURE_PROMPT

        async def _parse():
            return await parse_pdf(pdf_url, schema=TTI_DISCLOSURE_SCHEMA)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, _parse())
                result = future.result(timeout=200)
        else:
            result = asyncio.run(_parse())

        if result and not result.get("error"):
            return result
    except Exception as e:
        logger.warning(f"Reducto PDF parse failed for {entity_slug} ({pdf_url}): {e}")

    return None


def _unwrap_citations(obj):
    """Recursively unwrap Reducto citation-wrapped values."""
    if isinstance(obj, dict):
        if "value" in obj and "citations" in obj:
            return _unwrap_citations(obj["value"])
        return {k: _unwrap_citations(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_unwrap_citations(item) for item in obj]
    return obj


# =============================================================================
# Collection orchestrator
# =============================================================================

def collect_entity_disclosures(entity_slug: str, entity_name: str) -> dict | None:
    """Collect disclosure data for a single TTI entity.

    Returns structured_data dict, or None if collection fails/skipped.
    """
    if _already_collected_today(entity_slug):
        logger.info(f"TTI disclosure already collected today for {entity_slug}")
        # Return latest extraction
        row = fetch_one("""
            SELECT structured_data FROM tti_disclosure_extractions
            WHERE entity_slug = %s ORDER BY extracted_at DESC LIMIT 1
        """, (entity_slug,))
        if row and row.get("structured_data"):
            sd = row["structured_data"]
            return json.loads(sd) if isinstance(sd, str) else sd
        return None

    sources = TTI_DISCLOSURE_URLS.get(entity_slug, [])
    if not sources:
        return None

    combined_data = {}

    for source in sources:
        url = source["url"]
        source_type = source["type"]

        logger.info(f"TTI disclosure scraping {entity_slug}: {url}")
        time.sleep(2)  # Rate limit between sources

        markdown = _try_scrape_page(url, entity_slug=entity_slug)
        if not markdown:
            continue

        # Extract structured data from markdown
        page_data = _extract_from_markdown(markdown)

        # Search for PDF links (attestation reports, prospectuses)
        pdf_urls = PDF_URL_PATTERN.findall(markdown)
        # Filter to likely attestation/disclosure PDFs
        attestation_pdfs = [
            u for u in pdf_urls
            if not any(skip in u.lower() for skip in
                       ["whitepaper", "press", "terms", "privacy", "cookie"])
        ]

        if attestation_pdfs:
            # Try Reducto on the most promising PDF
            best_pdf = attestation_pdfs[0]
            time.sleep(2)
            pdf_result = _try_parse_pdf(entity_slug, best_pdf)
            if pdf_result:
                result_data = pdf_result.get("result", pdf_result)
                unwrapped = _unwrap_citations(result_data)
                if isinstance(unwrapped, dict):
                    # PDF data is higher quality — merge over page data
                    page_data.update({k: v for k, v in unwrapped.items() if v is not None})

                    _store_extraction(
                        entity_slug, entity_name, best_pdf,
                        "attestation_pdf", unwrapped,
                        "reducto_pdf", 0.8,
                    )

        # Store page extraction
        if page_data:
            _store_extraction(
                entity_slug, entity_name, url,
                source_type, page_data,
                "parallel_extract", 0.7,  # Parallel Extract is primary scraper
            )
            combined_data.update(page_data)

    # If page scraping yielded sparse results (<5 fields), supplement with Parallel Task
    # deep research — it searches the web and synthesizes structured facts
    if len(combined_data) < 5:
        issuer = ""
        for slug, url_list in TTI_DISCLOSURE_URLS.items():
            if slug == entity_slug:
                break
        # Look up issuer name from entity config
        try:
            from app.index_definitions.tti_v01 import TTI_ENTITIES
            for ent in TTI_ENTITIES:
                if ent["slug"] == entity_slug:
                    issuer = ent.get("issuer", entity_name)
                    break
        except Exception:
            issuer = entity_name

        logger.info(
            f"TTI disclosure {entity_slug}: only {len(combined_data)} fields from pages, "
            f"supplementing with Parallel Task research"
        )
        task_data = _try_parallel_task_research(entity_slug, entity_name, issuer)
        if task_data:
            # Task data fills gaps — don't overwrite existing page extractions
            for k, v in task_data.items():
                if k not in combined_data:
                    combined_data[k] = v

            _store_extraction(
                entity_slug, entity_name,
                f"parallel_task://{entity_slug}",
                "deep_research", task_data,
                "parallel_task", 0.7,
            )

    if combined_data:
        logger.info(f"TTI disclosure {entity_slug}: extracted {len(combined_data)} fields")
    return combined_data if combined_data else None


# =============================================================================
# Component value mapping — extraction fields → raw_values for scoring
# =============================================================================

def map_disclosure_to_components(data: dict, static: dict) -> dict:
    """Map extracted disclosure fields to TTI raw_values.

    Returns dict of {component_id: score} for components that can be automated.
    Uses max(live, static) to prevent regression.
    """
    if not data:
        return {}

    components = {}

    # issuer_aum: log normalization in index def
    aum = data.get("issuer_aum_usd")
    if aum and isinstance(aum, (int, float)) and aum > 0:
        components["issuer_aum"] = aum  # raw value, engine normalizes

    # issuer_track_record: years → 0-95
    years = data.get("years_in_operation")
    if years and isinstance(years, (int, float)):
        if years >= 10:
            score = 95
        elif years >= 5:
            score = 80
        elif years >= 3:
            score = 60
        elif years >= 1:
            score = 30
        else:
            score = 10
        components["issuer_track_record"] = max(score, static.get("issuer_track_record", 0))

    # issuer_regulatory_status: registration count + type
    regs = data.get("regulatory_registrations", [])
    if regs:
        reg_score = 0
        for r in regs:
            r_lower = r.lower() if isinstance(r, str) else ""
            if "sec" in r_lower or "ria" in r_lower:
                reg_score += 30
            elif "finra" in r_lower:
                reg_score += 20
            elif "state" in r_lower:
                reg_score += 5
            else:
                reg_score += 10
        components["issuer_regulatory_status"] = max(
            min(100, reg_score), static.get("issuer_regulatory_status", 0)
        )

    # bank_partner_quality: tier lookup
    banks = data.get("banking_partners", [])
    if banks:
        best_tier = 40
        for bank in banks:
            for name, tier in BANK_TIERS.items():
                if name in bank.lower():
                    best_tier = max(best_tier, tier)
        components["bank_partner_quality"] = max(best_tier, static.get("bank_partner_quality", 0))

    # custodian_quality: tier lookup
    custodians = data.get("custodians", [])
    if custodians:
        best_tier = 30
        for custodian in custodians:
            for name, tier in CUSTODIAN_TIERS.items():
                if name in custodian.lower():
                    best_tier = max(best_tier, tier)
        components["custodian_quality"] = max(best_tier, static.get("custodian_quality", 0))

    # counterparty_count: raw value for log normalization
    cp_count = data.get("counterparties_count")
    if cp_count and isinstance(cp_count, (int, float)) and cp_count > 0:
        components["counterparty_count"] = int(cp_count)

    # key_person_risk: more officers = lower risk
    officers = data.get("named_officers", [])
    if officers:
        count = len(officers)
        if count >= 5:
            score = 85
        elif count >= 3:
            score = 60
        elif count >= 1:
            score = 30
        else:
            score = 20
        components["key_person_risk"] = max(score, static.get("key_person_risk", 0))

    # operational_continuity_tti: BCP disclosed
    if data.get("business_continuity_plan"):
        components["operational_continuity_tti"] = max(80, static.get("operational_continuity_tti", 0))

    # conflict_of_interest: disclosed
    if data.get("conflict_of_interest_disclosed"):
        components["conflict_of_interest"] = max(80, static.get("conflict_of_interest", 0))

    # securities_registration: registered + exemption type
    if data.get("securities_registered"):
        exemption = (data.get("exemption_type") or "").lower()
        if "reg a" in exemption or "registered" in exemption:
            score = 90
        elif "reg d" in exemption:
            score = 70
        elif "reg s" in exemption:
            score = 50
        else:
            score = 60
        components["securities_registration"] = max(score, static.get("securities_registration", 0))

    # kyc_aml_compliance
    if data.get("kyc_aml_required"):
        components["kyc_aml_compliance"] = max(85, static.get("kyc_aml_compliance", 0))

    # investor_accreditation
    if data.get("accredited_only") is not None:
        score = 75 if data["accredited_only"] else 50
        components["investor_accreditation"] = max(score, static.get("investor_accreditation", 0))

    # prospectus_availability
    if data.get("prospectus_available"):
        components["prospectus_availability"] = max(90, static.get("prospectus_availability", 0))

    # tax_reporting
    if data.get("tax_reporting_provided"):
        components["tax_reporting"] = max(80, static.get("tax_reporting", 0))

    # transfer_restrictions
    if data.get("transfer_restrictions_described"):
        components["transfer_restrictions"] = max(75, static.get("transfer_restrictions", 0))

    # regulatory_change_risk: from jurisdiction
    jurisdiction = (data.get("jurisdiction") or "").lower()
    if jurisdiction:
        score = JURISDICTION_RISK.get(jurisdiction, 50)
        components["regulatory_change_risk"] = max(score, static.get("regulatory_change_risk", 0))

    # jurisdiction_risk_tti: sovereign risk mapping
    if jurisdiction:
        score = JURISDICTION_SOVEREIGN.get(jurisdiction, 70)
        components["jurisdiction_risk_tti"] = max(score, static.get("jurisdiction_risk_tti", 0))

    # collateral_segregation (from reserve section)
    if data.get("collateral_segregation_disclosed"):
        components["collateral_segregation"] = max(85, static.get("collateral_segregation", 0))

    # rehypothecation_risk (higher = safer — no rehypothecation)
    if data.get("rehypothecation_prohibited"):
        components["rehypothecation_risk"] = max(90, static.get("rehypothecation_risk", 0))

    return components


# =============================================================================
# Entrypoint — called from worker slow cycle
# =============================================================================

def run_tti_disclosure_collection() -> dict:
    """Run TTI disclosure collection for all entities with configured URLs.

    Returns {entity_slug: extracted_component_count}.
    Daily-gated per entity.
    """
    results = {}
    for slug in TTI_DISCLOSURE_URLS:
        try:
            data = collect_entity_disclosures(slug, slug)
            if data:
                results[slug] = len(data)
            else:
                results[slug] = 0
        except Exception as e:
            logger.warning(f"TTI disclosure collection failed for {slug}: {e}")
            results[slug] = 0

    logger.info(f"TTI disclosure collection complete: {results}")
    return results
