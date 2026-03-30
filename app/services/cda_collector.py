"""
CDA Collection Pipeline — Adaptive Vendor Waterfall.

For each active issuer, tries collection methods in order of cost/speed:
  1. Parallel Extract (cheapest — static transparency pages)
  2. Parallel Search (finds PDFs not linked from transparency page)
  3. Firecrawl JS render + Framer module discovery (JS-heavy sites like Paxos)
  4. Firecrawl JSON extract (schema-based DOM extraction)
  5. Parallel Task deep research (most expensive — finds SEC filings, alt sources)

Crypto-backed / algorithmic assets skip the waterfall (on-chain only).
"""

import re
import json
import asyncio
import logging
from datetime import datetime, timezone, date

from app.database import fetch_all, fetch_one, execute
from app.services import parallel_client, reducto_client, firecrawl_client

logger = logging.getLogger(__name__)

PDF_URL_PATTERN = re.compile(r'https?://[^\s\)\"\'<>]+\.pdf', re.IGNORECASE)

ON_CHAIN_CATEGORIES = ("crypto-backed", "algorithmic")


# =============================================================================
# Database helpers
# =============================================================================

def _already_collected_today(asset_symbol: str, source_url: str) -> bool:
    """Check if we already have an extraction for this asset+url today."""
    row = fetch_one(
        """
        SELECT id FROM cda_vendor_extractions
        WHERE asset_symbol = %s AND source_url = %s
          AND extracted_at::date = %s
        LIMIT 1
        """,
        (asset_symbol, source_url, date.today().isoformat()),
    )
    return row is not None


def _has_pdf_extraction_today(asset_symbol: str) -> bool:
    """Check if we already have a PDF attestation extraction today."""
    row = fetch_one(
        """
        SELECT id FROM cda_vendor_extractions
        WHERE asset_symbol = %s AND source_type = 'pdf_attestation'
          AND extracted_at::date = %s
        LIMIT 1
        """,
        (asset_symbol, date.today().isoformat()),
    )
    return row is not None


def _store_extraction(
    asset_symbol: str,
    source_url: str,
    source_type: str,
    extraction_method: str,
    extraction_vendor: str,
    raw_response: dict,
    structured_data: dict,
    confidence_score: float,
    warnings: list = None,
):
    """Store a vendor extraction result."""
    execute(
        """
        INSERT INTO cda_vendor_extractions
            (asset_symbol, source_url, source_type, extraction_method,
             extraction_vendor, raw_response, structured_data,
             confidence_score, extraction_warnings)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            asset_symbol,
            source_url,
            source_type,
            extraction_method,
            extraction_vendor,
            json.dumps(raw_response, default=str),
            json.dumps(structured_data, default=str) if structured_data else None,
            confidence_score,
            warnings,
        ),
    )


def _update_registry(
    asset_symbol: str,
    success: bool,
    collection_method: str = None,
    failure_reason: str = None,
):
    """Update issuer registry after collection attempt."""
    if success:
        if collection_method:
            execute(
                """
                UPDATE cda_issuer_registry
                SET last_successful_collection = NOW(),
                    consecutive_failures = 0,
                    collection_method = %s,
                    last_failure_reason = NULL,
                    updated_at = NOW()
                WHERE asset_symbol = %s
                """,
                (collection_method, asset_symbol),
            )
        else:
            execute(
                """
                UPDATE cda_issuer_registry
                SET last_successful_collection = NOW(),
                    consecutive_failures = 0,
                    last_failure_reason = NULL,
                    updated_at = NOW()
                WHERE asset_symbol = %s
                """,
                (asset_symbol,),
            )
    else:
        execute(
            """
            UPDATE cda_issuer_registry
            SET consecutive_failures = consecutive_failures + 1,
                last_failure_reason = %s, updated_at = NOW()
            WHERE asset_symbol = %s
            """,
            (failure_reason or "all_methods_exhausted", asset_symbol),
        )


# Legacy aliases used by other modules (server.py webhooks, etc.)
def _update_registry_success(asset_symbol: str):
    _update_registry(asset_symbol, success=True)


def _update_registry_failure(asset_symbol: str, reason: str):
    _update_registry(asset_symbol, success=False, failure_reason=reason)


# =============================================================================
# PDF extraction helpers
# =============================================================================

def _extract_pdf_urls(content: str) -> list[str]:
    """Find PDF URLs in markdown content."""
    if not content:
        return []
    urls = PDF_URL_PATTERN.findall(content)
    seen = set()
    unique = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique


def _filter_attestation_pdfs(pdf_urls: list[str]) -> list[str]:
    """Filter out PDFs that are clearly not attestation/reserve reports."""
    skip_terms = [
        "whitepaper", "white-paper", "white_paper", "white%20paper",
        "press-release", "pressrelease",
        "terms-of-service", "privacy-policy",
        "annual-report",
        "sok:", "arxiv",
    ]
    return [
        url for url in pdf_urls
        if not any(term in url.lower() for term in skip_terms)
    ]


def _pick_most_recent_pdf(pdf_urls: list[str]) -> str | None:
    """Pick the most recent PDF by looking for year/date patterns in URLs."""
    if not pdf_urls:
        return None

    pdf_urls = _filter_attestation_pdfs(pdf_urls)
    if not pdf_urls:
        return None
    if len(pdf_urls) == 1:
        return pdf_urls[0]

    attestation_terms = [
        "attestation", "reserves", "isae", "assurance",
        "examination", "reserve", "crr",
    ]

    def relevance_key(url):
        lower = url.lower()
        relevance = sum(1 for t in attestation_terms if t in lower)
        years = re.findall(r'20\d{2}', url)
        months = re.findall(
            r'(?:january|february|march|april|may|june|july|august|'
            r'september|october|november|december)', lower
        )
        date_matches = re.findall(r'(\d{2})[.-](\d{2})[.-](20\d{2})', url)
        month_num = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        y = int(years[-1]) if years else 0
        m = month_num.get(months[0], 0) if months else 0
        if date_matches and not m:
            dd, mm, yy = date_matches[-1]
            y = max(y, int(yy))
            m = max(m, int(mm))
        is_relevant = 1 if relevance > 0 else 0
        return (y, m, is_relevant, relevance)

    sorted_urls = sorted(pdf_urls, key=relevance_key, reverse=True)
    return sorted_urls[0]


def _extract_structured_from_reducto(result: dict) -> tuple[dict | None, float]:
    """Extract structured data and confidence from Reducto response."""
    raw_result = result.get("result", {})
    if not raw_result:
        return None, 0.0

    structured = {}
    confidences = []

    for key, val in raw_result.items():
        if isinstance(val, dict) and "value" in val:
            structured[key] = val["value"]
            for cit in val.get("citations", []):
                gc = cit.get("granular_confidence", {})
                ec = gc.get("extract_confidence")
                pc = gc.get("parse_confidence")
                if ec is not None and pc is not None:
                    confidences.append(max(ec, pc))
                elif ec is not None:
                    confidences.append(ec)
                elif pc is not None:
                    confidences.append(pc)
        else:
            structured[key] = val

    avg_confidence = sum(confidences) / len(confidences) if confidences else 0.5
    return structured, avg_confidence


def _build_search_queries(symbol: str, issuer_name: str) -> list[str]:
    """Build issuer-specific search queries, then generic fallbacks."""
    specific = {
        "USDT": [
            "BDO Tether consolidated reserves report 2025 2026 PDF",
            "Tether ISAE 3000R reserves report BDO PDF",
        ],
        "PYUSD": [
            "KPMG PYUSD reserve attestation report PDF 2025 2026",
            "Paxos PYUSD monthly attestation KPMG PDF",
        ],
        "TUSD": [
            "TrueUSD TUSD attestation proof of reserves PDF",
            "Archblock TUSD reserve report PDF",
        ],
        "USD1": [
            "World Liberty Financial USD1 reserve report PDF",
        ],
        "USDP": [
            "Paxos USDP Pax Dollar attestation report PDF",
        ],
    }

    queries = specific.get(symbol, [])
    queries.extend([
        f"{issuer_name} {symbol} reserve attestation report 2026 filetype:pdf",
        f"{issuer_name} {symbol} proof of reserves 2025 2026",
        f"{issuer_name} transparency attestation PDF",
    ])
    return queries


def _extract_reserve_data_from_markdown(markdown: str) -> dict | None:
    """
    Try to parse reserve data directly from rendered page markdown.
    Returns dict with BRSS-compatible fields, or None if nothing useful found.
    """
    if not markdown or len(markdown) < 100:
        return None

    data = {}

    # Total reserves patterns
    reserve_match = re.search(
        r'(?:total\s+)?reserves?\s*[:=]?\s*\$?([\d,]+(?:\.\d+)?)\s*(?:billion|million|B|M)?',
        markdown, re.IGNORECASE,
    )
    if reserve_match:
        val = reserve_match.group(1).replace(",", "")
        try:
            num = float(val)
            text_after = markdown[reserve_match.end():reserve_match.end() + 20].lower()
            if "billion" in text_after or num < 1000:
                num *= 1_000_000_000
            elif "million" in text_after:
                num *= 1_000_000
            data["total_reserves_usd"] = num
        except ValueError:
            pass

    # Total supply patterns
    supply_match = re.search(
        r'(?:total\s+)?(?:supply|circulation|outstanding)\s*[:=]?\s*\$?([\d,]+(?:\.\d+)?)',
        markdown, re.IGNORECASE,
    )
    if supply_match:
        val = supply_match.group(1).replace(",", "")
        try:
            data["total_supply"] = float(val)
        except ValueError:
            pass

    # Attestation date patterns
    date_match = re.search(
        r'(?:as\s+of|dated?|report\s+date)\s*[:=]?\s*'
        r'((?:January|February|March|April|May|June|July|August|'
        r'September|October|November|December)\s+\d{1,2},?\s+\d{4})',
        markdown, re.IGNORECASE,
    )
    if date_match:
        data["attestation_date"] = date_match.group(1)

    # Auditor patterns
    auditor_match = re.search(
        r'(?:audit(?:ed|or)|attested|examined|issued)\s+by\s+([A-Z][A-Za-z\s&,]+?)(?:\.|,|\n)',
        markdown, re.IGNORECASE,
    )
    if auditor_match:
        data["auditor_name"] = auditor_match.group(1).strip()

    return data if data.get("total_reserves_usd") else None


# =============================================================================
# Reducto PDF parsing helper
# =============================================================================

async def _try_reducto_pdf(symbol: str, pdf_url: str, prefix: str, disclosure_type: str = None) -> tuple[bool, str]:
    """
    Try to parse a PDF with Reducto and store the result.
    Returns (success: bool, method_label: str).
    """
    if _already_collected_today(symbol, pdf_url):
        logger.info(f"{prefix} — PDF already parsed today: {pdf_url[:60]}...")
        return True, "already_collected"

    await asyncio.sleep(2)
    logger.info(f"{prefix} — Reducto parsing: {pdf_url[:80]}...")
    reducto_result = await reducto_client.parse_pdf(pdf_url, disclosure_type=disclosure_type)

    if "error" in reducto_result:
        logger.warning(f"{prefix} — Reducto failed: {reducto_result['error']}")
        return False, f"reducto_error: {reducto_result['error']}"

    structured, confidence = _extract_structured_from_reducto(reducto_result)
    if confidence < 0.3:
        logger.warning(f"{prefix} — Reducto confidence too low: {confidence:.2f}")
        return False, f"low_confidence: {confidence:.2f}"

    logger.info(f"{prefix} — Reducto OK (confidence: {confidence:.2f})")

    _store_extraction(
        asset_symbol=symbol,
        source_url=pdf_url,
        source_type="pdf_attestation",
        extraction_method="reducto_pdf",
        extraction_vendor="reducto",
        raw_response=reducto_result,
        structured_data=structured,
        confidence_score=confidence,
    )

    # Run validation
    try:
        from app.services.cda_validator import validate_extraction
        last_ext = fetch_one(
            "SELECT id FROM cda_vendor_extractions WHERE asset_symbol = %s ORDER BY extracted_at DESC LIMIT 1",
            (symbol,)
        )
        if last_ext and structured:
            validate_extraction(last_ext["id"], symbol, structured, disclosure_type or "fiat-reserve")
    except Exception as ve:
        logger.warning(f"{prefix} — validation error: {ve}")

    return True, "reducto_pdf"


# =============================================================================
# Waterfall steps
# =============================================================================

async def _step_parallel_extract(issuer: dict, prefix: str) -> dict | None:
    """Step 1: Parallel Extract — fast page scrape for PDF links and inline data."""
    url = issuer.get("transparency_url")
    if not url:
        return None

    symbol = issuer["asset_symbol"]

    if _already_collected_today(symbol, url):
        logger.info(f"{prefix} — [Step 1] page already collected today")
        return None  # Already tried today, let waterfall continue

    logger.info(f"{prefix} — [Step 1] Parallel Extract: {url}")
    parallel_result = await parallel_client.extract(
        url,
        objective="Find reserve attestation data, PDF report links, total reserves, auditor"
    )

    if "error" in parallel_result:
        return None

    # Extract content
    results_list = parallel_result.get("results", [])
    full_content = ""
    excerpts = []
    if results_list:
        r = results_list[0]
        full_content = r.get("full_content", "") or ""
        excerpts = r.get("excerpts", []) or []

    # Store the web extraction
    _store_extraction(
        asset_symbol=symbol,
        source_url=url,
        source_type="transparency_page",
        extraction_method="parallel_extract",
        extraction_vendor="parallel",
        raw_response=parallel_result,
        structured_data={
            "title": results_list[0].get("title") if results_list else None,
            "content_length": len(full_content),
            "excerpt_count": len(excerpts),
            "excerpts": excerpts[:3],
        },
        confidence_score=0.7,
    )
    logger.info(f"{prefix} — [Step 1] Parallel OK ({len(full_content)} chars)")

    # Find PDF URLs in extracted content
    pdf_urls = _extract_pdf_urls(full_content)
    if pdf_urls:
        pdf_urls = _filter_attestation_pdfs(pdf_urls)
        best = _pick_most_recent_pdf(pdf_urls)
        if best:
            ok, method = await _try_reducto_pdf(symbol, best, prefix, disclosure_type=issuer.get("disclosure_type"))
            if ok:
                return {"status": "success", "method": "parallel_extract+reducto"}

    # Try extracting data directly from page markdown
    page_data = _extract_reserve_data_from_markdown(full_content)
    if page_data:
        _store_extraction(
            asset_symbol=symbol,
            source_url=url,
            source_type="transparency_page",
            extraction_method="parallel_extract",
            extraction_vendor="parallel",
            raw_response={},
            structured_data=page_data,
            confidence_score=0.7,
        )
        return {"status": "success", "method": "parallel_extract_direct"}

    return None


async def _step_parallel_search(issuer: dict, prefix: str) -> dict | None:
    """Step 2: Parallel Search — find PDFs via web search."""
    symbol = issuer["asset_symbol"]
    issuer_name = issuer.get("issuer_name", symbol)

    logger.info(f"{prefix} — [Step 2] Parallel Search")

    queries = _build_search_queries(symbol, issuer_name)
    pdf_urls = []
    secondary_pages = []

    for query in queries:
        await asyncio.sleep(2)
        search_result = await parallel_client.search(query, num_results=10)
        if "error" in search_result:
            continue

        for r in search_result.get("results", []):
            url = r.get("url", "")
            if url.lower().endswith(".pdf"):
                pdf_urls.append(url)
            else:
                snippet = (r.get("excerpt", "") or "") + (r.get("text", "") or "")
                found = PDF_URL_PATTERN.findall(snippet)
                pdf_urls.extend(found)

                if any(term in url.lower() for term in
                       ("transparency", "attestation", "reserves", "proof", "assurance")):
                    if url not in secondary_pages and url != issuer.get("transparency_url"):
                        secondary_pages.append(url)

        if pdf_urls:
            break

    pdf_urls = list(dict.fromkeys(pdf_urls))

    # If no PDFs, try Extract on promising secondary pages
    if not pdf_urls and secondary_pages:
        for page_url in secondary_pages[:2]:
            await asyncio.sleep(2)
            logger.info(f"{prefix} — [Step 2] secondary Extract: {page_url[:80]}...")
            secondary = await parallel_client.extract(
                page_url,
                objective="Find PDF attestation report links, reserve report downloads"
            )
            if "error" not in secondary:
                for sr in secondary.get("results", []):
                    content = (sr.get("full_content", "") or "") + (sr.get("excerpts", "") or "")
                    if isinstance(sr.get("excerpts"), list):
                        content += " ".join(sr["excerpts"])
                    found = PDF_URL_PATTERN.findall(content)
                    pdf_urls.extend(found)
            if pdf_urls:
                break

    pdf_urls = list(dict.fromkeys(pdf_urls))

    if not pdf_urls:
        return None

    logger.info(f"{prefix} — [Step 2] found {len(pdf_urls)} PDF(s)")

    best = _pick_most_recent_pdf(pdf_urls)
    if best:
        ok, method = await _try_reducto_pdf(symbol, best, prefix, disclosure_type=issuer.get("disclosure_type"))
        if ok:
            return {"status": "success", "method": "parallel_search+reducto"}

    return None


async def _step_firecrawl_js(issuer: dict, prefix: str) -> dict | None:
    """Step 3: Firecrawl JS render + Framer module PDF discovery."""
    url = issuer.get("transparency_url")
    if not url:
        return None

    symbol = issuer["asset_symbol"]

    logger.info(f"{prefix} — [Step 3] Firecrawl JS render")

    try:
        # 3a: Simple render — check for PDF links in rendered DOM
        result = firecrawl_client.scrape_js_page(url, wait_ms=8000)

        markdown = getattr(result, "markdown", "") or ""
        links = getattr(result, "links", []) or []

        pdf_links = [l for l in links if ".pdf" in str(l).lower()]
        pdf_links = _filter_attestation_pdfs(pdf_links)

        if pdf_links:
            best = _pick_most_recent_pdf(pdf_links)
            if best:
                ok, method = await _try_reducto_pdf(symbol, best, prefix, disclosure_type=issuer.get("disclosure_type"))
                if ok:
                    return {"status": "success", "method": "firecrawl_js+reducto"}

        # 3b: Try extracting reserve data from rendered markdown
        page_data = _extract_reserve_data_from_markdown(markdown)
        if page_data:
            _store_extraction(
                asset_symbol=symbol,
                source_url=url,
                source_type="transparency_page",
                extraction_method="firecrawl_js",
                extraction_vendor="firecrawl",
                raw_response={},
                structured_data=page_data,
                confidence_score=0.75,
            )
            return {"status": "success", "method": "firecrawl_js_direct"}

        # 3c: Framer module trick — discover PDFs embedded in JS components
        logger.info(f"{prefix} — [Step 3c] Framer module extraction")
        framer_pdfs = firecrawl_client.discover_framer_pdfs(url)
        if framer_pdfs:
            best = firecrawl_client.identify_most_recent_attestation(framer_pdfs)
            if best:
                ok, method = await _try_reducto_pdf(symbol, best, prefix, disclosure_type=issuer.get("disclosure_type"))
                if ok:
                    return {"status": "success", "method": "firecrawl_framer+reducto"}

    except Exception as e:
        logger.warning(f"{prefix} — [Step 3] Firecrawl error: {e}")

    return None


async def _step_firecrawl_json(issuer: dict, prefix: str) -> dict | None:
    """Step 4: Firecrawl JSON extraction with type-aware schema."""
    url = issuer.get("transparency_url")
    if not url:
        return None

    symbol = issuer["asset_symbol"]

    logger.info(f"{prefix} — [Step 4] Firecrawl JSON extract")

    try:
        from app.services.reducto_client import get_schema_for_type
        disc_type = issuer.get("disclosure_type", "fiat-reserve")
        schema, _ = get_schema_for_type(disc_type)

        client = firecrawl_client.get_client()

        result = client.scrape(
            url,
            formats=["extract"],
            extract={"schema": schema},
            actions=[{"type": "wait", "milliseconds": 8000}],
        )

        extract_data = getattr(result, "extract", None)
        if not extract_data:
            return None

        # Type-agnostic success check: any non-empty extracted field counts
        if isinstance(extract_data, dict) and any(
            v for v in extract_data.values() if v is not None and v != "" and v != 0
        ):
            _store_extraction(
                asset_symbol=symbol,
                source_url=url,
                source_type="transparency_page",
                extraction_method="firecrawl_json",
                extraction_vendor="firecrawl",
                raw_response={},
                structured_data=extract_data,
                confidence_score=0.80,
            )

            # Run validation
            try:
                from app.services.cda_validator import validate_extraction
                last_ext = fetch_one(
                    "SELECT id FROM cda_vendor_extractions WHERE asset_symbol = %s ORDER BY extracted_at DESC LIMIT 1",
                    (symbol,)
                )
                if last_ext and extract_data:
                    validate_extraction(last_ext["id"], symbol, extract_data, disc_type)
            except Exception as ve:
                logger.warning(f"{prefix} — validation error: {ve}")

            return {"status": "success", "method": "firecrawl_json"}

    except Exception as e:
        logger.warning(f"{prefix} — [Step 4] Firecrawl JSON error: {e}")

    return None


async def _step_parallel_task(issuer: dict, prefix: str) -> dict | None:
    """Step 5: Parallel Task deep research — most expensive, last resort."""
    symbol = issuer["asset_symbol"]
    issuer_name = issuer.get("issuer_name", symbol)

    logger.info(f"{prefix} — [Step 5] Parallel Task deep research")

    disc_type = issuer.get("disclosure_type", "fiat-reserve")
    if disc_type == "synthetic-derivative":
        research_q = (
            f"Research the synthetic stablecoin {symbol} issued by {issuer_name}. "
            f"Find: latest custodian attestation report URL (PDF), report date, "
            f"total backing assets, custodian names, open interest, collateral ratio."
        )
        research_fields = {
            "pdf_url": "Direct URL to most recent custodian attestation PDF",
            "attestation_date": "Date of most recent report",
            "total_backing_usd": "Total backing assets in USD (number)",
            "custodians": "Names of custodians holding assets",
            "collateral_ratio": "Ratio of backing assets to supply",
        }
    elif disc_type == "rwa-tokenized":
        research_q = (
            f"Research the tokenized asset {symbol} issued by {issuer_name}. "
            f"Find: latest NAV report URL (PDF), report date, NAV per token, "
            f"total AUM, underlying holdings, yield rate."
        )
        research_fields = {
            "pdf_url": "Direct URL to most recent NAV report PDF",
            "attestation_date": "Date of most recent report",
            "nav_per_token": "Net asset value per token",
            "total_assets_usd": "Total assets under management (number)",
            "yield_rate": "Current yield or APY",
        }
    else:
        research_q = (
            f"Research the stablecoin {symbol} issued by {issuer_name}. "
            f"Find: latest attestation report URL (PDF), attestation date, "
            f"total reserves, reserve composition, auditor name."
        )
        research_fields = {
            "pdf_url": "Direct URL to most recent attestation PDF",
            "attestation_date": "Date of most recent attestation",
            "total_reserves_usd": "Total reserves in USD (number)",
            "auditor_name": "Auditing/attestation firm name",
            "reserve_description": "Description of reserve composition",
        }

    task_result = await parallel_client.task(
        question=research_q,
        fields=research_fields,
        processor="core",
    )

    if "error" in task_result:
        return None

    fields = task_result.get("fields", task_result.get("data", task_result))

    # If we got a PDF URL, try Reducto
    pdf_url = fields.get("pdf_url")
    if pdf_url and pdf_url.lower().endswith(".pdf"):
        ok, method = await _try_reducto_pdf(symbol, pdf_url, prefix, disclosure_type=issuer.get("disclosure_type"))
        if ok:
            return {"status": "success", "method": "parallel_task+reducto"}

    # Store whatever research data we got
    total_reserves = fields.get("total_reserves_usd")
    attestation_date = fields.get("attestation_date")

    if total_reserves or attestation_date:
        structured = {
            "attestation_date": attestation_date,
            "auditor_name": fields.get("auditor_name"),
            "reserve_description": fields.get("reserve_description"),
        }
        if total_reserves:
            try:
                structured["total_reserves_usd"] = float(
                    str(total_reserves).replace(",", "").replace("$", "")
                )
            except ValueError:
                pass

        _store_extraction(
            asset_symbol=symbol,
            source_url="research",
            source_type="research",
            extraction_method="parallel_task",
            extraction_vendor="parallel",
            raw_response=task_result,
            structured_data=structured,
            confidence_score=0.6,
            warnings=["Data from deep research, not direct PDF extraction"],
        )
        return {"status": "partial", "method": "parallel_task_research"}

    return None


# =============================================================================
# Adaptive collector — the main waterfall
# =============================================================================

WATERFALL_STEPS = [
    ("parallel_extract", _step_parallel_extract),
    ("parallel_search", _step_parallel_search),
    ("firecrawl_js", _step_firecrawl_js),
    ("firecrawl_json", _step_firecrawl_json),
    ("parallel_task", _step_parallel_task),
]

# Map collection_method to the waterfall step index to start from.
# If a method worked before with 0 consecutive failures, start there.
FAST_PATH_INDEX = {
    "web_extract": 0,       # Start at Parallel Extract
    "parallel_search": 1,
    "firecrawl_js": 2,      # Start at Firecrawl JS
    "firecrawl_json": 3,
    "parallel_task": 4,
    "multi_source": 0,      # Multi-source doesn't use the single-URL waterfall
}


async def collect_issuer_adaptive(issuer: dict, prefix: str) -> dict:
    """
    Adaptive collection — if source_urls exist, iterate all sources.
    Otherwise fall back to single-URL waterfall (legacy path).
    """
    symbol = issuer["asset_symbol"]
    category = issuer.get("asset_category", "unknown")

    if category in ON_CHAIN_CATEGORIES:
        logger.info(f"{prefix} — skipped (on-chain only: {category})")
        return {"status": "on_chain_only", "method": None}

    if _has_pdf_extraction_today(symbol):
        logger.info(f"{prefix} — skipped (PDF already extracted today)")
        return {"status": "already_collected", "method": None}

    # Multi-source path: iterate source_urls if populated
    source_urls = issuer.get("source_urls")
    if source_urls and isinstance(source_urls, list) and len(source_urls) > 0:
        return await _collect_multi_source(issuer, source_urls, prefix)

    # Legacy single-URL waterfall (fiat-reserve issuers without source_urls)
    return await _collect_single_url_waterfall(issuer, prefix)


async def _collect_multi_source(issuer: dict, source_urls: list, prefix: str) -> dict:
    """
    Iterate all source_urls for an issuer. Each source has a type that determines
    the collection strategy. Collects from ALL sources, not stop-at-first.
    """
    symbol = issuer["asset_symbol"]
    disc_type = issuer.get("disclosure_type", "fiat-reserve")
    results = []
    any_success = False

    for i, source in enumerate(source_urls):
        src_url = source.get("url")
        src_type = source.get("type", "attestation_page")

        if not src_url:
            continue

        if _already_collected_today(symbol, src_url):
            logger.info(f"{prefix} — [Source {i+1}/{len(source_urls)}] {src_type} already collected today")
            results.append({"source": src_type, "status": "already_collected"})
            any_success = True
            continue

        logger.info(f"{prefix} — [Source {i+1}/{len(source_urls)}] {src_type}: {src_url[:80]}...")

        try:
            result = await _collect_from_source(issuer, src_url, src_type, disc_type, prefix)
            if result:
                results.append({"source": src_type, "status": "success", "method": result.get("method")})
                any_success = True
            else:
                results.append({"source": src_type, "status": "no_data"})
        except Exception as e:
            logger.warning(f"{prefix} — [Source {i+1}] {src_type} error: {e}")
            results.append({"source": src_type, "status": "error", "error": str(e)[:100]})

        await asyncio.sleep(2)

    if any_success:
        _update_registry(symbol, success=True, collection_method="multi_source")
        methods = [r.get("method", r["source"]) for r in results if r["status"] == "success"]
        return {"status": "success", "method": "+".join(methods) if methods else "multi_source", "sources": results}
    else:
        _update_registry(symbol, success=False, failure_reason="all_sources_failed")
        return {"status": "failed", "method": None, "sources": results}


async def _collect_from_source(issuer: dict, url: str, source_type: str, disc_type: str, prefix: str) -> dict | None:
    """
    Collect from a single source URL using the appropriate strategy for its type.
    """
    symbol = issuer["asset_symbol"]

    if source_type == "dashboard":
        return await _collect_dashboard(symbol, url, disc_type, prefix)

    elif source_type == "attestation_page":
        return await _collect_attestation_page(issuer, url, prefix)

    elif source_type == "pdf_direct":
        ok, method = await _try_reducto_pdf(symbol, url, prefix, disclosure_type=disc_type)
        if ok:
            return {"status": "success", "method": "pdf_direct+reducto"}
        return None

    elif source_type == "api":
        return await _collect_api_source(symbol, url, prefix)

    elif source_type == "docs_page":
        return await _collect_attestation_page(issuer, url, prefix)

    else:
        logger.info(f"{prefix} — unknown source type '{source_type}', trying parallel extract")
        return await _step_parallel_extract(
            {**issuer, "transparency_url": url}, prefix
        )


async def _collect_dashboard(symbol: str, url: str, disc_type: str, prefix: str) -> dict | None:
    """
    Collect from a JS-rendered transparency dashboard.
    Uses Firecrawl to render the page, then extracts data using type-specific schema.
    """
    logger.info(f"{prefix} — dashboard collection: {url[:80]}")

    try:
        # Step 1: Firecrawl JS render to get the page content
        result = firecrawl_client.scrape_js_page(url, wait_ms=10000)
        markdown = getattr(result, "markdown", "") or ""

        if not markdown or len(markdown) < 200:
            logger.warning(f"{prefix} — dashboard: insufficient content ({len(markdown)} chars)")
            return None

        # Step 2: Try Firecrawl JSON extraction with type-specific schema
        from app.services.reducto_client import get_schema_for_type
        schema, system_prompt = get_schema_for_type(disc_type)

        client = firecrawl_client.get_client()
        extract_result = client.scrape(
            url,
            formats=["extract"],
            extract={"schema": schema},
            actions=[{"type": "wait", "milliseconds": 10000}],
        )

        extract_data = getattr(extract_result, "extract", None)

        if isinstance(extract_data, dict) and any(
            v for v in extract_data.values() if v is not None and v != "" and v != 0
        ):
            _store_extraction(
                asset_symbol=symbol,
                source_url=url,
                source_type="dashboard",
                extraction_method="firecrawl_dashboard",
                extraction_vendor="firecrawl",
                raw_response={},
                structured_data=extract_data,
                confidence_score=0.80,
            )

            # Run validation
            try:
                from app.services.cda_validator import validate_extraction
                last_ext = fetch_one(
                    "SELECT id FROM cda_vendor_extractions WHERE asset_symbol = %s ORDER BY extracted_at DESC LIMIT 1",
                    (symbol,)
                )
                if last_ext:
                    validate_extraction(last_ext["id"], symbol, extract_data, disc_type)
            except Exception as ve:
                logger.warning(f"{prefix} — dashboard validation error: {ve}")

            return {"status": "success", "method": "firecrawl_dashboard"}

        # Step 3: Fall back to regex extraction from rendered markdown
        page_data = _extract_reserve_data_from_markdown(markdown)
        if page_data:
            _store_extraction(
                asset_symbol=symbol,
                source_url=url,
                source_type="dashboard",
                extraction_method="firecrawl_js_direct",
                extraction_vendor="firecrawl",
                raw_response={},
                structured_data=page_data,
                confidence_score=0.65,
            )
            return {"status": "success", "method": "firecrawl_js_markdown"}

    except Exception as e:
        logger.warning(f"{prefix} — dashboard error: {e}")

    return None


async def _collect_attestation_page(issuer: dict, url: str, prefix: str) -> dict | None:
    """
    Collect from a page that links to PDF attestation reports.
    Scrapes the page for PDF links, then parses the best one with Reducto.
    """
    symbol = issuer["asset_symbol"]
    disc_type = issuer.get("disclosure_type", "fiat-reserve")

    logger.info(f"{prefix} — attestation page: {url[:80]}")

    # Try Parallel Extract to find PDF links
    parallel_result = await parallel_client.extract(
        url,
        objective="Find PDF download links for attestation reports, custody reports, or reserve reports"
    )

    if "error" in parallel_result:
        # Fall back to Firecrawl JS render for JS-heavy pages
        try:
            result = firecrawl_client.scrape_js_page(url, wait_ms=8000)
            links = getattr(result, "links", []) or []
            pdf_links = [l for l in links if ".pdf" in str(l).lower()]
            pdf_links = _filter_attestation_pdfs(pdf_links)
            if pdf_links:
                best = _pick_most_recent_pdf(pdf_links)
                if best:
                    ok, method = await _try_reducto_pdf(symbol, best, prefix, disclosure_type=disc_type)
                    if ok:
                        return {"status": "success", "method": "firecrawl_js+reducto"}
        except Exception as e:
            logger.warning(f"{prefix} — attestation page firecrawl fallback error: {e}")
        return None

    # Extract PDF links from page content
    results_list = parallel_result.get("results", [])
    full_content = ""
    if results_list:
        full_content = results_list[0].get("full_content", "") or ""

    # Store page scrape
    _store_extraction(
        asset_symbol=symbol,
        source_url=url,
        source_type="attestation_page",
        extraction_method="parallel_extract",
        extraction_vendor="parallel",
        raw_response=parallel_result,
        structured_data={
            "content_length": len(full_content),
        },
        confidence_score=0.5,
    )

    pdf_urls = _extract_pdf_urls(full_content)
    if pdf_urls:
        pdf_urls = _filter_attestation_pdfs(pdf_urls)
        best = _pick_most_recent_pdf(pdf_urls)
        if best:
            ok, method = await _try_reducto_pdf(symbol, best, prefix, disclosure_type=disc_type)
            if ok:
                return {"status": "success", "method": "parallel_extract+reducto"}

    return None


async def _collect_api_source(symbol: str, url: str, prefix: str) -> dict | None:
    """
    Collect from a public JSON API endpoint.
    Stores the response directly as structured data.
    """
    import httpx

    logger.info(f"{prefix} — API source: {url[:80]}")

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

            if data:
                _store_extraction(
                    asset_symbol=symbol,
                    source_url=url,
                    source_type="api",
                    extraction_method="direct_api",
                    extraction_vendor="issuer",
                    raw_response=data,
                    structured_data=data,
                    confidence_score=0.90,
                )
                return {"status": "success", "method": "direct_api"}
    except Exception as e:
        logger.warning(f"{prefix} — API source error: {e}")

    return None


async def _collect_single_url_waterfall(issuer: dict, prefix: str) -> dict:
    """
    Legacy single-URL waterfall for issuers without source_urls.
    This is the original collect_issuer_adaptive logic.
    """
    symbol = issuer["asset_symbol"]
    attempts = []

    method = issuer.get("collection_method", "web_extract")
    failures = issuer.get("consecutive_failures", 0)
    fast_start = FAST_PATH_INDEX.get(method, 0)

    if failures == 0 and fast_start > 0:
        step_order = [WATERFALL_STEPS[fast_start]] + [
            s for i, s in enumerate(WATERFALL_STEPS) if i != fast_start
        ]
    else:
        step_order = list(WATERFALL_STEPS)

    for step_name, step_fn in step_order:
        try:
            result = await step_fn(issuer, prefix)
            if result:
                method_to_store = step_name
                if step_name == "parallel_extract":
                    method_to_store = "web_extract"
                elif step_name == "parallel_search":
                    method_to_store = "web_extract"

                _update_registry(symbol, success=True, collection_method=method_to_store)
                result["step"] = step_name
                logger.info(f"{prefix} — SUCCESS via {result['method']} (step: {step_name})")
                return result
            attempts.append(f"{step_name}: no data")
        except Exception as e:
            attempts.append(f"{step_name}: {str(e)[:100]}")
            logger.warning(f"{prefix} — [Step {step_name}] error: {e}")

        await asyncio.sleep(1)

    _update_registry(
        symbol,
        success=False,
        failure_reason="; ".join(attempts[-3:]),
    )
    logger.warning(f"{prefix} — ALL STEPS FAILED")
    for a in attempts:
        logger.warning(f"{prefix}   - {a}")

    return {"status": "failed", "method": None, "attempts": attempts}


# =============================================================================
# Public API — collection entrypoints
# =============================================================================

async def collect_issuer(issuer: dict, index: int, total: int) -> bool:
    """Collect CDA data for a single issuer. Returns True on success."""
    prefix = f"[{index}/{total}] {issuer['asset_symbol']}"
    result = await collect_issuer_adaptive(issuer, prefix)
    return result["status"] in ("success", "partial", "on_chain_only", "already_collected")


async def run_collection():
    """Run the full CDA collection pipeline across all active issuers."""
    logger.info("=== CDA Collection Pipeline Starting (Adaptive Waterfall) ===")

    issuers = fetch_all(
        """
        SELECT asset_symbol, issuer_name, transparency_url,
               collection_method, asset_category, consecutive_failures,
               disclosure_type, expected_fields, verification_rules, source_urls
        FROM cda_issuer_registry
        WHERE is_active = TRUE
        ORDER BY asset_symbol
        """
    )

    if not issuers:
        logger.warning("No active issuers in registry")
        return

    total = len(issuers)
    logger.info(f"Processing {total} issuers")

    results = {"success": 0, "partial": 0, "failed": 0, "skipped": 0}

    for i, issuer in enumerate(issuers, 1):
        try:
            result = await collect_issuer_adaptive(
                issuer, f"[{i}/{total}] {issuer['asset_symbol']}"
            )
            status = result.get("status", "failed")
            method = result.get("method", "none")

            if status in ("success", "partial"):
                results["success" if status == "success" else "partial"] += 1
            elif status in ("on_chain_only", "already_collected"):
                results["skipped"] += 1
            else:
                results["failed"] += 1

            logger.info(
                f"[{i}/{total}] {issuer['asset_symbol']} -> "
                f"{status} via {method}"
            )
        except Exception as e:
            logger.error(
                f"[{i}/{total}] {issuer['asset_symbol']} — unhandled error: {e}"
            )
            results["failed"] += 1

        # Rate limit between issuers
        if i < total:
            await asyncio.sleep(2)

    logger.info(
        f"=== CDA Collection Complete: "
        f"{results['success']} success, {results['partial']} partial, "
        f"{results['failed']} failed, {results['skipped']} skipped "
        f"out of {total} ==="
    )


async def collect_single_issuer(asset_symbol: str):
    """Run collection for a single issuer by symbol. Used by monitor webhooks."""
    issuer = fetch_one(
        """
        SELECT asset_symbol, issuer_name, transparency_url,
               collection_method, asset_category, consecutive_failures,
               disclosure_type, expected_fields, verification_rules, source_urls
        FROM cda_issuer_registry
        WHERE asset_symbol = %s AND is_active = TRUE
        """,
        (asset_symbol.upper(),),
    )
    if not issuer:
        logger.warning(f"collect_single_issuer: {asset_symbol} not found in registry")
        return False
    result = await collect_issuer_adaptive(issuer, f"[1/1] {asset_symbol}")
    return result["status"] in ("success", "partial")


# =============================================================================
# Monitor + Discovery (unchanged from original)
# =============================================================================

async def setup_monitors():
    """
    Create Parallel Monitor watches for all active web_extract issuers.
    Requires Monitor API access (higher Parallel.ai tier).
    """
    issuers = fetch_all(
        """
        SELECT asset_symbol, issuer_name, transparency_url
        FROM cda_issuer_registry
        WHERE transparency_url IS NOT NULL
          AND collection_method IN ('web_extract', 'firecrawl_js')
          AND is_active = TRUE
        ORDER BY asset_symbol
        """
    )

    created = 0
    for iss in issuers:
        symbol = iss["asset_symbol"]
        query = f"{iss['issuer_name']} {symbol} new attestation report or reserve update"

        result = await parallel_client.monitor_create(
            query=query,
            url=iss["transparency_url"],
            frequency="1d",
        )

        if "error" in result:
            logger.warning(f"Monitor setup failed for {symbol}: {result['error']}")
            continue

        monitor_id = result.get("monitor_id") or result.get("id") or ""
        logger.info(f"Monitor created for {symbol}: {monitor_id}")

        execute(
            """
            INSERT INTO cda_monitors (asset_symbol, parallel_monitor_id, query, url, frequency)
            VALUES (%s, %s, %s, %s, 'daily')
            ON CONFLICT (parallel_monitor_id) DO UPDATE SET
                query = EXCLUDED.query, url = EXCLUDED.url
            """,
            (symbol, monitor_id, query, iss["transparency_url"]),
        )

        execute(
            "UPDATE cda_issuer_registry SET parallel_monitor_id = %s WHERE asset_symbol = %s",
            (monitor_id, symbol),
        )
        created += 1

    logger.info(f"Monitor setup complete: {created}/{len(issuers)} created")
    return created


async def discover_new_issuer(asset_symbol: str, coingecko_id: str):
    """
    When backlog promotes a new asset, use Parallel Task API to research
    the issuer and populate the CDA issuer registry.
    New issuers start with collection_method='web_extract' — the adaptive
    waterfall will figure out the right method on first collection run.
    """
    logger.info(f"CDA: Discovering issuer for newly promoted {asset_symbol}")

    result = await parallel_client.task(
        question=(
            f"Research the stablecoin or digital asset '{asset_symbol}' "
            f"(CoinGecko ID: {coingecko_id}). Find the issuing company or protocol."
        ),
        fields={
            "issuer_name": "Name of the company or protocol that issues this token",
            "transparency_url": "URL of their transparency, reserves, or proof-of-reserves page (if any)",
            "asset_category": "One of: fiat-backed, crypto-backed, algorithmic, rwa-tokenized",
            "auditor_name": "Name of their auditing or attestation firm (if any)",
        },
        processor="lite",
    )

    if "error" in result:
        logger.warning(f"CDA: Parallel task failed for {asset_symbol}: {result['error']}")
        execute(
            """
            INSERT INTO cda_issuer_registry
                (asset_symbol, issuer_name, coingecko_id, collection_method, asset_category)
            VALUES (%s, 'Unknown', %s, 'web_extract', 'unknown')
            ON CONFLICT (asset_symbol) DO NOTHING
            """,
            (asset_symbol, coingecko_id),
        )
        return

    fields = result.get("fields", result.get("data", result))

    issuer_name = fields.get("issuer_name", "Unknown")
    transparency_url = fields.get("transparency_url")
    asset_category = fields.get("asset_category", "unknown")

    # All issuers start as web_extract — adaptive waterfall sorts it out
    collection_method = "web_extract"
    if asset_category in ON_CHAIN_CATEGORIES:
        collection_method = "nav_oracle"

    execute(
        """
        INSERT INTO cda_issuer_registry
            (asset_symbol, issuer_name, coingecko_id, transparency_url,
             collection_method, asset_category)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (asset_symbol) DO UPDATE SET
            issuer_name = EXCLUDED.issuer_name,
            transparency_url = EXCLUDED.transparency_url,
            collection_method = EXCLUDED.collection_method,
            asset_category = EXCLUDED.asset_category,
            updated_at = NOW()
        """,
        (asset_symbol, issuer_name, coingecko_id,
         transparency_url, collection_method, asset_category),
    )
    logger.info(f"CDA: Registered {asset_symbol} — {issuer_name} ({asset_category})")

    # Phase 2: Discover source URLs
    source_result = await parallel_client.task(
        question=(
            f"For the stablecoin or digital asset '{asset_symbol}' issued by "
            f"'{issuer_name}', find all transparency and attestation sources. "
            f"I need specific URLs for: "
            f"1. Transparency dashboard (live page showing reserves or backing) "
            f"2. Attestation report page (where PDF reports are published) "
            f"3. Direct PDF links to recent attestation reports "
            f"4. Any public API endpoints for proof-of-reserves or reserve data"
        ),
        fields={
            "dashboard_url": "URL of live transparency dashboard (if exists)",
            "attestation_page_url": "URL of page listing attestation/audit reports (if exists)",
            "latest_pdf_url": "Direct URL to most recent attestation PDF (if exists)",
            "api_url": "URL of public reserve/proof-of-reserves API endpoint (if exists)",
            "other_sources": "Any other transparency source URLs found",
        },
        processor="core",
    )

    if "error" not in source_result:
        src_fields = source_result.get("fields", source_result.get("data", source_result))
        sources = []

        if src_fields.get("dashboard_url"):
            sources.append({
                "url": src_fields["dashboard_url"],
                "type": "dashboard",
                "description": f"{issuer_name} transparency dashboard",
            })
        if src_fields.get("attestation_page_url"):
            sources.append({
                "url": src_fields["attestation_page_url"],
                "type": "attestation_page",
                "description": f"{issuer_name} attestation reports",
            })
        if src_fields.get("latest_pdf_url") and str(src_fields["latest_pdf_url"]).lower().endswith(".pdf"):
            sources.append({
                "url": src_fields["latest_pdf_url"],
                "type": "pdf_direct",
                "description": "Latest attestation report",
            })
        if src_fields.get("api_url"):
            sources.append({
                "url": src_fields["api_url"],
                "type": "api",
                "description": f"{issuer_name} reserve API",
            })

        if sources:
            execute(
                """
                UPDATE cda_issuer_registry
                SET source_urls = %s, updated_at = NOW()
                WHERE asset_symbol = %s
                """,
                (json.dumps(sources), asset_symbol),
            )
            logger.info(f"CDA: Discovered {len(sources)} source URLs for {asset_symbol}")
