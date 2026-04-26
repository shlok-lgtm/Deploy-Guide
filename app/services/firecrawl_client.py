"""
Firecrawl client — JS rendering, screenshot capture, and PDF discovery.

Used for:
1. JS-heavy pages that Parallel Extract can't handle (e.g. Paxos/PYUSD)
2. Static evidence capture: rendered screenshots + clean markdown extraction
   for the Witness page evidence pipeline.

Paxos serves attestation PDFs via a Framer-based site with no static PDF URLs in the HTML.
The PDFs are embedded as framerusercontent.com/assets/*.pdf URLs in Framer component JS modules.
This client fetches those modules, extracts PDF URLs, and identifies the most recent attestation.
"""
import base64
import os
import re
import logging
import time
import httpx
from app.api_usage_tracker import track_api_call

try:
    from firecrawl import Firecrawl
except ImportError:
    Firecrawl = None

logger = logging.getLogger(__name__)

FIRECRAWL_API_KEY = os.getenv("FIRECRAWL_API_KEY")


def get_client():
    if Firecrawl is None:
        raise ImportError("firecrawl package not installed — pip install firecrawl-py")
    if not FIRECRAWL_API_KEY:
        raise ValueError("FIRECRAWL_API_KEY not set")
    return Firecrawl(api_key=FIRECRAWL_API_KEY)


def scrape_js_page(url: str, wait_ms: int = 5000, actions: list = None) -> dict:
    """
    Scrape a JS-rendered page with optional interactions.
    Returns markdown content and any links/screenshots.
    """
    client = get_client()

    scrape_params = {
        "formats": ["markdown", "links"],
    }

    if actions:
        scrape_params["actions"] = actions
    else:
        scrape_params["actions"] = [
            {"type": "wait", "milliseconds": wait_ms},
            {"type": "screenshot", "fullPage": True},
        ]

    result = client.scrape(url, **scrape_params)
    return result


def discover_framer_pdfs(page_url: str) -> list[str]:
    """
    Discover PDF URLs embedded in Framer JS component modules.

    Framer sites (like paxos.com) embed PDF viewer URLs in compiled JS modules
    hosted at framerusercontent.com/sites/*/. This function:
    1. Scrapes the page HTML to find component module URLs
    2. Fetches each module and extracts framerusercontent.com/assets/*.pdf URLs
    3. Returns deduplicated list of PDF URLs
    """
    client = get_client()

    # Step 1: Get page HTML to find Framer component module URLs
    result = client.scrape(
        page_url,
        formats=["html"],
        actions=[{"type": "wait", "milliseconds": 5000}],
    )

    html = getattr(result, "html", "") or ""
    if not html:
        logger.warning(f"Firecrawl: No HTML returned for {page_url}")
        return []

    # Step 2: Find Framer component module URLs (exclude common libs)
    module_urls = list(set(re.findall(
        r'https://framerusercontent\.com/sites/[^\s"\']+\.mjs', html
    )))

    skip_modules = {"react.", "motion.", "framer.", "shared.", "shared-lib.", "rolldown-runtime.", "pdf.min.", "Pdfviewer."}
    component_modules = [
        u for u in module_urls
        if not any(skip in u.split("/")[-1] for skip in skip_modules)
    ]

    logger.info(f"Firecrawl: Found {len(component_modules)} component modules in {page_url}")

    # Step 3: Fetch each component module and extract PDF asset URLs
    all_pdfs = []
    for mod_url in component_modules:
        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = httpx.get(mod_url, timeout=15, follow_redirects=True)
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="firecrawl", endpoint="framer_module", caller="services.firecrawl_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            pdf_urls = re.findall(
                r'https://framerusercontent\.com/assets/[A-Za-z0-9_]+\.pdf',
                resp.text,
            )
            all_pdfs.extend(pdf_urls)
        except Exception as e:
            logger.warning(f"Firecrawl: Failed to fetch module {mod_url}: {e}")

    # Deduplicate preserving order
    seen = set()
    unique = []
    for u in all_pdfs:
        if u not in seen:
            seen.add(u)
            unique.append(u)

    logger.info(f"Firecrawl: Discovered {len(unique)} unique PDF URLs from Framer modules")
    return unique


def identify_most_recent_attestation(pdf_urls: list[str]) -> str | None:
    """
    Given a list of PDF URLs from a Framer page, identify the most recent
    attestation report by downloading the first page of candidates.

    The Framer component orders PDFs by section (Attestations first, then Reports)
    and within each section by year (newest first) then month.
    For the attestation section, each month typically has 3 PDFs corresponding
    to different years (e.g., Jan 2026, Jan 2025, Jan 2024).

    Strategy: check the first few PDFs from each month group until we find
    the newest KPMG attestation.
    """
    if not pdf_urls:
        return None

    # The first PDF in the list is typically a recurring icon/template (appears many times).
    # Skip it and check subsequent PDFs.
    # Also skip very small PDFs (< 100KB likely not full attestation reports).
    candidates = pdf_urls[1:10]  # Check first 9 non-icon PDFs

    best_url = None
    best_date = None

    for url in candidates:
        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = httpx.get(url, timeout=30, follow_redirects=True)
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="firecrawl", endpoint="pdf_probe", caller="services.firecrawl_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            if len(resp.content) < 100_000:
                continue  # Too small for an attestation report

            # Quick check: look for KPMG and date in first ~5000 bytes of text
            # PDF text extraction is limited but we can search for key patterns
            text = resp.content[:10000].decode("latin-1", errors="ignore")

            is_kpmg = "KPMG" in text
            is_attestation = "Independent Accountants" in text or "Examination Report" in text

            if not (is_kpmg and is_attestation):
                continue

            # Extract date from the report
            date_match = re.search(
                r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+202[4-9]',
                text,
            )
            if date_match:
                from datetime import datetime
                try:
                    d = datetime.strptime(date_match.group(), "%B %d, %Y")
                    if best_date is None or d > best_date:
                        best_date = d
                        best_url = url
                except ValueError:
                    pass

        except Exception as e:
            logger.warning(f"Firecrawl: Error checking PDF {url}: {e}")

    if best_url:
        logger.info(f"Firecrawl: Most recent attestation: {best_url} ({best_date})")
    else:
        logger.warning("Firecrawl: Could not identify most recent attestation from PDFs")

    return best_url


def capture_screenshot_and_markdown(url: str, wait_ms: int = 3000) -> dict:
    """
    Capture a rendered screenshot and clean markdown extraction in a single
    Firecrawl API call.

    Returns: {
        "screenshot_bytes": bytes | None  (PNG),
        "markdown_content": str | None    (clean page text),
        "success": bool,
    }
    """
    try:
        client = get_client()
        result = client.scrape(
            url,
            formats=["screenshot", "markdown"],
            actions=[{"type": "wait", "milliseconds": wait_ms}],
        )

        screenshot_b64 = getattr(result, "screenshot", None) or ""
        markdown = getattr(result, "markdown", None) or ""

        screenshot_bytes = None
        if screenshot_b64:
            # Strip data:image/png;base64, prefix if present
            if "," in screenshot_b64:
                screenshot_b64 = screenshot_b64.split(",", 1)[1]
            screenshot_bytes = base64.b64decode(screenshot_b64)

        return {
            "screenshot_bytes": screenshot_bytes,
            "markdown_content": markdown or None,
            "success": screenshot_bytes is not None,
        }
    except Exception as e:
        logger.warning(f"Firecrawl screenshot capture failed for {url}: {e}")
        return {
            "screenshot_bytes": None,
            "markdown_content": None,
            "success": False,
        }
