"""
RPI Documentation Scorer
==========================
Automated scoring of protocol risk documentation against a 5-criterion rubric.
Each criterion is worth 20 points (0-100 total).

Fetches each protocol's public docs site and checks for keyword presence
to score documentation depth. Stores evidence (URL + snippet) for each criterion.

Automates the documentation_depth lens component (risk_transparency lens).
"""

import logging
import re
import time

import httpx

from app.database import execute, fetch_all

logger = logging.getLogger(__name__)

# Documentation URLs per protocol
PROTOCOL_DOCS = {
    "aave": "https://docs.aave.com",
    "lido": "https://docs.lido.fi",
    "eigenlayer": "https://docs.eigenlayer.xyz",
    "sky": "https://docs.sky.money",
    "compound-finance": "https://docs.compound.finance",
    "uniswap": "https://docs.uniswap.org",
    "curve-finance": "https://resources.curve.fi",
    "morpho": "https://docs.morpho.org",
    "spark": "https://docs.spark.fi",
    "convex-finance": "https://docs.convexfinance.com",
    "drift": "https://docs.drift.trade",
    "jupiter-perpetual-exchange": "https://station.jup.ag/docs",
    "raydium": "https://docs.raydium.io",
}

# 5 rubric criteria with keywords to search for
RUBRIC = {
    "risk_framework": {
        "label": "Risk framework published",
        "keywords": ["risk framework", "risk management", "risk policy",
                     "risk assessment", "risk methodology", "risk parameters"],
        "paths": ["/risk", "/security/risk", "/governance/risk",
                  "/risk-framework", "/risk-management"],
    },
    "parameter_methodology": {
        "label": "Parameter methodology documented",
        "keywords": ["parameter", "interest rate model", "collateral factor",
                     "liquidation threshold", "supply cap", "borrow cap",
                     "parameter update", "risk parameters"],
        "paths": ["/risk/parameters", "/protocol/parameters",
                  "/governance/parameters", "/risk-parameters"],
    },
    "incident_response": {
        "label": "Incident response process documented",
        "keywords": ["incident response", "emergency procedure", "post-mortem",
                     "guardian", "emergency admin", "pause mechanism",
                     "circuit breaker", "emergency shutdown"],
        "paths": ["/security/incident", "/governance/emergency",
                  "/security/emergency", "/incident-response"],
    },
    "collateral_risk_policies": {
        "label": "Counterparty/collateral risk policies published",
        "keywords": ["collateral", "counterparty risk", "accepted assets",
                     "asset listing", "onboarding criteria", "asset risk",
                     "collateral requirements", "listing methodology"],
        "paths": ["/risk/asset", "/governance/asset-listing",
                  "/risk/collateral", "/asset-onboarding"],
    },
    "audit_history": {
        "label": "Audit history and scope documented",
        "keywords": ["audit", "security review", "security audit",
                     "bug bounty", "formal verification", "auditor",
                     "audit report", "security assessment"],
        "paths": ["/security/audits", "/security", "/audits",
                  "/security-audits", "/bug-bounty"],
    },
}


def _fetch_page(url: str) -> str | None:
    """Fetch a page and return text content."""
    time.sleep(1.0)
    try:
        resp = httpx.get(url, timeout=15, follow_redirects=True)
        if resp.status_code == 200:
            # Strip HTML tags for keyword matching
            text = re.sub(r'<[^>]+>', ' ', resp.text)
            return re.sub(r'\s+', ' ', text).strip()
    except Exception as e:
        logger.debug(f"Failed to fetch {url}: {e}")
    return None


def _score_criterion(docs_url: str, criterion_id: str, config: dict) -> tuple[int, str | None, str | None]:
    """Score a single rubric criterion. Returns (score, evidence_url, snippet)."""
    keywords = config["keywords"]
    paths = config["paths"]

    # Check main docs page first
    main_text = _fetch_page(docs_url)
    if main_text:
        text_lower = main_text.lower()
        matches = [kw for kw in keywords if kw.lower() in text_lower]
        if len(matches) >= 2:
            # Find a snippet around the first match
            idx = text_lower.find(matches[0].lower())
            snippet = main_text[max(0, idx - 50):idx + 150].strip()
            return 20, docs_url, snippet

    # Check specific subpaths
    for path in paths:
        url = f"{docs_url.rstrip('/')}{path}"
        text = _fetch_page(url)
        if text:
            text_lower = text.lower()
            matches = [kw for kw in keywords if kw.lower() in text_lower]
            if matches:
                idx = text_lower.find(matches[0].lower())
                snippet = text[max(0, idx - 50):idx + 150].strip()
                return 20, url, snippet

    # Partial credit: if main docs page mentions at least one keyword
    if main_text:
        text_lower = main_text.lower()
        if any(kw.lower() in text_lower for kw in keywords):
            return 10, docs_url, None

    return 0, None, None


def score_protocol_docs(protocol_slug: str, docs_url: str = None) -> dict:
    """Score a protocol's documentation against the 5-criterion rubric.

    Returns dict with total_score, per-criterion scores, and evidence.
    """
    if docs_url is None:
        docs_url = PROTOCOL_DOCS.get(protocol_slug)
        # Also check rpi_protocol_config
        if not docs_url:
            try:
                from app.database import fetch_one
                row = fetch_one(
                    "SELECT docs_url FROM rpi_protocol_config WHERE protocol_slug = %s",
                    (protocol_slug,)
                )
                if row and row.get("docs_url"):
                    docs_url = row["docs_url"]
            except Exception:
                pass

    if not docs_url:
        logger.debug(f"No docs URL for {protocol_slug}")
        return {"protocol_slug": protocol_slug, "total_score": 0, "criteria": {}}

    criteria_results = {}
    total = 0

    for criterion_id, config in RUBRIC.items():
        score, evidence_url, snippet = _score_criterion(docs_url, criterion_id, config)
        criteria_results[criterion_id] = {
            "label": config["label"],
            "score": score,
            "max": 20,
            "evidence_url": evidence_url,
            "snippet": snippet[:200] if snippet else None,
        }
        total += score

        # Store evidence in DB
        try:
            execute("""
                INSERT INTO rpi_doc_scores
                    (protocol_slug, criterion, score, evidence_url, evidence_snippet)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (protocol_slug, criterion) DO UPDATE SET
                    score = EXCLUDED.score,
                    evidence_url = EXCLUDED.evidence_url,
                    evidence_snippet = EXCLUDED.evidence_snippet,
                    scored_at = NOW()
            """, (protocol_slug, criterion_id, score, evidence_url,
                  snippet[:200] if snippet else None))
        except Exception as e:
            logger.debug(f"Failed to store doc score for {protocol_slug}/{criterion_id}: {e}")

    return {
        "protocol_slug": protocol_slug,
        "docs_url": docs_url,
        "total_score": total,
        "criteria": criteria_results,
    }


def score_all_docs() -> list[dict]:
    """Score documentation for all protocols and update the lens component."""
    from app.index_definitions.rpi_v2 import RPI_TARGET_PROTOCOLS

    results = []
    for slug in RPI_TARGET_PROTOCOLS:
        try:
            result = score_protocol_docs(slug)
            results.append(result)

            # Update the documentation_depth lens component
            if result["total_score"] > 0:
                execute("""
                    INSERT INTO rpi_components
                        (protocol_slug, component_id, component_type, lens_id,
                         raw_value, normalized_score, source_type, data_source,
                         collected_at)
                    VALUES (%s, 'documentation_depth', 'lens', 'risk_transparency',
                            %s, %s, 'automated', 'docs_scraper', NOW())
                """, (slug, result["total_score"], float(result["total_score"])))

            logger.info(f"RPI docs: {slug} = {result['total_score']}/100")
        except Exception as e:
            logger.warning(f"Docs scoring failed for {slug}: {e}")

    return results
