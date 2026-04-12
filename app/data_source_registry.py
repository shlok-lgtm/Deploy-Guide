"""
Data Source Registry
=====================
Utility for collectors to register the external HTTP calls they make.
The provenance service reads the registry and auto-proves whatever's in it.

Usage (one line at each HTTP call site):

    from app.data_source_registry import register_data_source
    register_data_source("pro-api.coingecko.com", "/api/v3/coins/{id}",
                         "sii_collector", description="Current coin data")

Key behaviors:
- Strips API keys and secrets from params before storing
- Upserts — if source exists, updates last_seen
- In-memory cache with 1-hour TTL to avoid DB writes on every HTTP call
"""

import json
import logging
import re
import time
from typing import Optional

logger = logging.getLogger(__name__)

# In-memory cache: (domain, endpoint, method) -> last_write_timestamp
# Only writes to DB once per hour per source to avoid hammering.
_registry_cache: dict[tuple[str, str, str], float] = {}
_CACHE_TTL = 3600  # 1 hour

# Patterns to strip from params templates (API keys, tokens, secrets)
_SECRET_PARAM_PATTERNS = re.compile(
    r"(api[_-]?key|apikey|auth|token|secret|password|credential|x[_-]cg[_-]pro)",
    re.IGNORECASE,
)


def _strip_secrets(params: Optional[dict]) -> Optional[dict]:
    """Remove any key/value pairs that look like API keys or secrets."""
    if not params:
        return params
    cleaned = {}
    for k, v in params.items():
        if _SECRET_PARAM_PATTERNS.search(k):
            continue
        cleaned[k] = v
    return cleaned if cleaned else None


def register_data_source(
    domain: str,
    endpoint: str,
    collector: str,
    method: str = "GET",
    description: Optional[str] = None,
    params_template: Optional[dict] = None,
    response_size_estimate: Optional[int] = None,
    prove_frequency: str = "hourly",
    prove: bool = True,
    notes: Optional[str] = None,
) -> None:
    """Register an external data source in the registry.

    Safe to call on every HTTP request — uses an in-memory cache so the DB
    is only touched once per hour per unique (domain, endpoint, method).
    """
    cache_key = (domain, endpoint, method)
    now = time.time()

    # Check in-memory cache — skip DB write if recently registered
    last_write = _registry_cache.get(cache_key)
    if last_write and (now - last_write) < _CACHE_TTL:
        return

    safe_params = _strip_secrets(params_template)

    try:
        from app.database import execute
        execute(
            """INSERT INTO data_source_registry
               (source_domain, source_endpoint, method, description, collector,
                params_template, response_size_estimate, prove, prove_frequency, notes)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (source_domain, source_endpoint, method)
               DO UPDATE SET
                   last_seen = NOW(),
                   collector = EXCLUDED.collector,
                   description = COALESCE(EXCLUDED.description, data_source_registry.description),
                   params_template = COALESCE(EXCLUDED.params_template, data_source_registry.params_template),
                   response_size_estimate = COALESCE(EXCLUDED.response_size_estimate, data_source_registry.response_size_estimate),
                   notes = COALESCE(EXCLUDED.notes, data_source_registry.notes)
            """,
            (
                domain,
                endpoint,
                method,
                description,
                collector,
                json.dumps(safe_params) if safe_params else None,
                response_size_estimate,
                prove,
                prove_frequency,
                notes,
            ),
        )
        _registry_cache[cache_key] = now
    except Exception as e:
        # Never let registration failure break a collector
        logger.debug(f"data_source_registry upsert failed (non-fatal): {e}")
