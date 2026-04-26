"""
Parallel.ai API client.
Web intelligence layer for CDA: extract pages, search for URLs,
research issuers, monitor for changes.
Docs: https://docs.parallel.ai

Endpoint base paths:
  - Extract / Search: /v1beta
  - Task:             /v1
  - Monitor:          /v1alpha
"""
import os
import asyncio
import httpx
import logging
import time
from typing import Optional, Dict, Any, List
from app.api_usage_tracker import track_api_call

logger = logging.getLogger(__name__)

API_HOST = "https://api.parallel.ai"


def _get_key():
    return os.getenv("PARALLEL_API_KEY")


def _headers():
    return {
        "x-api-key": _get_key(),
        "Content-Type": "application/json",
    }


# =============================================================================
# Extract API (/v1beta)
# =============================================================================

async def extract(url: str, objective: str = None, full_content: bool = True) -> dict:
    """
    Extract content from a URL. Returns markdown excerpts + full content.
    """
    if not _get_key():
        logger.warning("PARALLEL_API_KEY not set, skipping extract")
        return {"error": "no_api_key"}

    async with httpx.AsyncClient(timeout=120) as client:
        payload = {
            "urls": [url],
            "full_content": full_content,
            "excerpts": True,
        }
        if objective:
            payload["objective"] = objective

        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = await client.post(
                    f"{API_HOST}/v1beta/extract",
                    headers=_headers(),
                    json=payload
                )
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="parallel", endpoint="/v1beta/extract", caller="services.parallel_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Parallel extract failed for {url}: {e}")
            return {"error": str(e)}


async def extract_batch(urls: List[str], objective: str = None, full_content: bool = True) -> dict:
    """
    Extract content from multiple URLs in a single request.
    """
    if not _get_key():
        return {"error": "no_api_key"}

    async with httpx.AsyncClient(timeout=120) as client:
        payload = {
            "urls": urls,
            "full_content": full_content,
            "excerpts": True,
        }
        if objective:
            payload["objective"] = objective

        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = await client.post(
                    f"{API_HOST}/v1beta/extract",
                    headers=_headers(),
                    json=payload
                )
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="parallel", endpoint="/v1beta/extract_batch", caller="services.parallel_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Parallel batch extract failed: {e}")
            return {"error": str(e)}


# =============================================================================
# Search API (/v1beta)
# =============================================================================

async def search(query: str, num_results: int = 10) -> dict:
    """
    Search the web. Returns ranked URLs with excerpts.
    """
    if not _get_key():
        return {"error": "no_api_key"}

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = await client.post(
                    f"{API_HOST}/v1beta/search",
                    headers=_headers(),
                    json={"search_queries": [query]}
                )
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="parallel", endpoint="/v1beta/search", caller="services.parallel_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Parallel search failed for '{query}': {e}")
            return {"error": str(e)}


# =============================================================================
# Task API (/v1) — async: create run, then poll for result
# =============================================================================

TASK_PROCESSOR_MAP = {"lite": "base", "base": "base", "core": "core", "ultra": "ultra"}


async def task(
    question: str,
    fields: dict = None,
    processor: str = "base",
    poll_timeout: int = 300,
) -> dict:
    """
    Deep web research with structured output.

    Creates an async task run and polls for the result.
    `fields` is converted to a JSON output_schema so Parallel returns structured data.
    `processor`: "base" ($0.005), "core" ($0.025), "ultra" ($0.10).
    """
    if not _get_key():
        return {"error": "no_api_key"}

    proc = TASK_PROCESSOR_MAP.get(processor, processor)

    # Build request body
    body: dict = {
        "input": question,
        "processor": proc,
    }

    # Convert fields dict into a JSON output schema
    if fields:
        schema = {
            "type": "json",
            "json_schema": {
                "type": "object",
                "properties": {
                    k: {"type": "string", "description": v}
                    for k, v in fields.items()
                },
            },
        }
        body["task_spec"] = {"output_schema": schema}

    async with httpx.AsyncClient(timeout=poll_timeout + 30) as client:
        # Step 1: Create run
        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = await client.post(
                    f"{API_HOST}/v1/tasks/runs",
                    headers=_headers(),
                    json=body,
                )
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="parallel", endpoint="/v1/tasks/runs", caller="services.parallel_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            resp.raise_for_status()
            run_data = resp.json()
        except Exception as e:
            logger.error(f"Parallel task create failed: {e}")
            return {"error": str(e)}

        run_id = run_data.get("run_id")
        if not run_id:
            logger.error(f"Parallel task: no run_id in response: {run_data}")
            return {"error": "no_run_id", "response": run_data}

        logger.info(f"Parallel task created: {run_id} (processor={proc})")

        # Step 2: Poll for result (blocking endpoint with server-side timeout)
        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = await client.get(
                    f"{API_HOST}/v1/tasks/runs/{run_id}/result",
                    headers=_headers(),
                    params={"timeout": poll_timeout},
                )
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="parallel", endpoint="/v1/tasks/runs/result", caller="services.parallel_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            resp.raise_for_status()
            result = resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 408:
                logger.warning(f"Parallel task {run_id} timed out after {poll_timeout}s")
                return {"error": "timeout", "run_id": run_id}
            logger.error(f"Parallel task result failed: {e}")
            return {"error": str(e), "run_id": run_id}
        except Exception as e:
            logger.error(f"Parallel task result failed: {e}")
            return {"error": str(e), "run_id": run_id}

        # Extract output
        output = result.get("output", {})
        content = output.get("content", {})

        # If JSON output, return the content directly
        if isinstance(content, dict):
            return {"fields": content, "run_id": run_id, "output": output}

        # Text output — return as-is
        return {"text": content, "run_id": run_id, "output": output}


# =============================================================================
# Monitor API (/v1alpha)
# =============================================================================

async def monitor_create(
    query: str,
    frequency: str = "1d",
    url: str = None,
    webhook_url: str = None,
) -> dict:
    """
    Create a web monitor watch.

    frequency: "<number><unit>" where unit is h/d/w (e.g. "1d", "12h", "1w").
    webhook_url: URL to receive POST alerts when events are detected.
    """
    if not _get_key():
        return {"error": "no_api_key"}

    payload: dict = {"query": query, "frequency": frequency}
    if webhook_url:
        payload["webhook"] = {
            "url": webhook_url,
            "event_types": ["monitor.event.detected"],
        }
    # Parallel Monitor doesn't have a `url` field in the create body —
    # the query itself should reference the URL/topic to watch.
    # If a url was passed, incorporate it into the query.
    if url:
        payload["query"] = f"{query} — watch {url}"

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = await client.post(
                    f"{API_HOST}/v1alpha/monitors",
                    headers=_headers(),
                    json=payload,
                )
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="parallel", endpoint="/v1alpha/monitors", caller="services.parallel_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            detail = e.response.text[:500] if e.response else ""
            logger.error(f"Parallel monitor create failed: {e} — {detail}")
            return {"error": str(e), "detail": detail}
        except Exception as e:
            logger.error(f"Parallel monitor create failed: {e}")
            return {"error": str(e)}


async def monitor_list() -> dict:
    """List all monitors."""
    if not _get_key():
        return {"error": "no_api_key"}

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = await client.get(
                    f"{API_HOST}/v1alpha/monitors",
                    headers=_headers(),
                )
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="parallel", endpoint="/v1alpha/monitors", caller="services.parallel_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Parallel monitor list failed: {e}")
            return {"error": str(e)}


async def monitor_delete(monitor_id: str) -> dict:
    """Delete a monitor."""
    if not _get_key():
        return {"error": "no_api_key"}

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = await client.delete(
                    f"{API_HOST}/v1alpha/monitors/{monitor_id}",
                    headers=_headers(),
                )
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="parallel", endpoint=f"/v1alpha/monitors/{monitor_id}", caller="services.parallel_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Parallel monitor delete failed: {e}")
            return {"error": str(e)}


async def monitor_events(monitor_id: str, lookback: str = "10d") -> dict:
    """Get events for a monitor."""
    if not _get_key():
        return {"error": "no_api_key"}

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            _t0 = time.monotonic()
            _status = None
            try:
                resp = await client.get(
                    f"{API_HOST}/v1alpha/monitors/{monitor_id}/events",
                    headers=_headers(),
                    params={"lookback_period": lookback},
                )
                _status = resp.status_code
            except Exception:
                _status = 0
                raise
            finally:
                try:
                    track_api_call(provider="parallel", endpoint=f"/v1alpha/monitors/{monitor_id}/events", caller="services.parallel_client", status=_status, latency_ms=int((time.monotonic() - _t0) * 1000))
                except Exception:
                    pass
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Parallel monitor events failed: {e}")
            return {"error": str(e)}
