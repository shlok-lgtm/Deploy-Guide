#!/usr/bin/env python3
"""
Register the basis-hub `cg.coin.info.updated` subscription against
CoinGecko's webhook API for every scoring-enabled stablecoin slug and
every PSI protocol governance-token slug.

Idempotent: existing subscriptions for the same (event, coin_id,
callback_url) tuple are detected via a GET probe and skipped. Safe to
re-run after coin promotions or rotations.

Usage:
    export COINGECKO_API_KEY=...                          # pro-api key
    export COINGECKO_WEBHOOK_SECRET=...                   # subscription HMAC secret
    export COINGECKO_WEBHOOK_CALLBACK_URL=https://hub.example/api/webhooks/coingecko
    python scripts/coingecko/subscribe_webhooks.py
    python scripts/coingecko/subscribe_webhooks.py --dry-run

Pre-flight reminder (per task spec): confirm the production CoinGecko
pro-api plan supports webhooks before running. The script reports
which plan the key is attached to via `/api/v3/key` if the response
includes plan metadata.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Optional

import httpx

logger = logging.getLogger("subscribe_webhooks")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

CG_PRO_BASE = "https://pro-api.coingecko.com/api/v3"
EVENT_NAME = "cg.coin.info.updated"


def _require_env(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        logger.error(f"Missing required env: {name}")
        sys.exit(2)
    return val


def _stablecoin_slugs() -> list[str]:
    """All scoring-enabled stablecoin coingecko_ids from the DB.

    DB is the authoritative registry (auto-promotion fills it). Falls
    back to STABLECOIN_REGISTRY if DB is unreachable so the script is
    runnable from a workstation without DATABASE_URL.
    """
    try:
        from app.database import fetch_all
        rows = fetch_all(
            "SELECT coingecko_id FROM stablecoins "
            "WHERE scoring_enabled = TRUE AND coingecko_id IS NOT NULL "
            "ORDER BY coingecko_id"
        ) or []
        slugs = [r["coingecko_id"] for r in rows if r.get("coingecko_id")]
        if slugs:
            return slugs
    except Exception as e:
        logger.warning(f"DB stablecoin lookup failed, falling back to registry: {e}")

    from app.config import STABLECOIN_REGISTRY
    return [cfg["coingecko_id"] for cfg in STABLECOIN_REGISTRY.values() if cfg.get("coingecko_id")]


def _protocol_slugs() -> list[str]:
    """All PSI protocol governance-token coingecko_ids."""
    from app.collectors.psi_collector import PROTOCOL_GOVERNANCE_TOKENS
    return sorted({v for v in PROTOCOL_GOVERNANCE_TOKENS.values() if v})


def _headers(api_key: str) -> dict:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-cg-pro-api-key": api_key,
    }


def _list_existing(client: httpx.Client, api_key: str) -> list[dict]:
    """
    Fetch existing webhook subscriptions. CoinGecko's webhook list
    endpoint path may differ by plan — we try the documented form and
    return [] if the endpoint isn't reachable (the create path is
    still safe because we tolerate 409/conflict on create).
    """
    candidates = ["/webhooks", "/webhook/subscriptions", "/webhook/list"]
    for path in candidates:
        try:
            r = client.get(f"{CG_PRO_BASE}{path}", headers=_headers(api_key), timeout=15.0)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return data
                if isinstance(data, dict) and isinstance(data.get("subscriptions"), list):
                    return data["subscriptions"]
            logger.debug(f"list probe {path} → {r.status_code}")
        except Exception as e:
            logger.debug(f"list probe {path} failed: {e}")
    return []


def _already_subscribed(existing: list[dict], coin_id: str, callback_url: str) -> bool:
    for sub in existing:
        if not isinstance(sub, dict):
            continue
        same_event = sub.get("event") == EVENT_NAME or sub.get("event_name") == EVENT_NAME
        same_coin = sub.get("coin_id") == coin_id or sub.get("slug") == coin_id
        same_cb = sub.get("callback_url") == callback_url or sub.get("url") == callback_url
        if same_event and same_coin and same_cb:
            return True
    return False


def _subscribe(
    client: httpx.Client,
    api_key: str,
    coin_id: str,
    callback_url: str,
    secret: str,
    dry_run: bool,
) -> tuple[bool, str]:
    """Create a single subscription. Returns (ok, status_msg)."""
    body = {
        "event": EVENT_NAME,
        "coin_id": coin_id,
        "callback_url": callback_url,
        "secret": secret,
    }
    if dry_run:
        return True, "dry_run"

    candidates = ["/webhooks", "/webhook/subscriptions"]
    last_err = ""
    for path in candidates:
        try:
            r = client.post(
                f"{CG_PRO_BASE}{path}",
                headers=_headers(api_key),
                content=json.dumps(body),
                timeout=20.0,
            )
            if r.status_code in (200, 201):
                return True, f"created via {path}"
            if r.status_code == 409:
                return True, f"already_exists via {path}"
            last_err = f"{path} → {r.status_code}: {r.text[:200]}"
        except Exception as e:
            last_err = f"{path} exc: {e}"
    return False, last_err


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Print plan, do not POST.")
    args = parser.parse_args()

    api_key = _require_env("COINGECKO_API_KEY")
    secret = _require_env("COINGECKO_WEBHOOK_SECRET")
    callback_url = _require_env("COINGECKO_WEBHOOK_CALLBACK_URL")

    stablecoin_slugs = _stablecoin_slugs()
    protocol_slugs = _protocol_slugs()
    all_slugs = sorted(set(stablecoin_slugs) | set(protocol_slugs))

    logger.info(
        f"Plan: subscribe to {EVENT_NAME} for "
        f"{len(stablecoin_slugs)} stablecoin slugs + "
        f"{len(protocol_slugs)} protocol slugs "
        f"({len(all_slugs)} unique). callback={callback_url} dry_run={args.dry_run}"
    )

    with httpx.Client() as client:
        try:
            key_check = client.get(f"{CG_PRO_BASE}/key", headers=_headers(api_key), timeout=15.0)
            if key_check.status_code == 200:
                logger.info(f"key metadata: {key_check.text[:300]}")
            else:
                logger.warning(f"key probe → {key_check.status_code}: {key_check.text[:200]}")
        except Exception as e:
            logger.warning(f"key probe failed: {e}")

        existing = _list_existing(client, api_key)
        logger.info(f"existing subscriptions discovered: {len(existing)}")

        created = 0
        skipped = 0
        failed = 0
        for slug in all_slugs:
            if _already_subscribed(existing, slug, callback_url):
                logger.info(f"  [skip] {slug} already subscribed")
                skipped += 1
                continue
            ok, msg = _subscribe(client, api_key, slug, callback_url, secret, args.dry_run)
            if ok:
                logger.info(f"  [ok]   {slug} — {msg}")
                created += 1
            else:
                logger.error(f"  [fail] {slug} — {msg}")
                failed += 1
            time.sleep(0.1)  # gentle on the API

    logger.info(f"done: created={created} skipped={skipped} failed={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
