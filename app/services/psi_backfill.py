"""
PSI Historical Backfill
========================
Populates historical_protocol_data from DeFiLlama and CoinGecko.
Daily granularity, idempotent. Used by PSI temporal reconstruction.

DeFiLlama /protocol/{slug} returns daily TVL history (free, no key).
CoinGecko /coins/{id}/market_chart/range returns daily price/mcap/volume.
"""

import os
import time
import logging
from datetime import datetime, timezone, timedelta

import httpx

from app.database import execute, fetch_one
from app.index_definitions.psi_v01 import TARGET_PROTOCOLS
from app.collectors.psi_collector import PROTOCOL_GOVERNANCE_TOKENS

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"
CG_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
CG_BASE = "https://pro-api.coingecko.com/api/v3" if CG_API_KEY else "https://api.coingecko.com/api/v3"


def _cg_headers():
    h = {"Accept": "application/json"}
    if CG_API_KEY:
        h["x-cg-pro-api-key"] = CG_API_KEY
    return h


def _ensure_table():
    """Create historical_protocol_data table if it doesn't exist."""
    execute("""
        CREATE TABLE IF NOT EXISTS historical_protocol_data (
            id SERIAL PRIMARY KEY,
            protocol_slug VARCHAR(64) NOT NULL,
            record_date DATE NOT NULL,
            tvl NUMERIC,
            fees_24h NUMERIC,
            revenue_24h NUMERIC,
            token_price NUMERIC,
            token_mcap NUMERIC,
            token_volume NUMERIC,
            chain_count INTEGER,
            data_source VARCHAR(32) DEFAULT 'defillama+coingecko',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(protocol_slug, record_date)
        )
    """)
    execute(
        "CREATE INDEX IF NOT EXISTS idx_hist_protocol_slug_date "
        "ON historical_protocol_data(protocol_slug, record_date)"
    )


def backfill_protocol_tvl(slug: str) -> int:
    """Fetch full TVL history from DeFiLlama /protocol/{slug}."""
    try:
        resp = httpx.get(f"{DEFILLAMA_BASE}/protocol/{slug}", timeout=45)
        if resp.status_code != 200:
            logger.warning(f"DeFiLlama {slug} returned {resp.status_code}")
            return 0
        data = resp.json()
    except Exception as e:
        logger.error(f"DeFiLlama fetch error for {slug}: {e}")
        return 0

    tvl_history = data.get("tvl", [])
    if not tvl_history:
        logger.warning(f"No TVL history for {slug}")
        return 0

    # Chain count from currentChainTvls (snapshot — same for all dates)
    current_chain_tvls = data.get("currentChainTvls", {})
    chain_count = len([
        k for k, v in current_chain_tvls.items()
        if "-" not in k and isinstance(v, (int, float)) and v > 0
    ]) if current_chain_tvls else 1

    records = 0
    for entry in tvl_history:
        ts = entry.get("date")
        tvl_val = entry.get("totalLiquidityUSD", 0)
        if not ts or not tvl_val:
            continue

        record_date = datetime.fromtimestamp(ts, tz=timezone.utc).date()

        try:
            execute("""
                INSERT INTO historical_protocol_data
                    (protocol_slug, record_date, tvl, chain_count, data_source)
                VALUES (%s, %s, %s, %s, 'defillama')
                ON CONFLICT (protocol_slug, record_date) DO UPDATE SET
                    tvl = EXCLUDED.tvl,
                    chain_count = EXCLUDED.chain_count
            """, (slug, record_date.isoformat(), tvl_val, chain_count))
            records += 1
        except Exception as e:
            logger.debug(f"TVL insert error for {slug} @ {record_date}: {e}")

    logger.info(f"Backfilled {records} TVL records for {slug}")
    return records


def backfill_protocol_token(slug: str, gecko_id: str, from_date: str = "2024-01-01") -> int:
    """Fetch historical token price/mcap/volume from CoinGecko in 90-day chunks."""
    if not gecko_id:
        return 0

    from_dt = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    to_dt = datetime.now(timezone.utc)
    chunk_days = 90
    records = 0

    current = from_dt
    while current < to_dt:
        chunk_end = min(current + timedelta(days=chunk_days), to_dt)
        from_ts = int(current.timestamp())
        to_ts = int(chunk_end.timestamp())

        try:
            resp = httpx.get(
                f"{CG_BASE}/coins/{gecko_id}/market_chart/range",
                params={"vs_currency": "usd", "from": from_ts, "to": to_ts},
                headers=_cg_headers(),
                timeout=30,
            )
            if resp.status_code == 429:
                logger.warning("CoinGecko rate limit — sleeping 60s")
                time.sleep(60)
                continue
            if resp.status_code != 200:
                logger.warning(f"CoinGecko {gecko_id} returned {resp.status_code}")
                current = chunk_end
                continue

            data = resp.json()
        except Exception as e:
            logger.error(f"CoinGecko fetch error for {gecko_id}: {e}")
            current = chunk_end
            continue

        prices = data.get("prices", [])
        mcaps = data.get("market_caps", [])
        volumes = data.get("total_volumes", [])

        mcap_map = {}
        for ts_ms, val in mcaps:
            d = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
            mcap_map[d] = val

        vol_map = {}
        for ts_ms, val in volumes:
            d = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
            vol_map[d] = val

        for ts_ms, price in prices:
            d = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).date()
            try:
                execute("""
                    INSERT INTO historical_protocol_data
                        (protocol_slug, record_date, token_price, token_mcap, token_volume, data_source)
                    VALUES (%s, %s, %s, %s, %s, 'coingecko')
                    ON CONFLICT (protocol_slug, record_date) DO UPDATE SET
                        token_price = COALESCE(EXCLUDED.token_price, historical_protocol_data.token_price),
                        token_mcap = COALESCE(EXCLUDED.token_mcap, historical_protocol_data.token_mcap),
                        token_volume = COALESCE(EXCLUDED.token_volume, historical_protocol_data.token_volume)
                """, (slug, d.isoformat(), price, mcap_map.get(d), vol_map.get(d)))
                records += 1
            except Exception:
                pass

        current = chunk_end
        time.sleep(1.5)

    logger.info(f"Backfilled {records} token records for {slug} ({gecko_id})")
    return records


def backfill_all_protocols(from_date: str = "2024-01-01") -> dict:
    """Backfill historical data for all PSI-scored protocols."""
    _ensure_table()

    results = {}
    for slug in TARGET_PROTOCOLS:
        logger.info(f"Backfilling {slug}...")

        tvl_count = backfill_protocol_tvl(slug)
        time.sleep(1)

        gecko_id = PROTOCOL_GOVERNANCE_TOKENS.get(slug)
        token_count = 0
        if gecko_id:
            token_count = backfill_protocol_token(slug, gecko_id, from_date)
            time.sleep(2)

        results[slug] = {"tvl_records": tvl_count, "token_records": token_count}

    return results
