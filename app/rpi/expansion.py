"""
RPI Auto-Expansion Pipeline
==============================
Discovers new protocols and expands RPI coverage beyond the initial 13.
Follows the same pattern as PSI auto-expansion (discover → enrich → enable).

Uses rpi_protocol_config as the central registry, replacing hardcoded dicts.
"""

import logging
import time

import requests

from app.database import execute, fetch_one, fetch_all
from app.index_definitions.rpi_v2 import RPI_TARGET_PROTOCOLS
from app.rpi.snapshot_collector import SNAPSHOT_SPACES
from app.rpi.tally_collector import TALLY_ORGS
from app.rpi.parameter_collector import PROTOCOL_CONFIGS
from app.rpi.forum_scraper import RPI_FORUMS
from app.rpi.docs_scorer import PROTOCOL_DOCS

logger = logging.getLogger(__name__)

DEFILLAMA_BASE = "https://api.llama.fi"
SNAPSHOT_API = "https://hub.snapshot.org/graphql"

# Minimum TVL to consider a protocol for RPI expansion
MIN_TVL_USD = 50_000_000


def seed_initial_config():
    """Populate rpi_protocol_config from the hardcoded Phase 1 dicts.

    Idempotent — uses ON CONFLICT to avoid duplicates.
    """
    seeded = 0
    for slug in RPI_TARGET_PROTOCOLS:
        snapshot_space = SNAPSHOT_SPACES.get(slug)
        tally_org = TALLY_ORGS.get(slug)
        forum_url = RPI_FORUMS.get(slug, {}).get("base_url")
        docs_url = PROTOCOL_DOCS.get(slug)

        admin_contracts = None
        if slug in PROTOCOL_CONFIGS:
            import json
            admin_contracts = json.dumps({"ethereum": PROTOCOL_CONFIGS[slug]["contracts"]})

        # Get name from PSI scores
        row = fetch_one(
            "SELECT protocol_name FROM psi_scores WHERE protocol_slug = %s ORDER BY computed_at DESC LIMIT 1",
            (slug,),
        )
        name = row["protocol_name"] if row and row.get("protocol_name") else slug.replace("-", " ").title()

        try:
            execute("""
                INSERT INTO rpi_protocol_config
                    (protocol_slug, protocol_name, snapshot_space, tally_org_id,
                     governance_forum_url, docs_url, admin_contracts,
                     discovery_source, coverage_level, enabled)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'manual', 'full', TRUE)
                ON CONFLICT (protocol_slug) DO UPDATE SET
                    protocol_name = COALESCE(EXCLUDED.protocol_name, rpi_protocol_config.protocol_name),
                    snapshot_space = COALESCE(EXCLUDED.snapshot_space, rpi_protocol_config.snapshot_space),
                    tally_org_id = COALESCE(EXCLUDED.tally_org_id, rpi_protocol_config.tally_org_id),
                    governance_forum_url = COALESCE(EXCLUDED.governance_forum_url, rpi_protocol_config.governance_forum_url),
                    docs_url = COALESCE(EXCLUDED.docs_url, rpi_protocol_config.docs_url),
                    admin_contracts = COALESCE(EXCLUDED.admin_contracts, rpi_protocol_config.admin_contracts),
                    updated_at = NOW()
            """, (slug, name, snapshot_space, tally_org, forum_url, docs_url, admin_contracts))
            seeded += 1
        except Exception as e:
            logger.debug(f"Failed to seed config for {slug}: {e}")

    logger.info(f"RPI config: seeded {seeded} protocols")
    return seeded


def _search_snapshot_space(protocol_name: str) -> str | None:
    """Search Snapshot for a space matching the protocol name."""
    time.sleep(0.5)
    query = """
    query {
      spaces(first: 5, where: {name_contains: "%s"}) {
        id
        name
        members
      }
    }
    """ % protocol_name.replace('"', '')

    try:
        resp = requests.post(SNAPSHOT_API, json={"query": query}, timeout=10)
        if resp.status_code == 200:
            spaces = resp.json().get("data", {}).get("spaces", [])
            if spaces:
                # Return the space with most members
                spaces.sort(key=lambda s: s.get("members", 0), reverse=True)
                return spaces[0].get("id")
    except Exception as e:
        logger.debug(f"Snapshot search failed for {protocol_name}: {e}")
    return None


def discover_new_protocols() -> int:
    """Discover new protocols from DeFiLlama that qualify for RPI coverage.

    Criteria:
    - TVL > $50M
    - Has governance token (implies on-chain governance)
    - Not already in rpi_protocol_config
    """
    # Get existing slugs
    existing = set()
    try:
        rows = fetch_all("SELECT protocol_slug FROM rpi_protocol_config")
        existing = {r["protocol_slug"] for r in rows}
    except Exception:
        existing = set(RPI_TARGET_PROTOCOLS)

    # Also include PSI backlog promoted protocols
    try:
        backlog_rows = fetch_all("""
            SELECT slug, name, gecko_id, snapshot_space, main_contract
            FROM protocol_backlog
            WHERE enrichment_status IN ('promoted', 'scored', 'ready')
              AND stablecoin_exposure_usd >= %s
        """, (MIN_TVL_USD,))
    except Exception:
        backlog_rows = []

    discovered = 0
    for row in backlog_rows:
        slug = row["slug"]
        if slug in existing:
            continue

        name = row.get("name", slug)
        snapshot_space = row.get("snapshot_space")
        gecko_id = row.get("gecko_id")

        # Try to find Snapshot space if not known
        if not snapshot_space and name:
            snapshot_space = _search_snapshot_space(name)

        try:
            execute("""
                INSERT INTO rpi_protocol_config
                    (protocol_slug, protocol_name, snapshot_space,
                     discovery_source, coverage_level, enabled)
                VALUES (%s, %s, %s, 'auto-expansion', 'partial', TRUE)
                ON CONFLICT (protocol_slug) DO NOTHING
            """, (slug, name, snapshot_space))
            discovered += 1
            logger.info(f"RPI expansion: discovered {slug} (snapshot={snapshot_space})")
        except Exception as e:
            logger.debug(f"Failed to add {slug} to RPI config: {e}")

    # Also try direct DeFiLlama top protocols
    try:
        time.sleep(1)
        resp = requests.get(f"{DEFILLAMA_BASE}/protocols", timeout=30)
        if resp.status_code == 200:
            protocols = resp.json()
            # Sort by TVL descending
            protocols.sort(key=lambda p: p.get("tvl", 0) or 0, reverse=True)

            for p in protocols[:100]:  # top 100 by TVL
                slug = p.get("slug", "")
                tvl = p.get("tvl", 0) or 0
                if not slug or tvl < MIN_TVL_USD or slug in existing:
                    continue

                name = p.get("name", slug)
                gecko_id = p.get("gecko_id")
                governance_id = p.get("governanceID")

                # Extract Snapshot space from governanceID
                snapshot_space = None
                if governance_id:
                    if isinstance(governance_id, list):
                        for gid in governance_id:
                            if isinstance(gid, str) and ".eth" in gid:
                                snapshot_space = gid
                                break
                    elif isinstance(governance_id, str):
                        snapshot_space = governance_id

                # Only add if there's some governance signal
                if not snapshot_space and not gecko_id:
                    continue

                try:
                    execute("""
                        INSERT INTO rpi_protocol_config
                            (protocol_slug, protocol_name, snapshot_space,
                             discovery_source, coverage_level, enabled)
                        VALUES (%s, %s, %s, 'defillama', 'minimal', TRUE)
                        ON CONFLICT (protocol_slug) DO NOTHING
                    """, (slug, name, snapshot_space))
                    discovered += 1
                    existing.add(slug)
                    logger.info(f"RPI expansion: discovered {slug} from DeFiLlama (TVL=${tvl:,.0f})")
                except Exception as e:
                    logger.debug(f"Failed to add DeFiLlama protocol {slug}: {e}")

    except Exception as e:
        logger.warning(f"DeFiLlama protocols fetch failed: {e}")

    logger.info(f"RPI expansion: {discovered} new protocols discovered")
    return discovered


def get_enabled_protocols() -> list[dict]:
    """Get all enabled protocols from rpi_protocol_config."""
    try:
        return fetch_all("""
            SELECT protocol_slug, protocol_name, snapshot_space, tally_org_id,
                   governance_forum_url, docs_url, admin_contracts,
                   coverage_level
            FROM rpi_protocol_config
            WHERE enabled = TRUE
            ORDER BY coverage_level, protocol_slug
        """)
    except Exception:
        # Fallback to hardcoded list
        return [{"protocol_slug": s} for s in RPI_TARGET_PROTOCOLS]


def run_expansion_pipeline() -> dict:
    """Run the full expansion pipeline: seed → discover → report."""
    seeded = seed_initial_config()
    discovered = discover_new_protocols()

    total = fetch_one("SELECT COUNT(*) AS cnt FROM rpi_protocol_config WHERE enabled = TRUE")
    total_count = total["cnt"] if total else 0

    summary = {
        "seeded": seeded,
        "discovered": discovered,
        "total_enabled": total_count,
    }
    logger.info(f"RPI expansion pipeline: {summary}")
    return summary
