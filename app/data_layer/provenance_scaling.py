"""
Provenance Scaling
===================
Expand provenance coverage to all data layer tiers.

Two provenance mechanisms:
1. State attestation (internal): SHA-256 hash of batch data at persist time.
   Called via attest_state() from each collector. Proves WHAT data was used.
2. TLSNotary proofs (external): Cryptographic proof that data came from the
   claimed API. Registered in provenance_proofs by the external prover service.
   Proves WHERE data came from.

This module:
- Registers all data layer sources in the provenance source registry
- Provides attest_data_batch() helper that collectors call after storage
- Links data rows to nearest proof via provenance_proof_id
- Updates the data catalog with provenance status

Strategy — SAMPLE PROVENANCE:
- For each data type, prove ONE representative call per cycle
- The proven call anchors the batch — same code path
- Every data table has provenance_proof_id linking to nearest proof
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from app.database import fetch_one, execute, fetch_all, fetch_one_async, fetch_all_async, execute_async

logger = logging.getLogger(__name__)

# =============================================================================
# Complete provenance source registry — 14 sources across all tiers
# =============================================================================
# Maps source_domain (used in provenance_proofs) to data types and endpoints.
# The external prover uses this registry to know what to prove.

PROVENANCE_SOURCES = {
    # ---- Existing (V7.8) ----
    "coingecko_price": {
        "provider": "coingecko",
        "endpoint": "/coins/usd-coin",
        "data_types": ["scores", "component_readings", "entity_snapshots_hourly"],
        "description": "CoinGecko coin data — anchors SII components + entity snapshots",
    },
    "defillama_tvl": {
        "provider": "defillama",
        "endpoint": "/tvl/aave",
        "data_types": ["psi_scores"],
        "description": "DeFiLlama TVL — anchors PSI collateral components",
    },
    "etherscan_holders": {
        "provider": "etherscan",
        "endpoint": "/tokenholdercount",
        "data_types": ["wallet_holdings"],
        "description": "Etherscan holder data — anchors distribution components",
    },
    "snapshot_governance": {
        "provider": "snapshot",
        "endpoint": "/graphql",
        "data_types": ["governance_proposals", "governance_voters"],
        "description": "Snapshot proposals + votes — anchors governance data",
    },

    # ---- Tier 1: Liquidity Depth ----
    "geckoterminal_dex": {
        "provider": "coingecko",
        "endpoint": "/onchain/networks/eth/tokens/{address}/pools",
        "data_types": ["liquidity_depth"],
        "description": "GeckoTerminal DEX pools — anchors DEX liquidity depth",
    },
    "coingecko_tickers": {
        "provider": "coingecko",
        "endpoint": "/coins/usd-coin/tickers",
        "data_types": ["liquidity_depth"],
        "description": "CoinGecko CEX tickers — anchors CEX liquidity depth",
    },

    # ---- Tier 2: Yield Data ----
    "defillama_yields": {
        "provider": "defillama",
        "endpoint": "/pools",
        "data_types": ["yield_snapshots"],
        "description": "DeFiLlama yield pools — anchors yield/rate data",
    },

    # ---- Tier 4: Bridge Flows ----
    "defillama_bridges": {
        "provider": "defillama",
        "endpoint": "/bridges",
        "data_types": ["bridge_flows"],
        "description": "DeFiLlama bridges — anchors bridge flow volumes",
    },

    # ---- Tier 5: Exchange Data ----
    "coingecko_exchanges": {
        "provider": "coingecko",
        "endpoint": "/exchanges/binance",
        "data_types": ["exchange_snapshots"],
        "description": "CoinGecko exchange data — anchors exchange snapshots",
    },

    # ---- Tier 7: Volatility / Peg ----
    "coingecko_market_chart": {
        "provider": "coingecko",
        "endpoint": "/coins/usd-coin/market_chart",
        "data_types": ["peg_snapshots_5m", "volatility_surfaces"],
        "description": "CoinGecko market chart — anchors 5-min peg + volatility",
    },

    # ---- Mint/Burn ----
    "etherscan_tokentx": {
        "provider": "etherscan",
        "endpoint": "/tokentx",
        "data_types": ["mint_burn_events"],
        "description": "Etherscan token transfers — anchors mint/burn events",
    },

    # ---- Contract Surveillance ----
    "etherscan_sourcecode": {
        "provider": "etherscan",
        "endpoint": "/getsourcecode",
        "data_types": ["contract_surveillance"],
        "description": "Etherscan source code — anchors contract surveillance",
    },

    # ---- Wallet Graph ----
    "blockscout_balances": {
        "provider": "blockscout",
        "endpoint": "/v2/addresses/{address}/token-balances",
        "data_types": ["wallet_holdings", "wallet_behavior_tags"],
        "description": "Blockscout token balances — anchors wallet graph data",
    },

    # ---- Tally Governance ----
    "tally_governance": {
        "provider": "tally",
        "endpoint": "/query",
        "data_types": ["governance_proposals"],
        "description": "Tally on-chain governance — anchors on-chain governance data",
    },

    # ---- CDA Issuer Documents ----
    "cda_issuer_pdf": {
        "provider": "issuer_website",
        "endpoint": "cda_source_urls.source_url",
        "data_types": ["cda_extractions"],
        "description": "CDA issuer attestation PDFs — Range header approach for large docs",
    },
}

# Reverse map: data_type → source_domains
_DATA_TYPE_TO_SOURCES: dict[str, list[str]] = {}
for _src_id, _src in PROVENANCE_SOURCES.items():
    for _dt in _src["data_types"]:
        _DATA_TYPE_TO_SOURCES.setdefault(_dt, []).append(_src_id)


# =============================================================================
# State attestation helper — call from each collector after storage
# =============================================================================

def attest_data_batch(
    data_type: str,
    records: list[dict],
    entity_id: str = None,
) -> str:
    """
    Attest a batch of data from a data layer collector.
    Calls state_attestation.attest_state() and returns the hash.

    Usage in collectors:
        from app.data_layer.provenance_scaling import attest_data_batch
        attest_data_batch("liquidity_depth", records)
    """
    try:
        from app.state_attestation import attest_state
        domain = f"data_layer:{data_type}"
        return attest_state(domain, records, entity_id=entity_id)
    except Exception as e:
        logger.warning(f"State attestation failed for {data_type}: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="data_layer_attest_data_batch_failure",
                error_message=str(e)[:500],
                cycle_phase="provenance_scaling",
            )
        except Exception:
            pass
        return ""


# =============================================================================
# Proof linking — connect data rows to nearest TLSNotary proof
# =============================================================================

async def get_nearest_proof_id(data_type: str) -> int:
    """
    Get the ID of the most recent TLSNotary proof for this data type.
    Used to set provenance_proof_id on newly stored data rows.
    """
    source_domains = _DATA_TYPE_TO_SOURCES.get(data_type, [])
    if not source_domains:
        return None

    try:
        # Find the most recent proof across all source domains for this data type
        placeholders = ",".join(["%s"] * len(source_domains))
        row = await fetch_one_async(
            f"""SELECT id FROM provenance_proofs
                WHERE source_domain IN ({placeholders})
                ORDER BY proved_at DESC LIMIT 1""",
            tuple(source_domains),
        )
        return row["id"] if row else None
    except Exception:
        return None


async def link_batch_to_proof(table_name: str, data_type: str, batch_timestamp: str = None):
    """
    Update recently stored rows in a data table to link them to the nearest proof.
    Called after a collector stores data.

    Only updates rows where provenance_proof_id IS NULL (not already linked).
    """
    proof_id = await get_nearest_proof_id(data_type)
    if not proof_id:
        return 0

    # Table → timestamp column mapping
    time_cols = {
        "liquidity_depth": "snapshot_at",
        "yield_snapshots": "snapshot_at",
        "governance_proposals": "captured_at",
        "governance_voters": "collected_at",
        "bridge_flows": "snapshot_at",
        "exchange_snapshots": "snapshot_at",
        "peg_snapshots_5m": "timestamp",
        "mint_burn_events": "collected_at",
        "entity_snapshots_hourly": "snapshot_at",
        "contract_surveillance": "scanned_at",
        "volatility_surfaces": "computed_at",
        "correlation_matrices": "computed_at",
        "incident_events": "created_at",
        "wallet_behavior_tags": "computed_at",
    }

    time_col = time_cols.get(table_name)
    if not time_col:
        return 0

    # Whitelist check
    allowed = set(time_cols.keys())
    if table_name not in allowed:
        return 0

    try:
        await execute_async(
            f"""UPDATE {table_name}
                SET provenance_proof_id = %s
                WHERE provenance_proof_id IS NULL
                  AND {time_col} > NOW() - INTERVAL '2 hours'""",
            (proof_id,),
        )
        return proof_id
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"Proof linking failed for {table_name}: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="data_layer_link_batch_to_proof_failure",
                error_message=str(e)[:500],
                cycle_phase="provenance_scaling",
            )
        except Exception:
            pass
        return 0


# =============================================================================
# Registry and catalog functions
# =============================================================================

async def get_provenance_registry() -> dict:
    """Return the full provenance source registry with current proof status."""
    registry = {}

    for source_id, source in PROVENANCE_SOURCES.items():
        latest_proof = None
        try:
            row = await fetch_one_async(
                """SELECT id, proved_at, attestation_hash
                   FROM provenance_proofs
                   WHERE source_domain = %s
                   ORDER BY proved_at DESC LIMIT 1""",
                (source_id,),
            )
            if row:
                latest_proof = {
                    "proof_id": row["id"],
                    "proved_at": row["proved_at"].isoformat() if row.get("proved_at") else None,
                    "hash": row.get("attestation_hash"),
                }
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[provenance_scaling] proof lookup failed for {source_id}: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="data_layer_get_provenance_registry_proof_lookup_failure",
                    error_message=str(e)[:500],
                    cycle_phase="provenance_scaling",
                )
            except Exception:
                pass

        # Check state attestation too
        latest_attestation = None
        for dt in source["data_types"]:
            try:
                att = await fetch_one_async(
                    """SELECT batch_hash, cycle_timestamp
                       FROM state_attestations
                       WHERE domain = %s
                       ORDER BY cycle_timestamp DESC LIMIT 1""",
                    (f"data_layer:{dt}",),
                )
                if att:
                    latest_attestation = {
                        "hash": att.get("batch_hash"),
                        "timestamp": att["cycle_timestamp"].isoformat() if att.get("cycle_timestamp") else None,
                    }
                    break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[provenance_scaling] attestation lookup failed for {dt}: {e}")
                try:
                    from app.worker import _record_cycle_error
                    _record_cycle_error(
                        error_type="data_layer_get_provenance_registry_attestation_lookup_failure",
                        error_message=str(e)[:500],
                        cycle_phase="provenance_scaling",
                    )
                except Exception:
                    pass

        registry[source_id] = {
            **source,
            "latest_proof": latest_proof,
            "latest_attestation": latest_attestation,
            "status": (
                "proven" if latest_proof
                else "attested" if latest_attestation
                else "registered"
            ),
        }

    return registry


async def get_data_type_provenance(data_type: str) -> dict:
    """Get provenance chain for a specific data type."""
    source_domains = _DATA_TYPE_TO_SOURCES.get(data_type, [])

    if not source_domains:
        return {
            "data_type": data_type,
            "provenance_status": "no_source_registered",
            "sources": [],
        }

    proofs = []
    for source_id in source_domains:
        try:
            rows = await fetch_all_async(
                """SELECT id, proved_at, attestation_hash, source_domain, source_endpoint
                   FROM provenance_proofs
                   WHERE source_domain = %s
                   ORDER BY proved_at DESC LIMIT 5""",
                (source_id,),
            )
            if rows:
                proofs.extend([dict(r) for r in rows])
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[provenance_scaling] proof query failed for {source_id}: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="data_layer_get_data_type_provenance_proof_query_failure",
                    error_message=str(e)[:500],
                    cycle_phase="provenance_scaling",
                )
            except Exception:
                pass

    # Also check state attestations
    attestations = []
    try:
        att_rows = await fetch_all_async(
            """SELECT domain, batch_hash, record_count, cycle_timestamp
               FROM state_attestations
               WHERE domain = %s
               ORDER BY cycle_timestamp DESC LIMIT 5""",
            (f"data_layer:{data_type}",),
        )
        if att_rows:
            attestations = [dict(r) for r in att_rows]
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"[provenance_scaling] attestation query failed for {data_type}: {e}")
        try:
            from app.worker import _record_cycle_error
            _record_cycle_error(
                error_type="data_layer_get_data_type_provenance_attestation_query_failure",
                error_message=str(e)[:500],
                cycle_phase="provenance_scaling",
            )
        except Exception:
            pass

    return {
        "data_type": data_type,
        "provenance_status": (
            "proven" if proofs
            else "attested" if attestations
            else "registered"
        ),
        "registered_sources": source_domains,
        "recent_proofs": proofs[:10],
        "recent_attestations": attestations[:5],
    }


async def update_catalog_provenance():
    """Update data_catalog with provenance status for each data type."""
    updated = 0

    # Get all data types from catalog
    try:
        catalog_rows = await fetch_all_async("SELECT data_type FROM data_catalog")
        if not catalog_rows:
            return
    except Exception:
        return

    for row in catalog_rows:
        data_type = row["data_type"]
        prov = await get_data_type_provenance(data_type)
        status = prov["provenance_status"]

        # Build proof details for catalog display
        proof_details = None
        if prov.get("recent_proofs"):
            p = prov["recent_proofs"][0]
            proof_details = {
                "latest_proof_hash": p.get("attestation_hash"),
                "latest_proof_at": str(p.get("proved_at")) if p.get("proved_at") else None,
                "source_domain": p.get("source_domain"),
            }
        elif prov.get("recent_attestations"):
            a = prov["recent_attestations"][0]
            proof_details = {
                "latest_attestation_hash": a.get("batch_hash"),
                "latest_attestation_at": str(a.get("cycle_timestamp")) if a.get("cycle_timestamp") else None,
                "record_count": a.get("record_count"),
            }

        try:
            await execute_async(
                """UPDATE data_catalog
                   SET provenance_status = %s,
                       schema_info = COALESCE(schema_info, '{}'::jsonb) || %s::jsonb,
                       updated_at = NOW()
                   WHERE data_type = %s""",
                (
                    status,
                    json.dumps({"provenance": proof_details}) if proof_details else "{}",
                    data_type,
                ),
            )
            updated += 1
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Catalog provenance update failed for {data_type}: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="data_layer_update_catalog_provenance_update_failure",
                    error_message=str(e)[:500],
                    cycle_phase="provenance_scaling",
                )
            except Exception:
                pass

    logger.info(f"Data catalog provenance updated: {updated} data types")


async def run_provenance_linking():
    """
    Link all recent unlinked data rows to their nearest proofs.
    Called at end of enrichment cycle.
    """
    tables = [
        ("liquidity_depth", "liquidity_depth"),
        ("yield_snapshots", "yield_snapshots"),
        ("governance_proposals", "governance_proposals"),
        ("bridge_flows", "bridge_flows"),
        ("exchange_snapshots", "exchange_snapshots"),
        ("peg_snapshots_5m", "peg_snapshots_5m"),
        ("mint_burn_events", "mint_burn_events"),
        ("entity_snapshots_hourly", "entity_snapshots_hourly"),
        ("contract_surveillance", "contract_surveillance"),
        ("volatility_surfaces", "volatility_surfaces"),
        ("correlation_matrices", "correlation_matrices"),
        ("wallet_behavior_tags", "wallet_behavior_tags"),
    ]

    linked = 0
    for table_name, data_type in tables:
        try:
            result = await link_batch_to_proof(table_name, data_type)
            if result:
                linked += 1
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[provenance_scaling] proof linking failed for {table_name}: {e}")
            try:
                from app.worker import _record_cycle_error
                _record_cycle_error(
                    error_type="data_layer_run_provenance_linking_failure",
                    error_message=str(e)[:500],
                    cycle_phase="provenance_scaling",
                )
            except Exception:
                pass

    logger.info(f"Provenance linking complete: {linked}/{len(tables)} tables linked")
    return linked


async def get_coverage_report() -> dict:
    """
    Report: how many data sources are proven out of how many total?
    """
    registry = await get_provenance_registry()

    total = len(registry)
    proven = sum(1 for s in registry.values() if s["status"] == "proven")
    attested = sum(1 for s in registry.values() if s["status"] == "attested")
    registered_only = sum(1 for s in registry.values() if s["status"] == "registered")

    # Data type coverage
    all_data_types = set()
    for src in registry.values():
        all_data_types.update(src["data_types"])

    covered_types = set()
    for src in registry.values():
        if src["status"] in ("proven", "attested"):
            covered_types.update(src["data_types"])

    return {
        "sources": {
            "total": total,
            "proven": proven,
            "attested": attested,
            "registered_only": registered_only,
            "coverage_pct": round((proven + attested) / total * 100, 1) if total > 0 else 0,
        },
        "data_types": {
            "total": len(all_data_types),
            "covered": len(covered_types),
            "uncovered": sorted(all_data_types - covered_types),
            "coverage_pct": round(len(covered_types) / len(all_data_types) * 100, 1) if all_data_types else 0,
        },
        "by_source": {
            source_id: {
                "status": source["status"],
                "provider": source["provider"],
                "data_types": source["data_types"],
            }
            for source_id, source in registry.items()
        },
    }
