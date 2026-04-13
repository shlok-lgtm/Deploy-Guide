"""
Basis Protocol — Replit Entry Point
=====================================
Runs the API server + background worker in a single process.
Replit runs `python main.py` — this handles everything.
"""

import asyncio
import threading
import time
import os
import logging
import signal
import sys
from datetime import datetime, timezone

import subprocess

import uvicorn

from app.database import init_pool, close_pool, health_check as db_health_check
from app.server import app

# Module-level keeper process ref for watchdog access
_keeper_process = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("main")

# Worker interval (minutes) — how often to re-score all stablecoins
WORKER_INTERVAL = int(os.environ.get("COLLECTION_INTERVAL", "60"))


def free_port(port: int) -> None:
    """Kill any process holding the given port by reading /proc/net/tcp."""
    try:
        # Find the inode(s) listening on this port
        target_hex = f"{port:04X}"
        inodes = set()
        for proto_file in ("/proc/net/tcp", "/proc/net/tcp6"):
            try:
                with open(proto_file) as f:
                    for line in f.readlines()[1:]:
                        parts = line.split()
                        if len(parts) < 10:
                            continue
                        local_addr = parts[1]
                        state = parts[3]
                        inode = parts[9]
                        local_port_hex = local_addr.split(":")[1]
                        if local_port_hex.upper() == target_hex and state == "0A":
                            inodes.add(inode)
            except FileNotFoundError:
                pass

        if not inodes:
            return

        # Find PIDs that own those inodes
        own_pid = str(os.getpid())
        for pid_dir in os.listdir("/proc"):
            if not pid_dir.isdigit() or pid_dir == own_pid:
                continue
            fd_dir = f"/proc/{pid_dir}/fd"
            try:
                for fd in os.listdir(fd_dir):
                    try:
                        link = os.readlink(f"{fd_dir}/{fd}")
                        if "socket:[" in link:
                            inode = link.split("[")[1].rstrip("]")
                            if inode in inodes:
                                logger.info(f"Killing stale process {pid_dir} holding port {port}")
                                os.kill(int(pid_dir), signal.SIGKILL)
                                time.sleep(0.5)
                                break
                    except (OSError, PermissionError):
                        pass
            except (OSError, PermissionError):
                pass
    except Exception as e:
        logger.warning(f"free_port({port}) failed: {e}")


def _forward_keeper_logs(process):
    """Read keeper stdout and forward to Python logger."""
    try:
        for line in iter(process.stdout.readline, b""):
            line_str = line.decode().strip()
            if line_str:
                logger.info(f"[keeper] {line_str}")
    except Exception:
        pass


def run_worker_loop():
    """Background thread: runs scoring cycle on an interval."""
    # Wait for server to be up
    time.sleep(10)
    logger.info(f"Worker thread started (interval: {WORKER_INTERVAL} min)")

    # One-time wallet seeding: populate on fresh deployments where tables are empty
    try:
        from app.database import fetch_one
        result = fetch_one("SELECT COUNT(*) AS c FROM wallet_graph.wallets")
        count = result["c"] if result else 0
        if count == 0:
            logger.info("Wallet tables empty — running initial seeding...")
            from app.indexer.pipeline import run_pipeline
            asyncio.run(run_pipeline())  # uses INDEXER_HOLDERS_PER_COIN env var (default 5000)
            logger.info("Initial wallet seeding complete")
        else:
            logger.info(f"Wallets already seeded ({count} wallets) — skipping initial seed")
    except Exception as e:
        logger.warning(f"Wallet seeding skipped: {e}")

    # Ensure email alert channel is configured
    try:
        from app.database import fetch_one as _fone, execute as _exec
        existing = _fone("SELECT id FROM ops_alert_config WHERE channel = 'email'")
        if not existing:
            _exec(
                "INSERT INTO ops_alert_config (channel, config, alert_types, enabled) VALUES (%s, %s, %s, TRUE)",
                ("email", "{}", ["health_failure", "engagement_response", "state_growth"]),
            )
            logger.info("Email alert channel configured")
    except Exception as e:
        logger.debug(f"Alert config seed skipped: {e}")

    from app.worker import run_scoring_cycle

    cda_interval_hours = int(os.environ.get("CDA_COLLECTION_INTERVAL_HOURS", "24"))
    last_cda_at = 0  # Run CDA on first cycle
    last_expansion_at = 0  # Run PSI expansion on first cycle
    last_wallet_expansion_at = 0  # Run wallet expansion on first cycle
    # Seed edge build timestamp from DB so restarts don't re-trigger a fresh build
    last_edge_build_at = 0
    try:
        from app.database import fetch_one as _fone
        _edge_row = _fone("SELECT EXTRACT(EPOCH FROM MAX(last_built_at)) AS ts FROM wallet_graph.edge_build_status")
        if _edge_row and _edge_row["ts"]:
            last_edge_build_at = float(_edge_row["ts"])
            _hours_ago = (time.time() - last_edge_build_at) / 3600
            logger.info(f"Edge build last ran {_hours_ago:.1f}h ago — {'will rebuild' if _hours_ago >= 10 else 'fresh, skipping'}")
    except Exception as e:
        logger.warning(f"Could not read edge build timestamp: {e}")

    while True:
        # CDA collection before scoring so fresh off-chain data is available
        hours_since_cda = (time.time() - last_cda_at) / 3600
        if hours_since_cda >= cda_interval_hours:
            try:
                logger.info("Running CDA collection pipeline...")
                from app.services.cda_collector import run_collection
                asyncio.run(run_collection())
                last_cda_at = time.time()
                logger.info("CDA collection complete")
                # Attest CDA extractions
                try:
                    from app.state_attestation import attest_state
                    from app.database import fetch_all as _fa
                    cda_rows = _fa("SELECT asset_symbol, field_name, extracted_value, source_url FROM cda_vendor_extractions WHERE extracted_at > NOW() - INTERVAL '2 hours'")
                    if cda_rows:
                        attest_state("cda_extractions", [dict(r) for r in cda_rows])
                except Exception as ae:
                    logger.debug(f"CDA attestation skipped: {ae}")
            except Exception as e:
                logger.warning(f"CDA collection failed: {e}")

        try:
            asyncio.run(run_scoring_cycle())
        except Exception as e:
            logger.error(f"Worker cycle error: {e}")

        logger.error("=== SCORING CYCLE COMPLETE ===")

        # Wallet batch re-indexing — rescan stale wallets every cycle
        # Uses addresstokenbalance (Etherscan V2) or Blockscout per-address API
        try:
            from app.indexer.pipeline import run_pipeline_batch
            logger.info("Running wallet batch re-index (500 stalest wallets)...")
            reindex_result = run_pipeline_batch(batch_size=500)
            logger.info(
                f"Wallet re-index complete: {reindex_result.get('processed', 0)} processed, "
                f"{reindex_result.get('scored', 0)} scored, "
                f"{reindex_result.get('errors', 0)} errors, "
                f"{reindex_result.get('remaining', '?')} remaining"
            )
            # Attest wallet batch
            try:
                from app.state_attestation import attest_state
                if reindex_result.get('processed', 0) > 0:
                    attest_state("wallets", [{"cycle": "batch_reindex", "processed": reindex_result.get('processed', 0), "scored": reindex_result.get('scored', 0)}])
            except Exception as ae:
                logger.debug(f"Wallet attestation skipped: {ae}")
        except Exception as e:
            logger.warning(f"Wallet batch re-index failed: {e}")

        # PSI scoring — uses DeFiLlama only, no explorer API budget
        # Sleep to avoid API contention with SII cycle
        time.sleep(60)
        try:
            from app.collectors.psi_collector import run_psi_scoring
            logger.info("Running PSI scoring cycle...")
            psi_results = run_psi_scoring()
            logger.info(f"PSI scoring complete: {len(psi_results)} protocols scored")
            # Attest PSI scores
            try:
                from app.state_attestation import attest_state
                if psi_results:
                    attest_state("psi_components", [{"slug": r.get("protocol_slug", ""), "score": r.get("overall_score")} for r in psi_results if isinstance(r, dict)])
            except Exception as ae:
                logger.debug(f"PSI attestation skipped: {ae}")
        except Exception as e:
            logger.warning(f"PSI scoring failed: {e}")

        # PSI expansion pipeline — daily gate (discover → enrich → promote)
        hours_since_expansion = (time.time() - last_expansion_at) / 3600
        if hours_since_expansion >= 24:
            try:
                from app.collectors.psi_collector import (
                    collect_collateral_exposure,
                    sync_collateral_to_backlog,
                    discover_protocols,
                    enrich_protocol_backlog,
                    promote_eligible_protocols,
                )
                logger.info("Running PSI expansion pipeline...")
                collect_collateral_exposure()
                synced = sync_collateral_to_backlog()
                discovered = discover_protocols()
                enriched = enrich_protocol_backlog()
                promoted = promote_eligible_protocols()
                last_expansion_at = time.time()
                logger.info(
                    f"PSI expansion: {synced} stablecoins synced, {discovered} discovered, "
                    f"{enriched} enriched, {promoted} promoted"
                )
                # Attest PSI discoveries
                try:
                    from app.state_attestation import attest_state
                    if discovered or promoted:
                        attest_state("psi_discoveries", [{"synced": synced, "discovered": discovered, "enriched": enriched, "promoted": promoted}])
                except Exception as ae:
                    logger.debug(f"PSI discovery attestation skipped: {ae}")
            except Exception as e:
                logger.warning(f"PSI expansion pipeline failed: {e}")

        # Wallet expansion — daily gate (seed new addresses from under-covered stablecoins)
        hours_since_wallet_expansion = (time.time() - last_wallet_expansion_at) / 3600
        if hours_since_wallet_expansion >= 24:
            last_wallet_expansion_at = time.time()  # Set unconditionally — prevent retry storms on failure

            try:
                from app.indexer.expander import run_wallet_expansion
                logger.info("Running wallet expansion pipeline...")
                expansion_result = asyncio.run(run_wallet_expansion(max_etherscan_calls=50))
                logger.info(
                    f"Wallet expansion complete: {expansion_result.get('new_wallets_seeded', 0)} seeded, "
                    f"{expansion_result.get('etherscan_calls_used', 0)} Etherscan calls used"
                )
            except Exception as e:
                logger.warning(f"Wallet expansion failed: {e}")

            # Profile rebuild — piggyback on same daily gate so new wallets get profiled
            try:
                from app.indexer.profiles import rebuild_all_profiles
                logger.info("Rebuilding wallet profiles...")
                profile_result = rebuild_all_profiles()
                logger.info(
                    f"Profile rebuild complete: {profile_result.get('built', 0)} built, "
                    f"{profile_result.get('errors', 0)} errors out of {profile_result.get('total', 0)} addresses"
                )
                # Attest profiles
                try:
                    from app.state_attestation import attest_state
                    if profile_result.get('built', 0) > 0:
                        attest_state("wallet_profiles", [{"built": profile_result.get('built', 0), "total": profile_result.get('total', 0)}])
                except Exception as ae:
                    logger.debug(f"Profile attestation skipped: {ae}")
            except Exception as e:
                logger.warning(f"Profile rebuild failed: {e}")

        # Verification agent cycle — runs after every scoring cycle
        try:
            from app.agent.watcher import run_agent_cycle
            result = run_agent_cycle()
            if result:
                logger.info(f"Agent cycle: {result.get('assessments', 0)} assessments")
                # Attest actor classifications
                try:
                    from app.state_attestation import attest_state
                    if result.get('assessments', 0) > 0:
                        attest_state("actors", [{"assessments": result.get('assessments', 0), "severities": result.get('severities', {})}])
                except Exception as ae:
                    logger.debug(f"Actor attestation skipped: {ae}")
        except Exception as e:
            logger.error(f"Agent cycle error: {e}")

        # =====================================================================
        # Universal Data Layer collectors — guaranteed to run every cycle
        # =====================================================================
        logger.error("=== DATA LAYER SECTION REACHED ===")

        async def _run_data_layer_collectors():
            """Run all data layer collectors in one async context."""
            results = {}

            # --- Every-cycle collectors (hourly) ---
            for name, coro_fn in [
                ("liquidity_depth", lambda: __import__('app.data_layer.liquidity_collector', fromlist=['run_liquidity_collection']).run_liquidity_collection()),
                ("exchange_snapshots", lambda: __import__('app.data_layer.exchange_collector', fromlist=['run_exchange_collection']).run_exchange_collection()),
                ("entity_snapshots", lambda: __import__('app.data_layer.entity_snapshots', fromlist=['run_entity_snapshots']).run_entity_snapshots()),
            ]:
                try:
                    r = await coro_fn()
                    results[name] = r
                    logger.error(f"=== {name} COMPLETE: {r} ===")
                except Exception as e:
                    logger.error(f"=== {name} FAILED: {type(e).__name__}: {e} ===")
                    results[name] = {"error": str(e)}

            # --- Daily-gated collectors ---
            from app.database import fetch_one as _dl_fetch

            daily_collectors = [
                ("yield_snapshots", "SELECT MAX(snapshot_at) AS latest FROM yield_snapshots",
                 lambda: __import__('app.data_layer.yield_collector', fromlist=['run_yield_collection']).run_yield_collection()),
                ("bridge_flows", "SELECT MAX(snapshot_at) AS latest FROM bridge_flows",
                 lambda: __import__('app.data_layer.bridge_flow_collector', fromlist=['run_bridge_flow_collection']).run_bridge_flow_collection()),
                ("peg_5m", "SELECT MAX(timestamp) AS latest FROM peg_snapshots_5m",
                 lambda: __import__('app.data_layer.peg_monitor', fromlist=['run_peg_monitoring']).run_peg_monitoring()),
                ("market_chart", "SELECT MAX(timestamp) AS latest FROM market_chart_history",
                 lambda: __import__('app.data_layer.market_chart_backfill', fromlist=['run_market_chart_backfill']).run_market_chart_backfill(backfill_days=90)),
                ("governance", "SELECT MAX(collected_at) AS latest FROM governance_proposals WHERE source = 'snapshot'",
                 lambda: __import__('app.data_layer.governance_collector', fromlist=['run_governance_collection']).run_governance_collection()),
                ("mint_burn", "SELECT MAX(collected_at) AS latest FROM mint_burn_events",
                 lambda: __import__('app.data_layer.mint_burn_collector', fromlist=['run_mint_burn_collection']).run_mint_burn_collection()),
                ("holder_discovery", "SELECT MAX(created_at) AS latest FROM wallet_graph.wallets WHERE source = 'holder_discovery'",
                 lambda: __import__('app.data_layer.holder_discovery', fromlist=['run_holder_discovery']).run_holder_discovery()),
            ]

            for name, gate_query, coro_fn in daily_collectors:
                try:
                    # Check gate
                    should_run = True
                    try:
                        row = _dl_fetch(gate_query)
                        if row:
                            latest = None
                            for v in row.values():
                                if v is not None:
                                    latest = v
                                    break
                            if latest and hasattr(latest, 'tzinfo'):
                                if latest.tzinfo is None:
                                    latest = latest.replace(tzinfo=timezone.utc)
                                age_h = (datetime.now(timezone.utc) - latest).total_seconds() / 3600
                                if age_h < 20:
                                    should_run = False
                                    logger.info(f"  {name}: skipped (last ran {age_h:.1f}h ago)")
                    except Exception as gate_err:
                        logger.debug(f"  {name}: gate check failed ({gate_err}), running anyway")

                    if should_run:
                        r = await coro_fn()
                        results[name] = r
                        logger.info(f"  {name}: {r}")
                except Exception as e:
                    logger.error(f"  {name} FAILED: {type(e).__name__}: {e}")
                    results[name] = {"error": str(e)}

            # --- Sync computed collectors ---
            try:
                from app.data_layer.correlation_engine import run_correlation_computation
                r = run_correlation_computation()
                results["correlation"] = r
                logger.info(f"  correlation: {r}")
            except Exception as e:
                logger.error(f"  correlation FAILED: {type(e).__name__}: {e}")

            try:
                from app.data_layer.catalog import update_catalog
                update_catalog()
            except Exception as e:
                logger.debug(f"  catalog update failed: {e}")

            try:
                from app.api_usage_tracker import flush
                flush()
            except Exception:
                pass

            return results

        try:
            dl_results = asyncio.run(_run_data_layer_collectors())
            logger.error(f"=== DATA LAYER COMPLETE: {len(dl_results)} collectors ran: {dl_results} ===")
        except Exception as e:
            logger.error(f"=== DATA LAYER ASYNCIO.RUN FAILED: {type(e).__name__}: {e} ===")
            import traceback
            logger.error(traceback.format_exc())
            import traceback
            logger.error(traceback.format_exc())

        # Treasury flow detection — runs every cycle, minimal API budget
        try:
            from app.collectors.treasury_flows import collect_treasury_events
            logger.info("Running treasury flow detection...")
            treasury_events = asyncio.run(collect_treasury_events())
            logger.info(f"Treasury flow detection: {len(treasury_events)} events")
        except Exception as e:
            logger.warning(f"Treasury flow detection failed: {e}")

        # Edge building — time-based gate (~10 hours) instead of cycle counter.
        # Uses DB staleness so container restarts don't reset the schedule.
        hours_since_edge_build = (time.time() - last_edge_build_at) / 3600
        edge_stale = hours_since_edge_build >= 10
        if edge_stale:
            for edge_chain in ["ethereum", "base", "arbitrum", "solana"]:
                try:
                    from app.indexer.edges import run_edge_builder
                    logger.info(f"Running edge builder for {edge_chain} (top 200 unbuilt wallets by value)...")
                    edge_result = asyncio.run(run_edge_builder(max_wallets=200, priority="value", chain=edge_chain))
                    logger.info(
                        f"Edge builder ({edge_chain}) complete: {edge_result.get('wallets_processed', 0)} wallets, "
                        f"{edge_result.get('total_edges_created', 0)} edges"
                    )
                    # Attest edges for this chain
                    try:
                        from app.state_attestation import attest_state
                        if edge_result.get('total_edges_created', 0) > 0:
                            attest_state("edges", [{"chain": edge_chain, "wallets": edge_result.get('wallets_processed', 0), "edges": edge_result.get('total_edges_created', 0)}])
                    except Exception as ae:
                        logger.debug(f"Edge attestation skipped for {edge_chain}: {ae}")
                except Exception as e:
                    logger.warning(f"Edge building failed for {edge_chain}: {e}")

            last_edge_build_at = time.time()

            # Decay + prune after edge building
            try:
                from app.indexer.edges import decay_edges, prune_stale_edges
                decay_result = decay_edges()
                logger.info(f"Edge decay: {decay_result.get('edges_decayed', 0)} edges recalculated")
                prune_result = prune_stale_edges()
                logger.info(f"Edge prune: {prune_result.get('edges_archived', 0)} archived")
            except Exception as e:
                logger.warning(f"Edge decay/prune failed: {e}")

        # Health sweep — run after every scoring cycle, alert on transitions
        try:
            from app.ops.tools.health_checker import run_all_checks
            logger.info("Running health sweep...")
            health_results = run_all_checks()
            healthy_count = sum(1 for r in health_results if r.get("status") == "healthy")
            total_count = len(health_results)
            logger.info(f"Health sweep: {healthy_count}/{total_count} healthy")

            failures = [r for r in health_results if r.get("status") in ("degraded", "down")]
            if failures:
                try:
                    from app.ops.tools.alerter import check_and_alert_health
                    _aloop = asyncio.new_event_loop()
                    _aloop.run_until_complete(check_and_alert_health(health_results))
                    _aloop.close()
                    logger.info(f"Health alerts dispatched for {len(failures)} failing system(s)")
                except Exception as alert_err:
                    logger.warning(f"Health alert dispatch failed: {alert_err}")
        except Exception as e:
            logger.warning(f"Health sweep failed: {e}")

        # Daily state growth check (midnight UTC hour)
        try:
            from datetime import datetime as _dt, timezone as _tz
            current_hour = _dt.now(_tz.utc).hour
            if current_hour == 0:
                from app.ops.tools.health_checker import _safe_fetch_one
                stale_tables = []
                for tname, sql in [
                    ("scores", "SELECT MAX(computed_at) as ts FROM scores"),
                    ("psi_scores", "SELECT MAX(computed_at) as ts FROM psi_scores"),
                    ("wallet_risk_scores", "SELECT MAX(computed_at) as ts FROM wallet_graph.wallet_risk_scores"),
                    ("wallet_edges", "SELECT MAX(created_at) as ts FROM wallet_graph.wallet_edges"),
                    ("cda_extractions", "SELECT MAX(extracted_at) as ts FROM cda_vendor_extractions"),
                    ("assessment_events", "SELECT MAX(created_at) as ts FROM assessment_events"),
                ]:
                    row = _safe_fetch_one(sql)
                    if row and row.get("ts"):
                        ts = row["ts"]
                        if ts.tzinfo is None:
                            ts = ts.replace(tzinfo=_tz.utc)
                        age_h = (_dt.now(_tz.utc) - ts).total_seconds() / 3600
                        if age_h > 48:
                            stale_tables.append(f"{tname}: {age_h:.0f}h stale")

                if stale_tables:
                    from app.ops.tools.alerter import send_alert
                    msg = "STATE GROWTH WARNING\n\n"
                    msg += "The following tables haven't updated in 48+ hours:\n\n"
                    msg += "\n".join(f"  - {t}" for t in stale_tables)
                    msg += "\n\nThis means data is not accumulating. Check worker logs."
                    _aloop = asyncio.new_event_loop()
                    _aloop.run_until_complete(send_alert("state_growth", msg, {"stale_tables": stale_tables}))
                    _aloop.close()
                    logger.warning(f"State growth alert: {len(stale_tables)} stale tables")
                else:
                    logger.info("Daily state growth check: all tables fresh")
        except Exception as e:
            logger.warning(f"Daily state growth check failed: {e}")

        # Keeper watchdog — restart if it died
        global _keeper_process
        if _keeper_process is not None:
            poll = _keeper_process.poll()
            if poll is not None:
                try:
                    stdout = _keeper_process.stdout.read().decode() if _keeper_process.stdout else ""
                    if stdout:
                        logger.warning(f"Keeper died (exit code {poll}), last output: {stdout[-500:]}")
                except Exception:
                    pass
                logger.warning(f"Keeper process died (exit code {poll}) — restarting...")
                try:
                    keeper_dir = os.path.join(os.path.dirname(__file__), "keeper")
                    _keeper_process = subprocess.Popen(
                        ["npx", "tsx", "index.ts"],
                        cwd=keeper_dir,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                    )
                    logger.info(f"Keeper restarted (new PID {_keeper_process.pid})")
                    keeper_log_thread = threading.Thread(
                        target=_forward_keeper_logs, args=(_keeper_process,), daemon=True
                    )
                    keeper_log_thread.start()
                except Exception as e:
                    logger.error(f"Failed to restart keeper: {e}")

        logger.info(f"Worker sleeping {WORKER_INTERVAL} minutes...")
        time.sleep(WORKER_INTERVAL * 60)


def run_migrations():
    """Apply database migrations if not already applied."""
    from app.database import fetch_one, run_migration, get_conn

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '001_initial_schema'")
        if result:
            logger.info("Migration 001_initial_schema already applied ✓")
        else:
            raise Exception("not applied")
    except Exception:
        logger.info("Applying initial migration...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "001_initial_schema.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Initial migration applied successfully ✓")
            else:
                logger.error("Failed to apply initial migration")
                return
        else:
            logger.warning(f"Migration file not found: {migration_path}")
            return

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '002_import_governance'")
        if result:
            logger.info("Migration 002_import_governance already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 002: import governance data...")
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "migration_002",
            os.path.join(os.path.dirname(__file__), "migrations", "002_import_governance.py")
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        success = mod.run(get_conn)
        if success:
            logger.info("Migration 002_import_governance applied ✓")
        else:
            logger.warning("Migration 002_import_governance had issues")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '006_add_usd1'")
        if result:
            logger.info("Migration 006_add_usd1 already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 006: add USD1 stablecoin...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "006_add_usd1.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 006_add_usd1 applied ✓")
            else:
                logger.error("Failed to apply migration 006_add_usd1")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '007_wallet_graph'")
        if result:
            logger.info("Migration 007_wallet_graph already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 007: wallet graph schema...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "007_wallet_graph.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 007_wallet_graph applied ✓")
            else:
                logger.error("Failed to apply migration 007_wallet_graph")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '011_token_type_column'")
        if result:
            logger.info("Migration 011_token_type_column already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 011: token_type column on unscored_assets...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "011_token_type_column.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 011_token_type_column applied ✓")
            else:
                logger.error("Failed to apply migration 011_token_type_column")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '012_widen_symbol_columns'")
        if result:
            logger.info("Migration 012_widen_symbol_columns already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 012: widen symbol/dominant_asset columns to VARCHAR(50)...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "012_widen_symbol_columns.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 012_widen_symbol_columns applied ✓")
            else:
                logger.error("Failed to apply migration 012_widen_symbol_columns")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '013_api_usage'")
        if result:
            logger.info("Migration 013_api_usage already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 013: API usage tracking tables...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "013_api_usage.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 013_api_usage applied ✓")
            else:
                logger.error("Failed to apply migration 013_api_usage")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    try:
        result = fetch_one("SELECT 1 FROM migrations WHERE name = '014_assessment_events'")
        if result:
            logger.info("Migration 014_assessment_events already applied ✓")
    except Exception:
        result = None

    if not result:
        logger.info("Applying migration 014: assessment events + daily pulses...")
        migration_path = os.path.join(os.path.dirname(__file__), "migrations", "014_assessment_events.sql")
        if os.path.exists(migration_path):
            success = run_migration(migration_path)
            if success:
                logger.info("Migration 014_assessment_events applied ✓")
            else:
                logger.error("Failed to apply migration 014_assessment_events")
        else:
            logger.warning(f"Migration file not found: {migration_path}")

    # Auto-apply remaining SQL migrations (015+) not yet recorded
    import glob as _glob
    import re as _re

    migrations_dir = os.path.join(os.path.dirname(__file__), "migrations")
    sql_files = sorted(_glob.glob(os.path.join(migrations_dir, "*.sql")))

    for sql_path in sql_files:
        basename = os.path.basename(sql_path)
        # Extract migration name: "021_wallet_edges.sql" -> "021_wallet_edges"
        name = basename.replace(".sql", "")

        # Skip migrations already handled above (001-014)
        match = _re.match(r"^(\d+)", name)
        if match and int(match.group(1)) <= 14:
            continue

        try:
            already = fetch_one("SELECT 1 FROM migrations WHERE name = %s", (name,))
            if already:
                continue
        except Exception:
            pass

        logger.info(f"Applying migration: {name}...")
        success = run_migration(sql_path)
        # Record migration — even on failure, the objects may already exist
        # (e.g., table created manually before migration system was added)
        try:
            from app.database import execute as _exec
            _exec(
                "INSERT INTO migrations (name) VALUES (%s) ON CONFLICT DO NOTHING",
                (name,),
            )
        except Exception:
            pass
        if success:
            logger.info(f"Migration {name} applied ✓")
        else:
            logger.warning(f"Migration {name} failed (objects may already exist — recorded anyway)")


def main():
    # 1. Initialize database
    logger.info("Initializing database pool...")
    init_pool()

    db_status = db_health_check()
    if db_status.get("status") == "healthy":
        logger.info("Database connected ✓")
    else:
        logger.info("Database tables may not exist yet — running migrations...")

    # 1b. Auto-apply migrations
    run_migrations()

    db_status = db_health_check()
    if db_status.get("status") == "healthy":
        logger.info(f"Database ready: {db_status.get('stablecoin_count', 0)} stablecoins registered ✓")
    else:
        logger.warning(f"Database issue after migrations: {db_status}")

    # 2. Start worker thread
    worker_enabled = os.environ.get("WORKER_ENABLED", "true").lower() == "true"
    if worker_enabled:
        worker_thread = threading.Thread(target=run_worker_loop, daemon=True)
        worker_thread.start()
        logger.info("Background worker thread started")
    else:
        logger.info("Worker disabled (set WORKER_ENABLED=true to enable)")

    # 2b. Start keeper subprocess (on-chain oracle publisher)
    global _keeper_process
    keeper_enabled = os.environ.get("KEEPER_ENABLED", "true").lower() == "true"
    if keeper_enabled and os.environ.get("KEEPER_PRIVATE_KEY"):
        try:
            keeper_dir = os.path.join(os.path.dirname(__file__), "keeper")
            if os.path.exists(os.path.join(keeper_dir, "index.ts")):
                _keeper_process = subprocess.Popen(
                    ["npx", "tsx", "index.ts"],
                    cwd=keeper_dir,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                )
                logger.info(f"Keeper subprocess started (PID {_keeper_process.pid})")
                keeper_log_thread = threading.Thread(
                    target=_forward_keeper_logs, args=(_keeper_process,), daemon=True
                )
                keeper_log_thread.start()
            else:
                logger.warning("Keeper not found at keeper/index.ts — skipping")
        except Exception as e:
            logger.warning(f"Failed to start keeper subprocess: {e}")
    elif not os.environ.get("KEEPER_PRIVATE_KEY"):
        logger.info("Keeper disabled (KEEPER_PRIVATE_KEY not set)")
    else:
        logger.info("Keeper disabled (set KEEPER_ENABLED=true to enable)")

    # 3. Start API server (always port 5000 — mapped to :80 by Replit)
    port = int(os.environ.get("PORT", 5000))
    free_port(port)
    logger.info(f"Starting API on port {port}")

    workers = int(os.environ.get("WEB_WORKERS", "2"))
    uvicorn.run(
        "app.server:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        workers=workers,
    )


if __name__ == "__main__":
    main()
