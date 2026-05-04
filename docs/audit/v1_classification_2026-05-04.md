# Audit v1 Violation Classification

**Date:** 2026-05-04
**Based on:** docs/audit/v1_distribution_2026-05-04.md (513 violations, 69 files)
**Scope:** Top 20 files (411 violations, 80.1% of total)

## Classification

| File | Violations | Severity | Reasoning |
|------|-----------|----------|-----------|
| `app/server.py` | 84 | **MEDIUM** | FastAPI `@app.get`/`@app.post` request handlers; blast radius = one request 504 |
| `app/worker.py` | 43 | **HIGH** | Main asyncio loop — `run_fast_cycle`, `run_slow_cycle`, `_diagnostic_loop` inside `_supervised_loop` / `asyncio.create_task`; wedge = full scoring freeze |
| `app/payments.py` | 37 | **MEDIUM** | All in `@paid_router.get` request handlers (`paid_sii_rankings`, `paid_wallet_profile`, etc.) |
| `app/ops/routes.py` | 36 | **MEDIUM** | All in `@router.get`/`@router.post`/`@router.put` admin request handlers |
| `app/services/cda_collector.py` | 31 | **HIGH** | `run_collection()` called from worker slow cycle and via `asyncio.create_task` in server |
| `app/publisher/page_renderer.py` | 21 | **MEDIUM** | Mixed: `@app.get` route handlers (`wallet_page`, `asset_page`, `sitemap_xml`) dominate |
| `app/ops/tools/investor_monitor.py` | 21 | **MEDIUM** | Called via `await` from `@router` handlers in `ops/routes.py` |
| `app/mcp_server.py` | 21 | **MEDIUM** | All in `@mcp.tool()` handlers — MCP tool endpoints invoked per-request |
| `app/ops/tools/governance_monitor.py` | 20 | **MEDIUM** | Called via `await` from `@router` handlers in `ops/routes.py` |
| `app/enrichment_worker.py` | 18 | **HIGH** | `run_enrichment_pipeline()` called from worker slow cycle; tasks run via `EnrichmentTask` inside background loop |
| `app/budget/daily_cycle.py` | 13 | **HIGH** | `run_daily_cycle()` launched via `asyncio.create_task`; long-lived background task orchestrating SII/PSI/wallet phases |
| `app/agent/api.py` | 12 | **MEDIUM** | All in `@app.get` request handlers (`get_assessments`, `get_assessment`, `get_latest_pulse`) |
| `app/ops/tools/oracle_monitor.py` | 10 | **HIGH** | `poll_oracle_events()` and `poll_external_interactions()` called from `budget/daily_cycle.py` background task |
| `app/ops/tools/twitter_monitor.py` | 8 | **MEDIUM** | Called via `await` from `@router` handlers in `ops/routes.py` |
| `app/indexer/pipeline.py` | 8 | **HIGH** | `run_pipeline_batch()` called from worker slow cycle and via `background_tasks.add_task` |
| `app/ops/entity_routes.py` | 6 | **MEDIUM** | All in `@router.get` request handlers |
| `app/data_layer/contract_surveillance.py` | 6 | **HIGH** | `run_contract_surveillance()` called from worker slow cycle and enrichment pipeline |
| `app/collectors/vault_collector.py` | 6 | **HIGH** | `run_vsri_scoring()` called from worker slow cycle and enrichment pipeline |
| `app/ops/tools/scraper.py` | 5 | **MEDIUM** | Called via `await` from `@router` handlers in `ops/routes.py` |
| `app/indexer/api.py` | 5 | **MEDIUM** | All in `@app.get` request handlers (`wallets_top`, `wallets_riskiest`, `wallet_profile`) |

## Summary by Severity

| Severity | Files | Violations | % of Top 20 |
|----------|-------|-----------|-------------|
| **HIGH** | 8 | 135 | 32.8% |
| **MEDIUM** | 12 | 276 | 67.2% |
| **LOW** | 0 | 0 | 0% |

## Interpretation

- **HIGH (8 files, 135 violations):** These run inside continuous background loops or `asyncio.create_task` tasks. A sync DB call wedge here freezes the event loop — same shape as the April 26 and May 4 outages. Fix priority: immediate (Wave B).

- **MEDIUM (12 files, 276 violations):** These are request handlers where a sync DB call blocks one request but doesn't freeze the worker. Blast radius is bounded to one 504. Fix priority: important but not urgent — these were already converted on the production branch via PR-A1 (90ae9c8). Once that lands cleanly on main, ~120 of these disappear.

- **LOW (0 files):** None of the top 20 files are startup-only or one-shot. The remaining 49 files (102 violations) outside the top 20 may contain LOW-severity entries but were not classified in this pass.
