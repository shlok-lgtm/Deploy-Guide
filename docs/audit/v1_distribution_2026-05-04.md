# Audit v1 Violation Distribution Report

**Run timestamp:** 2026-05-04T09:33:05Z
**Git HEAD:** 24799a3b8735439f010672a6c753dfee19b18213
**Script:** scripts/audit_sync_db_in_async.py (v1, callgraph-aware)

## Summary

- **Total violations:** 513
- **Total files:** 69
- **Top 10 files:** 332 violations (64.7%)
- **Top 20 files:** 411 violations (80.1%)

## Top 20 Files by Violation Count

| Count | File |
|------:|------|
| 84 | app/server.py |
| 43 | app/worker.py |
| 37 | app/payments.py |
| 36 | app/ops/routes.py |
| 31 | app/services/cda_collector.py |
| 21 | app/publisher/page_renderer.py |
| 21 | app/ops/tools/investor_monitor.py |
| 21 | app/mcp_server.py |
| 20 | app/ops/tools/governance_monitor.py |
| 18 | app/enrichment_worker.py |
| 13 | app/budget/daily_cycle.py |
| 12 | app/agent/api.py |
| 10 | app/ops/tools/oracle_monitor.py |
| 8 | app/ops/tools/twitter_monitor.py |
| 8 | app/indexer/pipeline.py |
| 6 | app/ops/entity_routes.py |
| 6 | app/data_layer/contract_surveillance.py |
| 6 | app/collectors/vault_collector.py |
| 5 | app/ops/tools/scraper.py |
| 5 | app/indexer/api.py |

## Notes

- `app/server.py` and `app/ops/routes.py` are request handlers (FastAPI async def).
  These were already converted in PR-A1 (90ae9c8) on the production branch but
  the conversion hasn't landed on main in this sandbox. Once PR-A1 lands cleanly,
  these ~120 violations disappear.
- `app/worker.py` violations are mostly in `run_slow_cycle` and `main` startup
  (LOW severity, out of scope for Wave A).
- `app/payments.py` has 37 violations across 11 paid endpoint handlers — all
  request handlers calling sync DB.
- The remaining 49 files account for only 102 violations (19.9%).
