# Basis Protocol — Stablecoin Integrity Index (SII)

## Overview
Comprehensive stablecoin risk analysis platform that calculates SII scores by collecting data from multiple sources (CoinGecko, DeFiLlama, Curve, Etherscan), crawls DeFi governance forums for intelligence, and provides a REST API for accessing risk scores and generating content opportunities.

## Architecture
- **Runtime**: Python 3.11
- **Framework**: FastAPI with Uvicorn
- **Database**: PostgreSQL (Replit built-in)
- **Port**: 5000 (API server)

## Project Structure
```
main.py                     - Entry point (API + background worker)
app/
  server.py                 - FastAPI routes
  database.py               - PostgreSQL connection pool (contextmanager-based)
  config.py                 - Stablecoin registry and environment config
  scoring.py                - SII formula, weights, normalization
  governance.py             - Governance forum crawler + analysis
  content_engine.py         - Content opportunity generation
  worker.py                 - Background scoring cycle
  collectors/
    coingecko.py            - CoinGecko API collector
    defillama.py            - DeFiLlama collector
    curve.py                - Curve Finance collector
    offline.py              - Static/config-based components
migrations/
  001_initial_schema.sql    - Core database schema (8 tables + governance)
exports/
  governance_export.sql     - Pre-imported governance data (113 docs, 24k+ mentions)
import_governance.py        - Governance data import utility
```

## Database Tables
- `stablecoins` - Registry of 9 tracked stablecoins
- `component_readings` - Raw data points from collectors
- `scores` - Current computed SII scores
- `score_history` - Daily snapshots
- `score_events` - Crisis events timeline
- `historical_prices` - Hourly price data
- `deviation_events` - Detected peg deviations
- `data_provenance` - Audit trail
- `gov_documents` - Governance forum posts
- `gov_stablecoin_mentions` - Stablecoin mentions in governance
- `gov_metric_mentions` - Risk metric mentions
- `gov_analysis_tags` - Analysis tags
- `gov_crawl_logs` - Crawl history

## Key API Endpoints
- `GET /api/health` - System health
- `GET /api/scores` - All stablecoin scores
- `GET /api/scores/{coin}` - Detailed score
- `GET /api/scores/{coin}/history` - Historical scores
- `GET /api/compare?coins=usdc,usdt` - Compare stablecoins
- `GET /api/methodology` - Formula and weights
- `GET /api/governance/stats` - Governance intelligence
- `GET /api/governance/debates` - Hot debates
- `GET /api/governance/sentiment` - Sentiment trends
- `GET /api/content/opportunities` - Content opportunities

## Environment Variables
- `DATABASE_URL` - PostgreSQL connection (auto-configured)
- `COINGECKO_API_KEY` - Required for live scoring
- `ETHERSCAN_API_KEY` - Required for on-chain data
- `ANTHROPIC_API_KEY` - Optional, for content generation
- `WORKER_ENABLED` - Set to "true" to enable background scoring
- `COLLECTION_INTERVAL` - Minutes between scoring cycles (default: 60)

## Workflow
Command: `python main.py`

## Recent Changes
- 2026-02-10: Initial deployment to Replit
  - Fixed governance.py get_conn() context manager bugs
  - Updated port from 8000 to 5000
  - Fixed migration schema (immutable date function for unique index)
  - Imported governance data (113 docs, 24k metric mentions)
  - Fixed health_check return value handling in main.py
