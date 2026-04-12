# Basis Protocol — Claude Code Context

## What This Project Is

Basis Protocol is decision integrity infrastructure for on-chain finance. The core product is the **Stablecoin Integrity Index (SII)** — a deterministic, versioned scoring system for stablecoin risk. The platform has expanded to include three additional indices (**PSI**, **RPI**, **CQI**), a **Wallet Risk Graph** (every wallet gets a risk profile based on asset holdings), an **assessment/publishing pipeline**, and an **operations hub**.

**One-line thesis:** Basis is the shared risk state of the network — a canonical risk registry where every wallet-exposed asset has a quality score.

## What's Running (DO NOT BREAK)

The SII dashboard and API are **live in production**. The following are stable and should not be modified without explicit instruction:

- `main.py` (712 lines) — Entry point. Starts uvicorn + background worker thread on port 5000. Also starts keeper subprocess if `KEEPER_ENABLED=true`.
- `app/server.py` (6,684 lines) — FastAPI server with 180+ API endpoints. Serves React SPA. Includes response caching, rate limiting, usage tracking, x402 payment middleware.
- `app/scoring.py` — SII v1.0.0 formula. Canonical weights. Do not change without versioning.
- `app/worker.py` — Scoring cycle across all registered stablecoins. Runs in daemon thread. Handles SII/PSI scoring, CDA collection, wallet expansion, profile rebuilding, edge building, state attestation, health sweeps.
- `app/collectors/` — 15 data collectors (CoinGecko, DeFiLlama, Curve, Etherscan, Solana/Helius, smart contract, flows, offline, derived, governance, collateral, treasury, PSI, actor metrics).
- `app/config.py` — Stablecoin registry (10 base coins: USDC, USDT, DAI, FRAX, PYUSD, FDUSD, TUSD, USDD, USDe, USD1). Registry is DYNAMIC — assets auto-promote from backlog when they cross $1M in wallet exposure. Currently 14+ scored.
- `app/database.py` — Neon Postgres connection pool (psycopg2). Helpers: `fetch_one`, `fetch_all`, `execute`, `get_cursor`.
- `app/governance.py` — Governance document crawler + sentiment analysis (Aave, MakerDAO, Compound, Morpho, Frax forums).
- `app/content_engine.py` — Content signal generation from governance data.
- `frontend/src/App.jsx` (3,212 lines) — React dashboard. Vite build. Do not rewrite from scratch.
- `migrations/` — 54 applied SQL migrations (001 through 054). Next migration: 055.

## Architecture

```
main.py (uvicorn + worker thread + keeper subprocess)
├── app/server.py (FastAPI — serves API + React SPA)
│   ├── Core: /api/health, /api/scores, /api/scores/{coin}, /api/compare
│   ├── History: /api/scores/{coin}/history, /at/{date}, /range
│   ├── Methodology: /api/methodology, /api/methodology/versions, /api/indices
│   ├── Integrity: /api/integrity, /api/integrity/{domain}, /api/backtest/{coin}
│   ├── PSI: /api/psi/scores, /api/psi/scores/{slug}, /verify, /backtest
│   ├── RPI: /api/rpi/scores, /api/rpi/rankings, /api/rpi/lenses, /api/rpi/compare
│   ├── CDA: /api/cda, /api/cda/issuers, /api/cda/attestations, /api/cda/quality
│   ├── Composition: /api/compose/cqi, /api/compose/rqs
│   ├── Wallets: /api/wallets/top, /api/wallets/{address}, /profile, /connections, /contagion
│   ├── Assessments: /api/assessments, /api/assessment-events, /api/pulse/*
│   ├── Divergence: /api/divergence, /api/divergence/assets, /api/divergence/wallets
│   ├── Reports: /api/reports/{entity_type}/{entity_id}, /api/lenses
│   ├── Protocols: /api/protocols/{slug}/*, /api/treasury/*
│   ├── Query: /api/query (POST), /api/query/templates, /api/query/schema
│   ├── Discovery: /api/discovery/latest, /api/discovery/domain/{domain}
│   ├── Provenance: /api/provenance/*, /api/state-root/latest
│   ├── Admin: /api/admin/* (key-protected — reindex, backfill, edges, CDA, PSI, RPI)
│   ├── Ops: /api/ops/* (100+ routes — targets, content, health, alerts, investors, ABM, keeper)
│   ├── MCP: /mcp (GET/POST/DELETE — Model Context Protocol HTTP transport)
│   ├── Keys: /api/keys/generate, /api/admin/apikeys
│   ├── Specs: /api/specs/severity, /api/specs/composition, /api/specs/divergence
│   ├── SSR pages: /, /witness, /proof/sii/{symbol}, /proof/psi/{slug}, /report/*
│   └── Publisher pages: /wallet/{address}, /asset/{symbol}, /assessment/*, /pulse/*
│
├── app/worker.py (background scoring cycle — hourly)
│   ├── collectors/ → component_readings table
│   ├── scoring.py → scores table
│   ├── CDA collection, wallet expansion, profile rebuilding
│   ├── Edge building, state attestation, health sweeps
│   └── store_history_snapshot → score_history table
│
├── app/indexer/ (wallet risk graph)
│   ├── pipeline.py — seed → scan → score → store → expand
│   ├── scanner.py — Blockscout v2 / Etherscan V2 fallback
│   ├── scorer.py — risk_score, concentration_hhi, coverage_quality
│   ├── edges.py / solana_edges.py — wallet-to-wallet transfer edges
│   ├── backlog.py — unscored asset tracking + auto-promotion
│   └── expander.py — deeper top-holder discovery
│
├── app/agent/ (verification agent)
│   ├── watcher.py — monitors wallet graph for state changes
│   ├── assessor.py — generates assessment events
│   ├── classifier.py — severity assignment (silent/notable/alert/critical)
│   └── store.py — persists with idempotency guard (content_hash)
│
├── app/publisher/ (multi-layer publishing)
│   ├── pipeline.py — dispatches to renderers
│   ├── page_renderer.py — HTML + JSON-LD
│   ├── social_renderer.py — Twitter/Discord formatting
│   ├── pulse_renderer.py — daily risk surface snapshots
│   └── onchain_renderer.py — keccak256 hash + calldata
│
├── app/rpi/ (Revenue Protocol Index — 13+ DeFi protocols)
│   ├── scorer.py — base RPI + optional lens overlays
│   ├── expansion.py — auto-discovers new protocols
│   ├── historical.py — backfills governance/parameter/incident history
│   └── collectors: snapshot, tally, forum, parameter, incident, revenue, docs
│
├── app/services/ (utility services)
│   ├── cda_collector.py — adaptive vendor waterfall (Extract, Search, Firecrawl)
│   ├── temporal_engine.py / psi_temporal_engine.py — historical score reconstruction
│   ├── historical_backfill.py / psi_backfill.py — data backfill
│   └── firecrawl_client.py, parallel_client.py, reducto_client.py
│
├── app/ops/ (operations hub — 100+ admin routes)
│   ├── routes.py — targets, content, health, alerts, investors, ABM campaigns
│   ├── tools/ — 17 monitoring tools (alerter, analytics, exposure, news, twitter, etc.)
│   └── seed.py — seeds stablecoin registry, RPI protocols, governance forums
│
├── keeper/ (TypeScript — on-chain oracle publisher)
│   ├── index.ts — main keeper loop
│   ├── publisher.ts — on-chain transaction submission
│   ├── differ.ts — score change detection
│   └── alerter.ts — alert generation
│
├── app/database.py (Neon Postgres pool)
├── frontend/dist/ (pre-built React app)
└── dbt/ (analytics — staging → intermediate → discovery signal layers)
```

## Four Indices

| Index | Scope | Definition File | Version |
|-------|-------|-----------------|---------|
| **SII** | Stablecoin risk | `app/index_definitions/sii_v1.py` | v1.0.0 |
| **PSI** | Protocol safety | `app/index_definitions/psi_v01.py` | v0.2.0 |
| **RPI** | Protocol revenue/governance | `app/index_definitions/rpi_v2.py` | v2.0.0 |
| **CQI** | Composite (SII + PSI) | `app/composition.py` | on-demand |

All indices use the generic schema in `app/index_definitions/schema.py` and are scored by `app/scoring_engine.py`.

## SII Formula (v1.0.0 — canonical, do not modify)

```
SII = 0.30×Peg + 0.25×Liquidity + 0.15×MintBurn + 0.10×Distribution + 0.20×Structural

Structural = 0.30×Reserves + 0.20×SmartContract + 0.15×Oracle + 0.20×Governance + 0.15×Network
```

102 components across 11 categories. 83 automated, deterministic. Scores are 0-100, grades A+ through F.

## Database (Neon Postgres)

54 migrations applied (001 through 054). Key table groups:

- **Core SII:** stablecoins, component_readings, scores, score_history, score_events, historical_prices, deviation_events, data_provenance
- **Wallet graph:** wallets, wallet_holdings, wallet_risk_scores, wallet_edges, wallet_profiles, unscored_assets
- **PSI:** psi_scores, psi_components, psi_temporal_reconstructions
- **RPI:** rpi_scores, rpi_components, rpi_protocol_config
- **Assessment:** assessment_events, daily_pulses
- **CDA:** cda_extractions, cda_monitors, cda_source_urls
- **Ops:** ops_targets, ops_content, ops_alerts, abm_campaigns
- **Infrastructure:** api_keys, api_usage, payment_log, keeper_cycles, state_attestations, provenance_proofs, discovery_signals, lens_configs, sbt_tokens
- **Governance:** governance_documents, governance_stablecoin_mentions, governance_metric_mentions, governance_snapshots

Connection via `DATABASE_URL` env var. Pool: min=2, max=10, with keepalives.

## Environment Variables

**Required:**
- `DATABASE_URL` — Neon Postgres connection string
- `COINGECKO_API_KEY` — CoinGecko Pro API
- `ETHERSCAN_API_KEY` — Etherscan API

**Optional APIs:**
- `ALCHEMY_API_KEY` — Alchemy (EVM)
- `HELIUS_API_KEY` — Helius (Solana)
- `ANTHROPIC_API_KEY` — Claude API (CDA deep research, content drafting)

**Server:**
- `API_HOST` — default `0.0.0.0`
- `API_PORT` / `PORT` — default 5000
- `CORS_ORIGINS` — default `*`
- `ADMIN_KEY` — Admin panel + ops routes access

**Worker:**
- `WORKER_ENABLED` — true/false for background scoring (default: true)
- `COLLECTION_INTERVAL` — minutes between scoring cycles (default: 60)

**Keeper:**
- `KEEPER_ENABLED` — true/false for on-chain oracle publishing
- `KEEPER_PRIVATE_KEY` — Wallet private key for keeper transactions

## Key Modules Reference

| Module | Purpose |
|--------|---------|
| `app/scoring_engine.py` | Generic entity scorer — works with any index definition |
| `app/composition.py` | Composes indices (SII + PSI → CQI) via geometric mean, weighted average, minimum |
| `app/actor_classification.py` | Deterministic wallet actor classifier (autonomous_agent, human, contract_vault) |
| `app/query_engine.py` | Safe parameterized query interface for wallet graph — whitelisted filters, hard limits |
| `app/wallet_profile.py` | On-demand wallet risk profile generator |
| `app/report.py` | Assembles complete reports from existing scores (SII, PSI, CQI, wallet) |
| `app/pulse_generator.py` | Daily snapshot of entire risk surface — idempotent |
| `app/divergence.py` | Detects capital-flow / quality mismatches |
| `app/integrity.py` | Data freshness checks + coherence rules — pre-render validation |
| `app/computation_attestation.py` | SHA-256 input hashing for retroactive verification |
| `app/state_attestation.py` | Universal state hashing at persist time |
| `app/mcp_server.py` | MCP HTTP transport — 8 tools exposing hub REST API |
| `app/rate_limiter.py` | Sliding window: public 10 req/min per IP, keyed 120 req/min |
| `app/usage_tracker.py` | Buffered request logging, bulk-flush every 30s or 100 entries |
| `app/payments.py` | x402 payment middleware — agents pay USDC on Base per-request |
| `app/budget/manager.py` | Daily API budget coordination: SII 25K → PSI 10K → wallet 40K → expansion |
| `app/discovery.py` | Orchestrator for dbt analytics — triggers dbt run, stores top signals |

## Solidity Contracts (Foundry)

- `src/BasisSIIOracle.sol` — On-chain SII score oracle (Base/Arbitrum)
- `src/BasisRating.sol` — Rating contract
- `src/BasisSafeGuard.sol` — Gnosis Safe Guard module
- `src/interfaces/IBasisSIIOracle.sol` — Oracle interface
- Config: `foundry.toml` (Solidity 0.8.24, optimizer 200 runs)
- Tests: `test/BasisSIIOracle.t.sol`, `test/BasisRating.t.sol`
- Deploy scripts: `script/Deploy.s.sol`, `script/DeployRating.s.sol`

## Frontend

- **Framework:** React 18.3.1 + Vite 6.2.3
- **Main app:** `frontend/src/App.jsx` (3,212 lines — tabbed dashboard: rankings, protocols, wallets, witness, methodology)
- **Ops dashboard:** `frontend/src/pages/OpsDashboard.jsx`
- **Build:** `cd frontend && npm run build` → `frontend/dist/`
- **Served by:** FastAPI static mount at `/assets` + SPA catch-all
- **SSR:** Bot crawlers get server-rendered HTML for key pages (rankings, witness, proof, report)

## Deployment

| Service | Dockerfile | Runtime | Notes |
|---------|-----------|---------|-------|
| API server | `Dockerfile.api` | Python 3.11, port 8000 | Worker disabled, keeper disabled |
| Scoring worker | `Dockerfile.worker` | Python 3.11 | Runs single scoring cycle |
| Oracle keeper | `Dockerfile.keeper` | Node 22 | TypeScript via tsx |

- **Railway:** `railway.json` — restart on failure, max 5 retries
- **Replit:** `.replit` — Python 3.11, Node.js 20, Postgres 16

## dbt Analytics

- **Location:** `dbt/`
- **Layers:** staging (12 models) → intermediate (17 models) → discovery (10 models)
- **Purpose:** Raw Postgres → analytics-ready tables → signal discovery
- **Macros:** `rank_stability`, `rolling_stats`, `z_score`

## Tests

- **Python:** `tests/` — pytest. `conftest.py` + `e2e_test.py`
- **Solidity:** `test/` — Foundry. `BasisRating.t.sol`, `BasisSIIOracle.t.sol`

## Hub-Spoke Architecture

This repo is the **HUB**. It owns Postgres, the scoring engine, REST API, and frontend.

**Spokes** (separate repos) interact ONLY through the API:
- basis-snap (MetaMask)
- basis-safe (Safe Guard)
- basis-bots (Twitter/Telegram/Discord)
- basis-mcp (MCP server)
- basis-oracle (contracts + keeper)
- basis-npm (npm package)

Spokes never import from `app/`. They never touch the database directly.

## Architectural Patterns

1. **Index Definitions as Configuration** — SII, PSI, RPI all use a generic schema. Generic `scoring_engine.py` works with any definition.
2. **Multi-Layer Publishing** — Canonical storage → Machine (pages/API) → Amplification (social) → Contextual → Institutional.
3. **Computation Attestation** — Input hashes for retroactive verification. State hashes for domains that can't be reconstructed.
4. **Temporal Reconstruction** — Historical scores computed from backfilled data. Confidence tags at each point.
5. **Wallet Graph as Central Intelligence** — Wallets, holdings, edges, profiles, actor classification. Universal join key.
6. **Agent-Based Assessment** — Watcher → Assessor → Classifier → Store → Publisher pipeline.
7. **Budget-Aware Collection** — Daily API limits with priority cascade (SII → PSI → wallet_refresh → expansion).
8. **Regulatory Lenses** — Overlay views: Basel III (`basel_sco60.json`), MiCA Article 67 (`mica_art67.json`), GENIUS Act (`genius_act.json`).

## Conventions

- Python backend (FastAPI, psycopg2, httpx for async HTTP)
- React frontend (Vite, single-file App.jsx pattern)
- All database access through `app/database.py` helpers (`fetch_one`, `fetch_all`, `execute`, `get_cursor`)
- New migrations go in `migrations/` with sequential numbering (next: 055)
- Scores are 0-100, grades A+ through F
- **Never use the word "rating"** — use "score," "index," "surface"
- **Terminology:** validation (not traction), bear/base/bull (not conservative)
- All new API routes under `/api/` prefix
- CORS is open (`*`) for now
- NEVER hardcode stablecoin lists — registry is dynamic, always query API or DB
- New work goes in NEW files/modules. Do not modify existing stable code unless fixing a bug.

## Git

- Remote: `basis-protocol/basis-hub` (GitHub)
- Branch: main
- Push regularly. Large SQL files are in .gitignore.

## Do NOT

- Restart the running server (it's serving production traffic)
- Modify scoring weights without explicit instruction and version bump
- Delete or restructure existing database tables
- Rewrite App.jsx from scratch (it's 3,212 lines and working)
- Install heavy dependencies without asking first
- Use `sudo` for anything
- Hardcode stablecoin lists — the registry is dynamic
- Create arrays/lists like `['usdc', 'usdt', 'dai', ...]` — always query
