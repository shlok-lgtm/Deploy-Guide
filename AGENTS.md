# AGENTS.md — Universal Agent Context

> Any AI agent (Replit Agent, Claude Code, Copilot, etc.) working on this
> project MUST read this file first. It prevents the most common mistakes.

## CRITICAL RULES

### 1. NEVER hardcode stablecoin lists
The stablecoin registry is DYNAMIC. New assets are auto-promoted from the
backlog when they cross $1M in wallet exposure. The system scored 10 coins
at launch and now scores 14+. It will keep growing.

- ALWAYS query the API or database for current scored assets
- NEVER write code that assumes a fixed number of stablecoins
- NEVER create arrays/lists like `['usdc', 'usdt', 'dai', ...]`
- ALWAYS use: `GET /api/scores` (returns ALL currently scored assets)
- The stablecoins table in Postgres is the source of truth

### 2. Hub vs Spoke architecture
The Replit app (this repo) is the HUB. It owns:
- Neon Postgres (all data)
- Scoring engine
- REST API
- Frontend

Distribution channels are SPOKES (separate repos):
- basis-snap (MetaMask)
- basis-safe (Safe Guard)
- basis-bots (Twitter/Telegram/Discord)
- basis-mcp (MCP server)
- basis-oracle (contracts + keeper)
- basis-npm (npm package)

Spokes ONLY interact through the API. They never import from app/.
They never touch the database directly.

### 3. Don't break what's running
Read CLAUDE.md for the full list. Key items:
- Don't modify scoring.py weights without versioning
- Don't restart the running server during builds
- Don't restructure existing database tables
- Don't rewrite App.jsx from scratch
- New work goes in new files/modules

### 4. Wallet Risk Graph is V4
The wallet is the universal join key. Every feature should think in terms
of BOTH asset-level scores (SII) AND wallet-level risk profiles.
- GET /api/wallets/{address} — wallet risk profile
- GET /api/scores — asset-level scores
- Both matter. Surface both.

### 5. Terminology
- Use: score, index, surface
- Never use: rating
- Use: validation (not traction)
- Use: bear/base/bull (not conservative)

## API REFERENCE (for spoke developers)

Base URL: https://basisprotocol.xyz

### Stablecoin scores
- GET /api/scores — ALL scored stablecoins (dynamic count, currently 14+)
- GET /api/scores/{coin} — detailed breakdown for one coin
- GET /api/methodology — formula, weights, version

### Wallet risk graph
- GET /api/wallets/{address} — full risk profile
- GET /api/wallets/{address}/history — daily risk score history
- GET /api/wallets/top — top wallets by value (filterable by tier)
- GET /api/wallets/riskiest — lowest risk scores
- GET /api/wallets/stats — aggregate stats

### Backlog
- GET /api/backlog — unscored assets ranked by capital exposure
- GET /api/backlog/{token_address} — detail for one unscored asset

### System
- GET /api/health — system status
- POST /api/admin/index-wallets?key=KEY — trigger indexing run
