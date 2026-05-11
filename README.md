# Basis Protocol — Stablecoin Integrity Index (SII)

Decision integrity infrastructure for on-chain finance. The hub computes
four indices (SII, PSI, RPI, CQI) and a wallet risk graph, and serves
them via a REST API and a React dashboard.

Production: [basisprotocol.xyz](https://basisprotocol.xyz) — runs on
Railway against an owned Neon Postgres instance.

---

## Quick Start (local dev)

### 1. Prerequisites
- Python 3.11+
- Node.js 20+ (for the frontend build)
- A Neon Postgres database (free tier is fine for dev)

### 2. Environment
Copy `.env.example` to `.env` and fill in values:

| Variable | Required | Notes |
|---|---|---|
| `DATABASE_URL` | yes | Neon pooler URL — keep the `-pooler` suffix. Do NOT append `?options=...statement_timeout=...`; the pooler rejects libpq startup options. |
| `COINGECKO_API_KEY` | yes | CoinGecko Pro key |
| `ETHERSCAN_API_KEY` | yes | etherscan.io/myapikey |
| `WORKER_ENABLED` | no | `true` to enable the background scoring loop; `false` to run API-only |
| `COLLECTION_INTERVAL` | no | Minutes between scoring cycles (default 60) |
| `ANTHROPIC_API_KEY` | no | Enables content drafting and CDA deep research |

### 3. Install
```
pip install -r requirements.txt
cd frontend && npm install && npm run build && cd ..
```

### 4. Migrate
```
python setup.py
```
This applies SQL migrations from `migrations/` and verifies the
database is reachable.

### 5. Run
```
python main.py
```
The API serves on `http://localhost:5000` (override with `PORT`). If
`WORKER_ENABLED=true`, the background scoring worker starts in a daemon
thread on the same process. First cycle takes ~90s (CoinGecko +
DeFiLlama + Curve + Etherscan calls); subsequent reads are served from
Postgres.

### 6. Verify
- `GET /api/health` — database status
- `GET /api/scores` — current scores for all tracked stablecoins
- `GET /api/methodology` — formula and weights
- Open `http://localhost:5000/` in a browser for the React dashboard

---

## Production

Production runs on **Railway** in the `valiant-celebration` project.
Three services are built from this repo:

| Service | Image |
|---|---|
| API server | `Dockerfile.api` |
| Scoring worker | `Dockerfile.worker` |
| Oracle keeper (TypeScript) | `Dockerfile.keeper` |

Database is owned **Neon** (project `basis-prod`), accessed via the
pgbouncer pooler endpoint. `DATABASE_URL` is the single source of
truth — never set `PGHOST`/`PGUSER`/etc. directly.

---

## Architecture

```
main.py                    ← entry point (FastAPI + worker thread + optional keeper subprocess)
├── app/server.py          ← API (180+ endpoints) + SSR + SPA serving
├── app/worker.py          ← Scoring cycle (collectors → component_readings → scores)
├── app/scoring.py         ← SII v1.0.0 formula (canonical)
├── app/config.py          ← Dynamic stablecoin registry
├── app/database.py        ← Neon Postgres pool
├── app/collectors/        ← 15 data collectors
├── app/indexer/           ← Wallet risk graph (seed → scan → score → expand)
├── app/agent/             ← Verification agent (watcher → assessor → classifier)
├── app/publisher/         ← Multi-layer publishing (page, social, on-chain)
├── app/rpi/               ← Revenue Protocol Index
├── app/services/          ← CDA collector, temporal engines, vendor clients
└── app/ops/               ← Operations hub routes + tools
```

See `CLAUDE.md` for the canonical architecture and conventions reference.

---

## SII Formula (v1.0.0)

```
SII = 0.30×Peg + 0.25×Liquidity + 0.15×MintBurn + 0.10×Distribution + 0.20×Structural

Structural = 0.30×Reserves + 0.20×Contract + 0.15×Oracle + 0.20×Governance + 0.15×Network
```

102 components across 11 categories. 83 automated. Deterministic — same
inputs always produce the same score.

---

## Troubleshooting

**"Database pool not initialized"**
→ `DATABASE_URL` not set, or it points at a host the runtime can't
reach. Check that the value retains the `-pooler` suffix and no
`?options=...` parameter.

**Scores are empty after starting**
→ The worker hasn't completed its first cycle. Wait ~90s, or run
manually: `python -m app.worker`.

**CoinGecko rate limiting**
→ Set `COLLECTION_INTERVAL=120` to slow the worker down.

**Migration says "relation already exists"**
→ Safe to ignore. Migrations use `IF NOT EXISTS`.
