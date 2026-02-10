# Basis Protocol — SII Rebuild

Stablecoin Integrity Index. 3,200 lines replacing 100,000.

## Quick Start (Replit)

### Step 1: Create new Replit
1. Go to replit.com → Create Repl → Python
2. Delete any starter files
3. Upload all files from this package (drag the whole folder contents in)

### Step 2: Set environment variables
In the Replit Secrets tab (lock icon in sidebar), add:

| Secret | Value | Where to get it |
|--------|-------|-----------------|
| `DATABASE_URL` | `postgresql://...` | Your Neon dashboard (use the same DB as the old prototype) |
| `COINGECKO_API_KEY` | `CG-xxx...` | Your CoinGecko Pro account |
| `ETHERSCAN_API_KEY` | `xxx...` | etherscan.io/myapikey |

Optional:
| `WORKER_ENABLED` | `true` | Set to `false` to run API-only (no background scoring) |
| `COLLECTION_INTERVAL` | `60` | Minutes between scoring cycles |

### Step 3: Install dependencies
In the Replit Shell tab:
```
pip install -r requirements.txt
```

### Step 4: Run setup
```
python setup.py
```
This applies the database migration and verifies everything works.

### Step 5: Import historical data (optional but recommended)
If you have pg_dump files from the old Neon/Replit databases:
1. Create a `dumps/` folder
2. Upload your .sql dump files there
3. Run: `python import_history.py --dir ./dumps/`

This preserves your backtest history and score timeline.

### Step 6: Start
Click the Run button, or:
```
python main.py
```

The API starts on port 8000. The background worker begins scoring all 9 stablecoins automatically.

### Step 7: Verify
- Visit your Replit URL → should show the API docs
- Hit `/api/health` → should show database connected
- Hit `/api/scores` → should show stablecoin scores (after first worker cycle)
- Hit `/api/methodology` → should show formula and weights

---

## What's Running

**`main.py`** — Single entry point. Starts:
1. FastAPI server (port 8000) — serves scores, history, comparisons
2. Background worker thread — collects data, computes SII, writes to database

**The worker runs every 60 minutes by default.** First cycle takes ~90 seconds (API calls to CoinGecko, DeFiLlama, Curve). After that, scores are served from the database instantly.

---

## Architecture

```
main.py                    ← Replit runs this
├── app/server.py          ← API (9 endpoints, read-only)
├── app/worker.py          ← Collector + scorer (write)
├── app/scoring.py         ← SII formula (your IP)
├── app/config.py          ← Stablecoin registry
├── app/database.py        ← PostgreSQL pool
└── app/collectors/
    ├── coingecko.py       ← Peg, liquidity, market components
    ├── defillama.py       ← Chain data, lending TVL
    ├── curve.py           ← 3pool balance
    └── offline.py         ← Transparency, regulatory, governance, reserves
```

**Total: ~3,200 lines.** The old system was ~100,000 lines across 155 files.

---

## API Endpoints

| Endpoint | What it returns |
|----------|----------------|
| `GET /api/health` | Database status, scored count, last update |
| `GET /api/scores` | All stablecoin scores (rankings table) |
| `GET /api/scores/{coin}` | Detailed breakdown + component readings |
| `GET /api/scores/{coin}/history?days=90` | Historical score timeline |
| `GET /api/compare?coins=usdc,usdt,dai` | Side-by-side comparison |
| `GET /api/methodology` | Formula, weights, component specs |
| `GET /api/config` | Stablecoin registry |
| `GET /api/events` | Crisis events timeline |
| `GET /api/deviations/{coin}` | Peg deviation history |

---

## SII Formula (v1.0.0)

```
SII = 0.30×Peg + 0.25×Liquidity + 0.15×Flows + 0.10×Distribution + 0.20×Structural

Structural = 0.30×Reserves + 0.20×Contract + 0.15×Oracle + 0.20×Governance + 0.15×Network
```

102 components across 11 categories. 83 automated. Deterministic — same inputs always produce same outputs.

---

## Dashboard

The React dashboard (`basis-sii-dashboard.jsx`) renders as a Claude artifact or can be deployed to any static host. It connects to this API via the `API_BASE` constant.

To deploy the dashboard separately: drop the jsx into a Vite/Next.js project, or ask Claude to convert it to standalone HTML.

---

## Files from the Old System

If you're migrating from the old prototype:

**What to keep:**
- Your Neon database (the new schema adds tables alongside, doesn't destroy data)
- pg_dump exports of `score_history`, `score_events`, `deviation_events`
- Your CoinGecko Pro API key
- Your Etherscan API key

**What to discard:**
- The old `main.py` (12,426 lines)
- All 155 files in the old repo
- The old Replit deployment

The new system reads the same Neon database but uses a cleaner schema. Historical data imports via `import_history.py`.

---

## Troubleshooting

**"Database pool not initialized"**
→ `DATABASE_URL` not set. Check Replit Secrets.

**Scores are empty after starting**
→ Worker hasn't run yet. Wait 60 seconds for the first cycle, or run manually:
```
python -m app.worker
```

**CoinGecko rate limiting**
→ The worker pauses 2 seconds between coins. If you're on the free tier, set `COLLECTION_INTERVAL=120` for longer pauses.

**Import fails with "relation already exists"**
→ Safe to ignore. The migration uses `IF NOT EXISTS` for tables.
