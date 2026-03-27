# Basis Verification Agent — Build Specification

**Version:** 1.0 | **Date:** March 27, 2026
**Repo:** Deploy-Guide (hub) — this is a hub feature, not a spoke
**Depends on:** wallet indexer (live), scoring engine (live), API (live), rate limiting + usage tracking (deploying)

---

## What This Is

An autonomous verification agent that watches the wallet risk graph for material state changes, generates assessment events, and propagates them through a five-layer publish architecture. The agent is the thing that goes first — it creates the public record of risk state at the moment decisions are being made.

The agent does NOT:
- Execute transactions
- Provide trading signals or recommendations
- Offer opinions or commentary
- Interact conversationally with anyone

The agent DOES:
- Watch the wallet graph and SII scores for material changes
- Generate structured assessment events when triggers fire
- Store every assessment in the canonical database
- Classify assessments by severity
- Hand assessments to the publish pipeline for propagation

---

## Architecture

```
Existing Infrastructure (DO NOT MODIFY)
├── app/worker.py          — hourly SII scoring cycle (writes to scores table)
├── app/indexer/pipeline.py — wallet indexer (writes to wallet_graph schema)
├── app/indexer/scorer.py   — wallet risk computation
├── app/server.py           — API endpoints
└── migrations/007          — wallet_graph schema

New Modules (THIS SPEC)
├── app/agent/
│   ├── __init__.py
│   ├── watcher.py          — monitors for trigger conditions
│   ├── assessor.py         — generates assessment event objects
│   ├── classifier.py       — assigns severity (silent/notable/alert/critical)
│   ├── store.py            — writes assessment events to canonical DB
│   └── config.py           — thresholds, intervals, toggle flags
├── app/publisher/
│   ├── __init__.py
│   ├── pipeline.py         — subscribes to new assessments, dispatches to renderers
│   ├── page_renderer.py    — generates/updates assessment pages (HTML + JSON-LD)
│   ├── social_renderer.py  — formats for X/Telegram/Farcaster
│   ├── onchain_renderer.py — posts content hash via oracle contract
│   └── pulse_renderer.py   — generates daily pulse summary
├── migrations/
│   └── 014_assessment_events.sql
└── templates/
    ├── wallet.html          — /wallet/{address} page template
    ├── asset.html           — /asset/{symbol} page template
    ├── assessment.html      — /assessment/{id} page template
    └── pulse.html           — /pulse/{date} page template
```

The agent and publisher are SEPARATE. The agent emits events. The publisher renders them. This separation is the protocol architecture — the agent is the engine, the publisher is a renderer that can be replaced or multiplied.

---

## Migration: 014_assessment_events.sql

```sql
BEGIN;

-- Assessment events table — the protocol primitive
CREATE TABLE IF NOT EXISTS assessment_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ DEFAULT NOW(),

    -- What was assessed
    wallet_address VARCHAR(42) NOT NULL,
    chain VARCHAR(20) DEFAULT 'ethereum',

    -- Trigger
    trigger_type VARCHAR(30) NOT NULL,
        -- daily_cycle, large_movement, score_change,
        -- concentration_shift, depeg, auto_promote
    trigger_detail JSONB,
        -- e.g. {"movement_usd": 12000000, "direction": "in", "asset": "TUSD"}

    -- Assessment snapshot
    wallet_risk_score DOUBLE PRECISION,
    wallet_risk_grade VARCHAR(2),
    wallet_risk_score_prev DOUBLE PRECISION,  -- previous score for delta
    concentration_hhi DOUBLE PRECISION,
    concentration_hhi_prev DOUBLE PRECISION,
    coverage_ratio DOUBLE PRECISION,
    total_stablecoin_value DOUBLE PRECISION,
    holdings_snapshot JSONB,
        -- [{symbol, value_usd, pct_of_wallet, sii_score, sii_grade, sii_7d_delta}]

    -- Classification
    severity VARCHAR(10) NOT NULL DEFAULT 'silent',
        -- silent, notable, alert, critical
    broadcast BOOLEAN DEFAULT FALSE,

    -- Verification
    content_hash VARCHAR(66),  -- keccak256 of canonical payload
    onchain_tx VARCHAR(66),    -- tx hash once anchored (null until posted)
    methodology_version VARCHAR(20) DEFAULT 'wallet-v1.0.0',

    -- Publish tracking
    page_url VARCHAR(255),
    social_posted_at TIMESTAMPTZ,
    onchain_posted_at TIMESTAMPTZ
);

CREATE INDEX idx_ae_wallet ON assessment_events(wallet_address, created_at DESC);
CREATE INDEX idx_ae_severity ON assessment_events(severity, created_at DESC);
CREATE INDEX idx_ae_broadcast ON assessment_events(broadcast, created_at DESC);
CREATE INDEX idx_ae_created ON assessment_events(created_at DESC);
CREATE INDEX idx_ae_trigger ON assessment_events(trigger_type);

-- Daily pulse summaries
CREATE TABLE IF NOT EXISTS daily_pulses (
    id SERIAL PRIMARY KEY,
    pulse_date DATE UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    summary JSONB NOT NULL,
        -- {scores: [{symbol, score, grade, delta_24h}],
        --  total_tracked: float, wallets_indexed: int,
        --  alerts_today: int, notable_events: [...]}
    page_url VARCHAR(255),
    social_posted_at TIMESTAMPTZ
);

INSERT INTO migrations (name) VALUES ('014_assessment_events') ON CONFLICT DO NOTHING;

COMMIT;
```

---

## Watcher: Trigger Logic

The watcher runs on a configurable interval (default: every 15 minutes). It queries the wallet graph and SII scores for material changes since the last run.

### Trigger Conditions

| Trigger | Detection Query | Threshold | Notes |
|---------|----------------|-----------|-------|
| `large_movement` | Compare current `wallet_holdings` to previous snapshot. Detect value change >$1M in a single asset for a single wallet. | Movement >$1M USD | Direction (in/out) and target asset recorded. |
| `score_change` | Compare current `scores.overall_score` to score 24h ago from `score_history`. | Delta >3 points in 24h for any scored stablecoin | Re-assess all wallets with material exposure to that asset. |
| `concentration_shift` | Compare current wallet HHI to previous. Detect single asset going from <20% to >40% of wallet. | Pct change from <20% to >40% | Only for wallets with >$500K total value. |
| `depeg` | Check current prices via CoinGecko. Detect deviation >1% from peg sustained >1 hour. | Price deviation >1% for >1 hour | Immediate broadcast. Re-score all affected wallets. |
| `auto_promote` | Check `unscored_assets` for newly promoted assets (scoring_status changed to 'scored'). | Any new promotion | Log event. Include in daily pulse. |
| `daily_cycle` | Runs once per day at 00:00 UTC after SII scoring cycle completes. | Always fires | Generates daily pulse. Assesses top 100 wallets by value. |

### What the Watcher Does NOT Do

- Monitor mempool (too complex, not needed for daily-cycle scoring)
- Track individual transactions in real-time (the indexer already does batch scanning)
- Watch external data sources directly (it reads from existing database tables)

The watcher reads from the *existing* database tables that the indexer and scoring engine already populate. It does not duplicate their work. It detects *changes between runs* by comparing current state to previous state.

### Previous State Tracking

The watcher needs to compare current state to previous state. Two approaches:

**Option A (recommended): Use assessment_events table itself.** The most recent assessment for each wallet IS the previous state. The watcher queries `assessment_events WHERE wallet_address = X ORDER BY created_at DESC LIMIT 1` to get the previous score, HHI, and holdings. First run seeds from current wallet_risk_scores.

**Option B: Snapshot table.** A separate `watcher_snapshots` table stores the last-seen state per wallet. Simpler queries but redundant data.

Use Option A. The assessment_events table is the canonical history — use it as the comparison baseline.

---

## Assessor: Event Generation

When a trigger fires, the assessor generates an assessment event object.

```python
def generate_assessment(
    wallet_address: str,
    trigger_type: str,
    trigger_detail: dict,
    current_holdings: list[dict],
    current_risk: dict,
    previous_assessment: dict | None,
    sii_scores: dict,
) -> dict:
    """
    Generate a canonical assessment event.
    
    Returns a dict matching the assessment_events schema.
    The content_hash is computed from the canonical payload:
        keccak256(json.dumps(sorted_payload, sort_keys=True))
    """
```

The assessor:
1. Pulls current wallet holdings from `wallet_graph.wallet_holdings`
2. Pulls current SII scores from `scores`
3. Pulls 7-day score deltas from `score_history`
4. Computes wallet risk using existing `compute_wallet_risk()`
5. Pulls previous assessment from `assessment_events` for delta computation
6. Builds the holdings snapshot with per-asset SII + 7d delta
7. Computes content hash (keccak256 of canonical JSON payload)
8. Returns the complete assessment event dict

---

## Classifier: Severity Assignment

```python
def classify_severity(
    assessment: dict,
    previous: dict | None,
    config: AgentConfig,
) -> tuple[str, bool]:
    """
    Returns (severity, broadcast_worthy).
    
    Rules:
    - silent:   No material change. Daily cycle with delta <1 pt.
    - notable:  Score movement 1-3 pts. Moderate activity. Included in daily pulse.
    - alert:    Capital flowing toward deteriorating quality. Score delta >3 pts.
                Concentration spike. Broadcast immediately.
    - critical: Depeg event >1%. Score drop >5 pts in 24h. Broadcast + on-chain.
    """
```

### Divergence Detection (the key innovation)

The classifier doesn't just look at score changes. It detects **divergence** — capital flowing *toward* deteriorating quality. This is the signal that makes Basis different from a whale alert.

```python
def detect_divergence(assessment: dict, previous: dict | None) -> bool:
    """
    Returns True if money is moving toward assets with declining scores.
    
    Divergence = any holding where:
        1. pct_of_wallet increased (capital flowed in), AND
        2. sii_7d_delta < 0 (the asset's quality is declining)
    
    Only triggers if the asset's SII is also below a threshold (default: 80).
    """
```

---

## Store: Canonical Write

Every assessment event — regardless of severity — writes to `assessment_events`. This is mandatory. The compounding asset requires every event to be persisted, even silent ones.

```python
def store_assessment(assessment: dict) -> str:
    """
    Insert assessment event into canonical database.
    Returns the UUID of the created event.
    
    This function NEVER skips. Silent events are stored.
    The only condition that prevents storage is a duplicate
    content_hash within the same hour (idempotency guard).
    """
```

---

## Publisher Pipeline

The publisher subscribes to new assessment events and dispatches to renderers based on severity.

```python
async def publish(assessment: dict) -> None:
    """
    Propagate assessment through the five-layer architecture.
    
    Layer 1 (Canonical):    Always. Already stored by agent.
    Layer 2 (Machine):      Always. Update page + API cache.
    Layer 3 (Amplification): Only if broadcast=True.
    Layer 4 (Contextual):   Manual/deferred. Not automated.
    Layer 5 (Institutional): Aggregated. Weekly/monthly.
    """
    
    # Layer 2: Always
    await page_renderer.update_wallet_page(assessment)
    await page_renderer.update_asset_pages(assessment)
    if assessment["broadcast"]:
        await page_renderer.create_assessment_page(assessment)
    
    # Layer 3: Only on broadcast
    if assessment["broadcast"]:
        if assessment["severity"] in ("alert", "critical"):
            await social_renderer.post_alert(assessment)
        if assessment["severity"] == "critical":
            await onchain_renderer.post_attestation(assessment)
    
    # Daily pulse (aggregated, not per-event)
    # Handled by pulse_renderer.generate_daily_pulse() at 07:00 UTC
```

### Page Renderer

Generates static HTML pages with embedded JSON-LD. Uses Jinja2 templates.

Every page includes:
- Human-readable risk profile
- `<script type="application/ld+json">` with full structured data
- `<link rel="alternate" type="application/json" href="/api/...">` to API endpoint
- OpenGraph tags for social preview cards
- Canonical URL that never changes

Page types:
- `/wallet/{address}` — current risk profile, updated on every assessment
- `/asset/{symbol}` — current SII + pillar breakdown, updated on SII cycle
- `/assessment/{id}` — specific assessment event, created once, never changes
- `/pulse/{date}` — daily summary, created once per day

**Implementation:** Jinja2 templates rendered by FastAPI. Pages served as static HTML from a `/pages/` directory, or rendered on-demand from the database. For V1, render on-demand (simpler). Optimize to static generation later if traffic warrants.

### Social Renderer

Formats assessment for X, Telegram, Farcaster. Posts via their respective APIs.

**X/Twitter:** Requires a developer account + API keys. Post via tweepy or httpx to Twitter API v2. Format:

```
BASIS ALERT · 2026-03-27 14:32 UTC

Wallet 0x7a25...3f4d
Increased TUSD exposure +$12M (now 40%)

TUSD SII: 71 (C+) ▼ 3.2 pts / 7d
Wallet risk: 76.4 → 72.1
HHI: 0.32 → 0.46

basis.protocol/assessment/{id}
```

**Telegram:** Bot via python-telegram-bot or httpx to Bot API. Same format. Post to a public channel.

**Farcaster:** Post via Farcaster Hub API or Neynar SDK. Same format as a cast.

**Daily Pulse:** Posted at 07:00 UTC on all three channels. Links to `/pulse/{date}` page.

### On-chain Renderer

Posts content_hash to the existing BasisOracle contract on Base/Arbitrum. Only fires on `critical` severity events.

**Implementation:** Call the keeper script (already built in basis-oracle repo) with the assessment content_hash. This is a separate process — the publisher emits an event, the keeper picks it up.

For V1, simplify: write the content_hash to a new `assessment_attestations` table. The keeper script (already built) can be extended to post these. Don't build a new on-chain integration — extend the existing one.

---

## Configuration

```python
# app/agent/config.py

AGENT_CONFIG = {
    # Watcher
    "watch_interval_minutes": 15,
    "daily_cycle_utc_hour": 0,       # 00:00 UTC, after SII scoring
    "daily_pulse_utc_hour": 7,       # 07:00 UTC, social post time
    
    # Trigger thresholds
    "movement_threshold_usd": 1_000_000,
    "score_change_threshold_pts": 3.0,
    "concentration_shift_from_pct": 20.0,
    "concentration_shift_to_pct": 40.0,
    "concentration_min_wallet_value": 500_000,
    "depeg_threshold_pct": 1.0,
    "depeg_duration_minutes": 60,
    
    # Classifier
    "divergence_sii_ceiling": 80,     # only flag divergence below this SII
    "alert_score_delta_pts": 3.0,
    "critical_score_delta_pts": 5.0,
    "critical_depeg_pct": 1.0,
    
    # Publisher
    "pages_enabled": True,
    "social_enabled": False,          # OFF by default until accounts set up
    "onchain_enabled": False,         # OFF until oracle keys funded
    "pulse_enabled": True,
    
    # Limits
    "max_assessments_per_cycle": 500, # prevent runaway on first run
    "max_broadcasts_per_day": 10,     # prevent spam
}
```

---

## Integration with Existing Codebase

### How the Agent Runs

The agent runs in the existing worker thread pattern. In `main.py`, after the SII scoring cycle, the agent watcher runs.

```python
# In main.py worker thread, after scoring cycle:
from app.agent.watcher import run_agent_cycle

# After SII scores are computed:
run_agent_cycle()
```

The agent does NOT get its own process or thread. It piggybacks on the existing worker cycle. The SII scoring engine runs hourly. The agent watcher runs after each scoring cycle (checking for triggers every hour) plus a dedicated 15-minute interval check for price-based triggers (depeg detection).

### Database Access

The agent uses the SAME database helpers as everything else: `fetch_all`, `fetch_one`, `execute` from `app/database.py`. No new connection pools. No new database.

### API Routes

New routes for assessment data:

```python
# In app/agent/api.py (registered on FastAPI app at startup)

GET /api/assessments                    — recent assessment events (paginated)
GET /api/assessments/{id}               — specific assessment event
GET /api/assessments/wallet/{address}   — assessments for a wallet
GET /api/pulse/{date}                   — daily pulse summary
GET /api/pulse/latest                   — most recent pulse
```

### Page Routes

```python
# In app/publisher/pages.py (registered on FastAPI app at startup)

GET /wallet/{address}          — rendered HTML wallet risk page
GET /asset/{symbol}            — rendered HTML asset page
GET /assessment/{id}           — rendered HTML assessment event page
GET /pulse/{date}              — rendered HTML daily pulse page
```

These serve HTML (not JSON). The JSON-LD is embedded in the HTML. The API endpoints serve JSON. Both coexist.

---

## What to Build First (Priority Order)

1. **Migration 014** — create assessment_events and daily_pulses tables
2. **app/agent/config.py** — thresholds and toggles
3. **app/agent/watcher.py** — trigger detection (start with daily_cycle + score_change only)
4. **app/agent/assessor.py** — assessment event generation
5. **app/agent/classifier.py** — severity assignment + divergence detection
6. **app/agent/store.py** — canonical database write
7. **app/agent/api.py** — API routes for assessment data
8. **Integration in main.py** — wire agent into worker cycle
9. **app/publisher/pulse_renderer.py** — daily pulse generation
10. **app/publisher/page_renderer.py** — wallet/asset/assessment pages with JSON-LD
11. **Templates** — Jinja2 templates for pages
12. **app/publisher/social_renderer.py** — X/Telegram/Farcaster posting (after accounts set up)
13. **app/publisher/onchain_renderer.py** — content hash attestation (after oracle funded)

Items 1-8 are the core. They make the agent run and accumulate state. Items 9-11 make it visible. Items 12-13 are amplification layers added after core is proven.

---

## Do NOT

- Create a new database or connection pool
- Import from spoke repos (basis-mcp, basis-safe, etc.)
- Modify scoring.py, worker.py, or existing server.py routes
- Build a chatbot, conversation interface, or interactive agent
- Add external dependencies without asking (use httpx for HTTP, Jinja2 for templates — both already in requirements)
- Post to social media before social_enabled is flipped to True
- Post on-chain before onchain_enabled is flipped to True and wallet is funded
- Exceed max_broadcasts_per_day (prevent spam)
- Add commentary, opinion, or recommendation to any assessment output
- Use the word "rating" anywhere

---

## Success Criteria

After this build:

- [ ] Agent runs after every SII scoring cycle
- [ ] Assessment events are stored in assessment_events table
- [ ] Content hashes are computed for every assessment
- [ ] Daily pulse generates at 00:00 UTC
- [ ] `/api/assessments` returns recent events
- [ ] `/api/pulse/latest` returns the most recent pulse
- [ ] `/wallet/{address}` serves an HTML page with JSON-LD
- [ ] `/asset/{symbol}` serves an HTML page with JSON-LD
- [ ] Assessment count in database grows every hour
- [ ] Divergence detection correctly identifies capital flowing toward declining assets

This is the minimum viable accumulation engine. Everything else — social posting, on-chain attestation, governance injection — is amplification added after the core is proven.
