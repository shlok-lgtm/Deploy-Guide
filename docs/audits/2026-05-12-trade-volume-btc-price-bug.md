# `trade_volume_24h_usd` BTC-price bug — investigation

**Date:** 2026-05-12
**Status:** Investigation only — no code changes in this pass.
**Scope:** Data-integrity bug in the exchange-data ingest path; sibling bug in
CXRI on-chain signals; audit of other hardcoded market values in the same
modules.

## 1. Producer sites (where the field is computed)

### Primary producer

**`app/data_layer/exchange_collector.py:240`** — the bug as described.

```python
btc_vol = data.get("trade_volume_24h_btc")   # line 237, from CoinGecko /exchanges/{id}
if btc_vol:
    # Use a rough BTC price — will be refined with live price
    snapshot["trade_volume_24h_usd"] = btc_vol * 65000   # line 240
```

- Constant: `65000`.
- Author flagged this with two comments (`# Computed from BTC price` at line 222 and `# Rough estimate` at line 240) and a TODO-style note (`will be refined with live price`) that never got implemented.
- The value is then persisted by `_store_exchange_snapshots` at lines 162-180 into `exchange_snapshots.trade_volume_24h_usd` (column declared in `migrations/058_universal_data_layer.sql:215`, replayed onto prod by `migrations/109_exchange_snapshots_drift_replay.sql:52` on 2026-05-12).
- Cycle entry point: `main.py:335` → `run_exchange_collection_scheduled()` → `run_exchange_collection()` → 15 exchanges in `TOP_EXCHANGES`, hourly when `>= _EXCHANGE_FRESHNESS_MINUTES (50)` old.

### Sibling producer (same antipattern, different field, different constant)

**`app/collectors/cex_collector.py:329`** — same `vol_btc × hardcoded_btc_price` pattern, written into a differently-named field.

```python
vol_btc = cg_data.get("trade_volume_24h_btc")   # line 326
if vol_btc:
    # Very rough: known wallet balance proxy (exchange with more volume = larger reserves)
    raw["known_wallet_balance"] = vol_btc * 60000   # line 329
```

- Constant: `60000`. **Note: the two collectors disagree on BTC price — 65k vs 60k — in the same codebase.**
- Feeds CXRI scoring via `extract_cex_raw_values` → `score_cex` → `score_entity` → `generic_index_scores` (write at `app/collectors/cex_collector.py:397-438`).
- The component definition (`app/index_definitions/cxri_v01.py:223-229`) names this `Known Wallet Total Balance (USD)` with a log normalization at `{100M:10, 1B:30, 5B:50, 10B:70, 50B:100}`. With BTC near actual market price, the multiplier is wrong by ~40 %, which can shift exchanges across bracket boundaries (esp. the 1B → 5B and 5B → 10B steps).

## 2. Downstream blast radius

### `exchange_snapshots.trade_volume_24h_usd`

| # | Consumer | File:line | Notes |
|---|----------|-----------|-------|
| 1 | DB column | `migrations/058_universal_data_layer.sql:215`; replay `migrations/109_exchange_snapshots_drift_replay.sql:52` | The number lives in the canonical row. |
| 2 | REST: `GET /api/data/exchanges?exchange_id=…` | `app/server.py:7874-7881` | `SELECT *` — returns the wrong USD verbatim to API callers. |
| 3 | REST: `GET /api/data/exchanges` (list mode) | `app/server.py:7883-7889` | Selects the field **and orders by it** (`ORDER BY trade_volume_24h_usd DESC NULLS LAST`). Sort order is stable today only because the multiplier is uniform across rows — but the absolute USD figure is wrong. |
| 4 | MCP tool: `basis_exchange_health` | `app/mcp_server.py:393-413` | Proxies the same endpoint — every agent/LLM caller gets the wrong USD. |
| 5 | Index simulator metric registry | `app/data_layer/index_simulator.py:70-76` | Exposes `trade_volume_24h_usd` as a simulatable metric for third-party indices. **Related bug** — see §5 below. |
| 6 | Coherence guards (exchange validation) | `app/data_layer/coherence_guards.py:323-364` | Reads `trust_score` only, **not** `trade_volume_24h_usd`. No alert path triggers off the wrong volume. |
| 7 | Frontend | — | No App.jsx / OpsDashboard consumer of this endpoint or field. Confirmed by grep. |
| 8 | dbt models | — | No dbt model references `exchange_snapshots` or `trade_volume_24h_usd`. |
| 9 | Tests | `test_data_layer.py:97` | Only does a `COUNT(*)` — does not read the column. No regression test pins the value. |

The `db_schema_validator.py:109-113` allowlist for `exchange_snapshots` doesn't even list `trade_volume_24h_usd` (or any other migration-109 column) — separate ticket (§5).

### `known_wallet_balance` (the sibling bug, `cex_collector.py:329`)

| # | Consumer | File:line | Notes |
|---|----------|-----------|-------|
| 1 | Scoring engine | `app/scoring_engine.score_entity` invoked at `app/collectors/cex_collector.py:389` | Log-normalized via `cxri_v01.py:227`, weight 0.30 inside `onchain_signals` category. |
| 2 | DB | `generic_index_scores` (`raw_values` JSONB and component score row) | Wrong number cached for the day; backfill won't fix retroactively. |
| 3 | REST: `GET /api/circle7/{idx}/scores` (CXRI included) | `app/server.py:6267-6297` (`valid_indices` at 6268; CXRI def loaded at 6292) | Publicly served. |
| 4 | Daily pulse | `app/pulse_generator.py:255` (`cxri_components`) | Wrong score gets snapshotted into the daily risk surface. |
| 5 | Track record | `app/track_record.py:92`, `app/track_record_followups.py:69` | Historical CXRI score is locked in. |
| 6 | Integrity / freshness | `app/integrity.py:450-451` | Freshness query only — doesn't validate the value. |
| 7 | Component coverage map | `app/component_coverage.py:191` | Marks `known_wallet_balance` as `"live"`. Misleading — it's a hardcoded fabrication, not a live signal. |
| 8 | Coherence | `app/coherence.py:47,86` (`cxri_components`, freshness 24h) | Coverage only, no value checks. |
| 9 | Backfill | `scripts/backfill/backfill_cxri.py:135-181` | Writes `raw_values["trade_volume_24h"] = round(daily_volume, 2)` (line 181) — a **third** field name (USD-denominated raw daily volume from CoinGecko volume_chart, no BTC multiplier). So the backfill path and the live path disagree on field name *and* unit; the live path is the buggy one. |
| 10 | Worker dispatch | `app/worker.py:1763-1766`, `app/enrichment_worker.py:332-357` | Calls `run_cxri_scoring` cyclically. |

## 3. Other hardcoded market values in the same modules

### `app/data_layer/exchange_collector.py`

| Line | Value | Decays? | Risk |
|------|-------|---------|------|
| 34-39 | `TOP_EXCHANGES` (15 fixed slugs) | Slowly | Operational — list staleness over months. |
| 45-50 | `_EX_FIX` legacy slug remap | No | Stable — CoinGecko-side names. |
| 128 | `stablecoins = {"USDC","USDT","DAI","FRAX","PYUSD","FDUSD","USDE","TUSD","USDD","USD1"}` | **Yes** | **Violates the CLAUDE.md rule "NEVER hardcode stablecoin lists — registry is dynamic"**. Misses newly-promoted assets; ticker filter silently drops them. |
| 227 | `stablecoin_pairs[:50]` truncation | No | Truncation; not market-data drift. |
| 240 | `* 65000` (BTC) | **Yes** | The reported bug. |
| 311 | `_EXCHANGE_FRESHNESS_MINUTES = 50` | No | Operational. |

### `app/collectors/cex_collector.py`

| Line | Value | Decays? | Risk |
|------|-------|---------|------|
| 43-148 | `CEX_STATIC_CONFIG` (PoR method/frequency, license counts, MiCA status, US licensing, jurisdiction quality, security_breach_count, insurance_coverage, …) | **Yes (slow)** | Multi-month drift: regulatory landscape (MiCA Article 67, US state licenses) changes quarterly; PoR cadence changes when exchanges revise programs. `years_in_operation` is overridden live, the rest are not. |
| 192-201 | `CEX_API_ENDPOINTS` (8 URLs) | Rare | Domain/path changes break health checks. |
| 204-212 | `CEX_RESERVE_URLS` (7 URLs) | **Yes (moderate)** | PoR-page URLs are restructured fairly often — `realtime_reserve_dashboard` silently flips to 20 on 404 (line 293). |
| 269-275 | Magic scores `95 / 60 / 40 / 50` for API outcomes | No | Calibration; non-market. |
| 291-293 | Magic scores `80 / 20` for dashboard liveness | No | Calibration; non-market. |
| 323 | `trust * 10` rescaling 1-10 → 0-100 | No | Unit-only; safe. |
| 329 | `* 60000` (BTC) | **Yes** | Sibling of the reported bug, different constant. |

No FX rates, gas prices, token supplies, or chain-specific conversion factors in either module.

## 4. Fix plan

The reported field is genuinely two bugs in one module family. Any fix should address `exchange_collector.py:240` **and** `cex_collector.py:329` together, since they share the antipattern and a code-search for new occurrences will produce both.

### Shared groundwork (needed for all options)

A canonical BTC USD price reader. The codebase already calls CoinGecko `/simple/price` from `app/collectors/oracle_behavior.py:278-282` and `app/collectors/lst_collector.py:144`, and has `app/collectors/coingecko.py` (`fetch_current` / `extract_price_context`) already imported in `app/worker.py:39`. A small `get_btc_usd_price()` helper that consults `historical_prices` (table populated by `app/services/historical_backfill.py:90-91`) with a CG fallback is the natural fit — no new dependency.

### Option A — Fetch BTC price at ingest, keep the precomputed USD field

Get BTC USD once per exchange-collection cycle (one extra CG call, or a single `historical_prices` lookup), pass it into both producer sites.

- Edits: `exchange_collector.py:237-240`, `cex_collector.py:326-329`. ~6-10 lines each.
- Migration cost: **zero** — column shape and consumer contracts unchanged.
- Downstream changes needed: **none**. All 10+ readers keep working.
- Risk: still silently wrong if the BTC fetch fails — needs an explicit `None` path (don't fall back to 0; that's worse than null).
- Doesn't help retroactively for already-stored rows.

### Option B — Drop the precomputed USD field; compute downstream from a live price table

Store only `trade_volume_24h_btc` and let readers join against a `btc_usd_price_at(ts)` view.

- Edits: drop INSERT of `trade_volume_24h_usd`; rewrite `/api/data/exchanges` (`server.py:7883-7889`) and `mcp_server.py:393-413` to compute USD on the fly; remove the field from `index_simulator.py:70-76`; fix the `ORDER BY` to use `trade_volume_24h_btc DESC`.
- Migration cost: **medium**. A new migration (086) to keep the column for historical replay but mark it deprecated; or drop it (breaks SSR cached responses + MCP shape).
- Downstream changes needed: 4 sites — `server.py:7884`, `mcp_server.py` (passthrough — auto), `index_simulator.py:74` (metric registry), and the integration test in `test_data_layer.py:97` (no value asserted, safe).
- Cleanest semantically — single source of price truth — but touches public REST contracts.

### Option C — Keep the field, add `price_used` + `price_as_of` provenance columns

Same as A but each row carries the BTC price and timestamp used. Consumers can detect staleness; auditors can replay.

- Edits: producer changes from A, **plus** migration 086 adding two columns (`price_used NUMERIC`, `price_as_of TIMESTAMPTZ`). Add fields to `exchange_collector.py:162-180` INSERT and `:216-234` snapshot dict. Surface in `server.py:7884` SELECT (additive) and `mcp_server` schema.
- Migration cost: **low** — additive, idempotent `ADD COLUMN IF NOT EXISTS`.
- Downstream changes needed: only consumers that want to *use* the provenance — current readers ignore the new columns and keep working. `db_schema_validator.py:109-113` allowlist would need to be updated to recognise the new columns (it currently doesn't even know about `trade_volume_24h_usd` — see §5).
- Best fit for the project's stated "computation attestation" pattern (CLAUDE.md §"Architectural Patterns" #3). Matches `data_provenance` and `provenance_proofs` conventions already in use.

**Recommendation, briefly:** Option C — additive, audit-friendly, no breakage. Apply the same `price_used` / `price_as_of` pattern to `cex_collector.py:329` by storing the price alongside the `known_wallet_balance` raw value in the `generic_index_scores.raw_values` JSONB (no schema change needed for CXRI). Don't backfill historical rows — flag them as stale via `price_as_of IS NULL`.

## 5. Related bugs found (separate tickets — not fixed here)

1. **Two different hardcoded BTC prices in the same codebase.** `exchange_collector.py:240` uses `65000`; `cex_collector.py:329` uses `60000`. Even a "rough estimate" should be consistent.

2. **`db_schema_validator.py:109-113`** doesn't list `trade_volume_24h_usd`, `has_trading_incentive`, `stablecoin_pairs`, or `raw_data` for `exchange_snapshots`, despite migration 109 replaying them onto prod. The validator's known-columns map is out of sync with the schema.

3. **`index_simulator.py:217`** clamps any raw value to `min(100, max(0, float(value)))`. A multi-billion-USD `trade_volume_24h_usd` becomes a literal `100`. This hides the BTC-multiplier bug **and** is a unit/scaling bug that breaks every USD-denominated metric in `DATA_SOURCE_REGISTRY` (`tvl_usd`, `volume_24h`, etc.).

4. **Naming/unit mismatch in `cex_collector.py:329`.** The field is called `known_wallet_balance` ("Known Wallet Total Balance (USD)" per `cxri_v01.py:224`), but the value is `vol_btc × hardcoded_price` — 24-hour *flow*, not *balance*. Even with a correct BTC price this is the wrong physical quantity.

5. **`component_coverage.py:191`** flags `known_wallet_balance` as `"live"`. With the current implementation it is *not* live — it's a hardcoded fabrication. Misleads coverage dashboards.

6. **Backfill / live disagreement.** `scripts/backfill/backfill_cxri.py:181` writes the CoinGecko-provided daily USD volume into `raw_values["trade_volume_24h"]`, while the live `extract_cex_raw_values` writes a different-named field (`known_wallet_balance`) with a *different unit transformation*. Historical CXRI scores and live CXRI scores are not directly comparable.

7. **Silent-zero risk in Option A's failure mode.** Not a current bug, but worth pre-empting: if `get_btc_usd_price()` returns `None` and a producer falls back to `0` or `None * btc_vol`, downstream `ORDER BY trade_volume_24h_usd DESC NULLS LAST` will silently re-rank exchanges. Producers must propagate `None` cleanly and the API contract must document it.

8. **`exchange_collector.py:128`** hardcodes the stablecoin set, violating CLAUDE.md's "registry is dynamic" rule. New assets auto-promoted from the backlog won't appear in `stablecoin_pairs`.
