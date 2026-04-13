-- Migration 058: Universal Risk Data Layer — foundation tables
-- API usage tracking, rate limiter state, and time-series data tables

-- =============================================================================
-- API Usage Tracking (Phase 3G)
-- =============================================================================

CREATE TABLE IF NOT EXISTS api_usage_tracker (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL,           -- coingecko, etherscan, defillama, snapshot, etc.
    endpoint TEXT NOT NULL,           -- /coins/{id}, /v2/api, etc.
    calls_count INTEGER NOT NULL DEFAULT 1,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    caller TEXT,                      -- module/collector that made the call
    response_status INTEGER,          -- HTTP status code
    latency_ms INTEGER               -- response time
);

CREATE INDEX IF NOT EXISTS idx_api_usage_provider_time
    ON api_usage_tracker(provider, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_usage_recorded
    ON api_usage_tracker(recorded_at DESC);

-- Hourly rollup for dashboard queries
CREATE TABLE IF NOT EXISTS api_usage_hourly (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    hour TIMESTAMPTZ NOT NULL,
    total_calls INTEGER NOT NULL DEFAULT 0,
    success_calls INTEGER NOT NULL DEFAULT 0,
    error_calls INTEGER NOT NULL DEFAULT 0,
    avg_latency_ms INTEGER,
    p95_latency_ms INTEGER,
    callers JSONB,                    -- {"coingecko_collector": 45, "lst_collector": 12}
    UNIQUE(provider, hour)
);

CREATE INDEX IF NOT EXISTS idx_api_usage_hourly_provider
    ON api_usage_hourly(provider, hour DESC);

-- Provider limits reference (static config, updated on deploy)
CREATE TABLE IF NOT EXISTS api_provider_limits (
    provider TEXT PRIMARY KEY,
    calls_per_second NUMERIC,
    calls_per_minute NUMERIC,
    calls_per_day NUMERIC,
    calls_per_month NUMERIC,
    plan_tier TEXT,                    -- free, pro, analyst, etc.
    notes TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed known provider limits
INSERT INTO api_provider_limits (provider, calls_per_second, calls_per_minute, calls_per_day, calls_per_month, plan_tier, notes)
VALUES
    ('coingecko', 8.3, 500, 16600, 500000, 'analyst', 'CoinGecko Analyst plan. Overage: $250 per extra 500K.'),
    ('etherscan', 10, 600, 200000, NULL, 'standard', 'Etherscan Standard. 10 req/s. 200K/day hard cap. V2 multi-chain (ETH+Base+Arb on one key).'),
    ('blockscout', 5, 300, 100000, NULL, 'free', 'Blockscout Free. 5 req/s. 100K credits/day shared across all chains.'),
    ('defillama', NULL, NULL, NULL, NULL, 'free', 'DeFiLlama Free. No key required. Generous limits.'),
    ('snapshot', NULL, NULL, NULL, NULL, 'free', 'Snapshot GraphQL. No key required.'),
    ('tally', NULL, NULL, NULL, NULL, 'free', 'Tally GraphQL. No key required.'),
    ('helius', 10, 600, NULL, NULL, 'free', 'Helius Free. 10 RPS. 1M credits/month.'),
    ('immunefi', NULL, NULL, NULL, NULL, 'free', 'Immunefi public API. No key required.')
ON CONFLICT (provider) DO UPDATE SET
    calls_per_second = EXCLUDED.calls_per_second,
    calls_per_minute = EXCLUDED.calls_per_minute,
    calls_per_day = EXCLUDED.calls_per_day,
    calls_per_month = EXCLUDED.calls_per_month,
    plan_tier = EXCLUDED.plan_tier,
    notes = EXCLUDED.notes,
    updated_at = NOW();

-- =============================================================================
-- Universal Data Layer — Tier 1: Per-Asset Liquidity Depth
-- =============================================================================

CREATE TABLE IF NOT EXISTS liquidity_depth (
    id BIGSERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,            -- coingecko_id or contract address
    venue TEXT NOT NULL,               -- uniswap_v3, curve, binance, etc.
    venue_type TEXT NOT NULL,          -- dex or cex
    chain TEXT,                        -- ethereum, base, arbitrum (NULL for CEX)
    pool_address TEXT,                 -- DEX pool contract (NULL for CEX)
    bid_depth_1pct NUMERIC,           -- USD depth within 1% of mid
    ask_depth_1pct NUMERIC,
    bid_depth_2pct NUMERIC,
    ask_depth_2pct NUMERIC,
    spread_bps NUMERIC,               -- bid-ask spread in basis points
    volume_24h NUMERIC,
    trade_count_24h INTEGER,
    buy_sell_ratio NUMERIC,           -- buy volume / sell volume
    trust_score TEXT,                  -- CoinGecko trust score for CEX
    liquidity_score NUMERIC,          -- normalized 0-100
    raw_data JSONB,                   -- full API response for replay
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(asset_id, venue, chain, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_liquidity_depth_asset
    ON liquidity_depth(asset_id, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_liquidity_depth_venue
    ON liquidity_depth(venue, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_liquidity_depth_time
    ON liquidity_depth(snapshot_at DESC);

-- =============================================================================
-- Universal Data Layer — Tier 2: Yield and Rate Data
-- =============================================================================

CREATE TABLE IF NOT EXISTS yield_snapshots (
    id BIGSERIAL PRIMARY KEY,
    pool_id TEXT NOT NULL,             -- DeFiLlama pool UUID
    protocol TEXT NOT NULL,            -- aave-v3, compound-v3, etc.
    chain TEXT NOT NULL,
    asset TEXT NOT NULL,               -- underlying asset symbol
    apy NUMERIC,                      -- current APY
    apy_base NUMERIC,                 -- base APY (no rewards)
    apy_reward NUMERIC,               -- reward APY
    tvl_usd NUMERIC,
    utilization NUMERIC,              -- utilization rate 0-1
    il_risk TEXT,                      -- impermanent loss risk level
    exposure TEXT,                     -- single, multi
    stable_pool BOOLEAN,
    pool_meta JSONB,                  -- full pool metadata
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(pool_id, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_yield_snapshots_protocol
    ON yield_snapshots(protocol, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_yield_snapshots_asset
    ON yield_snapshots(asset, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_yield_snapshots_time
    ON yield_snapshots(snapshot_at DESC);

-- =============================================================================
-- Universal Data Layer — Tier 3: Governance Activity
-- =============================================================================

CREATE TABLE IF NOT EXISTS governance_proposals (
    id SERIAL PRIMARY KEY,
    protocol TEXT NOT NULL,
    source TEXT NOT NULL,              -- snapshot, tally, onchain
    proposal_id TEXT NOT NULL,
    title TEXT,
    state TEXT,                        -- active, closed, pending, executed
    author TEXT,
    created_at TIMESTAMPTZ,
    start_at TIMESTAMPTZ,
    end_at TIMESTAMPTZ,
    votes_for NUMERIC,
    votes_against NUMERIC,
    votes_abstain NUMERIC,
    voter_count INTEGER,
    quorum_reached BOOLEAN,
    scores JSONB,                     -- per-choice scores
    raw_data JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(protocol, source, proposal_id)
);

CREATE INDEX IF NOT EXISTS idx_gov_proposals_protocol
    ON governance_proposals(protocol, created_at DESC);

CREATE TABLE IF NOT EXISTS governance_voters (
    id BIGSERIAL PRIMARY KEY,
    protocol TEXT NOT NULL,
    proposal_id TEXT NOT NULL,
    voter_address TEXT NOT NULL,
    voting_power NUMERIC,
    choice INTEGER,
    created_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(protocol, proposal_id, voter_address)
);

CREATE INDEX IF NOT EXISTS idx_gov_voters_protocol
    ON governance_voters(protocol, collected_at DESC);

-- =============================================================================
-- Universal Data Layer — Tier 4: Bridge Flow Volumes
-- =============================================================================

CREATE TABLE IF NOT EXISTS bridge_flows (
    id BIGSERIAL PRIMARY KEY,
    bridge_id TEXT NOT NULL,           -- DeFiLlama bridge ID or slug
    bridge_name TEXT,
    source_chain TEXT NOT NULL,
    dest_chain TEXT NOT NULL,
    volume_usd NUMERIC,               -- directional volume
    txn_count INTEGER,
    tvl_usd NUMERIC,                  -- TVL at time of snapshot
    period TEXT NOT NULL DEFAULT '24h', -- 24h, 7d, 30d
    raw_data JSONB,
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(bridge_id, source_chain, dest_chain, period, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_bridge_flows_bridge
    ON bridge_flows(bridge_id, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_bridge_flows_chains
    ON bridge_flows(source_chain, dest_chain, snapshot_at DESC);

-- =============================================================================
-- Universal Data Layer — Tier 5: Exchange-Level Data
-- =============================================================================

CREATE TABLE IF NOT EXISTS exchange_snapshots (
    id BIGSERIAL PRIMARY KEY,
    exchange_id TEXT NOT NULL,         -- CoinGecko exchange ID
    name TEXT,
    trust_score INTEGER,              -- CoinGecko trust score 1-10
    trust_score_rank INTEGER,
    trade_volume_24h_btc NUMERIC,
    trade_volume_24h_usd NUMERIC,
    year_established INTEGER,
    country TEXT,
    trading_pairs INTEGER,
    has_trading_incentive BOOLEAN,
    stablecoin_pairs JSONB,           -- stablecoin-specific ticker data
    raw_data JSONB,
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(exchange_id, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_exchange_snapshots_id
    ON exchange_snapshots(exchange_id, snapshot_at DESC);

-- =============================================================================
-- Universal Data Layer — Tier 6: Cross-Entity Correlation
-- =============================================================================

CREATE TABLE IF NOT EXISTS correlation_matrices (
    id SERIAL PRIMARY KEY,
    matrix_type TEXT NOT NULL,        -- sii_30d, psi_30d, cross_90d
    window_days INTEGER NOT NULL,     -- 30, 90
    entity_ids JSONB NOT NULL,        -- ordered list of entity IDs
    matrix_data JSONB NOT NULL,       -- 2D correlation matrix
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(matrix_type, window_days, computed_at)
);

CREATE INDEX IF NOT EXISTS idx_correlation_type
    ON correlation_matrices(matrix_type, computed_at DESC);

-- =============================================================================
-- Universal Data Layer — Tier 7: Historical Volatility Surfaces
-- =============================================================================

CREATE TABLE IF NOT EXISTS volatility_surfaces (
    id BIGSERIAL PRIMARY KEY,
    asset_id TEXT NOT NULL,
    realized_vol_1d NUMERIC,
    realized_vol_7d NUMERIC,
    realized_vol_30d NUMERIC,
    realized_vol_90d NUMERIC,
    max_drawdown_7d NUMERIC,
    max_drawdown_30d NUMERIC,
    max_drawdown_90d NUMERIC,
    recovery_time_hours NUMERIC,      -- time from max drawdown to recovery
    correlation_btc_30d NUMERIC,
    correlation_eth_30d NUMERIC,
    correlation_usd_30d NUMERIC,      -- vs USD index
    raw_prices JSONB,                 -- price array used for computation
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(asset_id, computed_at)
);

CREATE INDEX IF NOT EXISTS idx_vol_surfaces_asset
    ON volatility_surfaces(asset_id, computed_at DESC);

-- =============================================================================
-- Universal Data Layer — Tier 8: Structured Incident History
-- =============================================================================

CREATE TABLE IF NOT EXISTS incident_events (
    id SERIAL PRIMARY KEY,
    entity_id TEXT NOT NULL,          -- affected entity (coin ID, protocol slug, etc.)
    entity_type TEXT NOT NULL,        -- stablecoin, protocol, bridge, exchange
    incident_type TEXT NOT NULL,      -- exploit, depeg, oracle_failure, governance_attack, etc.
    severity TEXT NOT NULL,           -- critical, high, medium, low
    title TEXT NOT NULL,
    description TEXT,
    started_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    loss_usd NUMERIC,                -- estimated financial impact
    affected_entities JSONB,         -- other entities impacted
    sources JSONB,                   -- URLs, discovery signals, etc.
    detection_method TEXT,           -- automated, manual, external
    raw_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(entity_id, incident_type, started_at)
);

CREATE INDEX IF NOT EXISTS idx_incidents_entity
    ON incident_events(entity_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_incidents_type
    ON incident_events(incident_type, started_at DESC);

-- =============================================================================
-- 5-Minute Peg Resolution
-- =============================================================================

CREATE TABLE IF NOT EXISTS peg_snapshots_5m (
    id BIGSERIAL PRIMARY KEY,
    stablecoin_id TEXT NOT NULL,
    price NUMERIC NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    deviation_bps NUMERIC,           -- deviation from $1.00 in basis points
    UNIQUE(stablecoin_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_peg_5m_coin_time
    ON peg_snapshots_5m(stablecoin_id, timestamp DESC);

-- =============================================================================
-- Per-Event Mint/Burn
-- =============================================================================

CREATE TABLE IF NOT EXISTS mint_burn_events (
    id BIGSERIAL PRIMARY KEY,
    stablecoin_id TEXT NOT NULL,
    chain TEXT NOT NULL,
    event_type TEXT NOT NULL,          -- mint or burn
    amount NUMERIC NOT NULL,
    tx_hash TEXT NOT NULL,
    block_number BIGINT,
    from_address TEXT,
    to_address TEXT,
    timestamp TIMESTAMPTZ,
    raw_data JSONB,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(chain, tx_hash, event_type)
);

CREATE INDEX IF NOT EXISTS idx_mint_burn_coin
    ON mint_burn_events(stablecoin_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_mint_burn_chain
    ON mint_burn_events(chain, timestamp DESC);

-- =============================================================================
-- Hourly Entity Snapshots
-- =============================================================================

CREATE TABLE IF NOT EXISTS entity_snapshots_hourly (
    id BIGSERIAL PRIMARY KEY,
    entity_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,        -- stablecoin, protocol_token, circle7
    market_cap NUMERIC,
    total_volume NUMERIC,
    price_usd NUMERIC,
    price_change_24h NUMERIC,
    circulating_supply NUMERIC,
    total_supply NUMERIC,
    exchange_tickers_count INTEGER,
    developer_data JSONB,
    community_data JSONB,
    raw_data JSONB,
    snapshot_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(entity_id, entity_type, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_entity_snapshots_id
    ON entity_snapshots_hourly(entity_id, snapshot_at DESC);

-- =============================================================================
-- Contract Surveillance
-- =============================================================================

CREATE TABLE IF NOT EXISTS contract_surveillance (
    id SERIAL PRIMARY KEY,
    entity_id TEXT NOT NULL,
    chain TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    has_admin_keys BOOLEAN,
    is_upgradeable BOOLEAN,
    has_pause_function BOOLEAN,
    has_blacklist BOOLEAN,
    timelock_hours NUMERIC,
    multisig_threshold TEXT,          -- e.g. "3/5"
    source_code_hash TEXT,            -- SHA256 of verified source
    analysis JSONB,                   -- full analysis result
    scanned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(entity_id, chain, contract_address, scanned_at)
);

CREATE INDEX IF NOT EXISTS idx_contract_surv_entity
    ON contract_surveillance(entity_id, scanned_at DESC);

-- =============================================================================
-- Wallet Behavioral Classification
-- =============================================================================

CREATE TABLE IF NOT EXISTS wallet_behavior_tags (
    id BIGSERIAL PRIMARY KEY,
    wallet_address TEXT NOT NULL,
    behavior_type TEXT NOT NULL,       -- accumulator, distributor, rotator, bridge_user, etc.
    confidence NUMERIC,               -- 0-1 confidence in classification
    metrics JSONB,                    -- supporting metrics
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(wallet_address, behavior_type, computed_at)
);

CREATE INDEX IF NOT EXISTS idx_wallet_behavior_addr
    ON wallet_behavior_tags(wallet_address, computed_at DESC);

-- =============================================================================
-- Data Catalog Registry
-- =============================================================================

CREATE TABLE IF NOT EXISTS data_catalog (
    data_type TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    source_table TEXT NOT NULL,
    update_frequency TEXT NOT NULL,    -- 5m, 15m, hourly, daily
    providers JSONB,                  -- ["coingecko", "defillama"]
    provenance_status TEXT NOT NULL DEFAULT 'unproven',  -- proven, unproven
    earliest_record TIMESTAMPTZ,
    latest_record TIMESTAMPTZ,
    row_count BIGINT DEFAULT 0,
    schema_info JSONB,               -- column descriptions
    used_by_indices JSONB,           -- ["sii", "psi", "bri"]
    integrity_domain TEXT,
    staleness_threshold_hours NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO migrations (name) VALUES ('058_universal_data_layer') ON CONFLICT DO NOTHING;
