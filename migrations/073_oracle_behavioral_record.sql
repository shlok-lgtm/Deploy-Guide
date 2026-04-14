-- Migration 073: Oracle Behavioral Record (Pipeline 10)
-- Continuously records oracle price feed behavior: deviation from CEX
-- prices and update latency for every oracle feeding scored entities.

CREATE TABLE IF NOT EXISTS oracle_price_readings (
    id SERIAL PRIMARY KEY,
    oracle_address VARCHAR(42) NOT NULL,
    oracle_name VARCHAR(200),
    oracle_provider VARCHAR(50),
    chain VARCHAR(20) NOT NULL,
    asset_symbol VARCHAR(20) NOT NULL,
    quote_symbol VARCHAR(20) NOT NULL DEFAULT 'usd',
    oracle_price DECIMAL(30,8) NOT NULL,
    oracle_price_raw VARCHAR(200),
    oracle_decimals INTEGER,
    cex_price DECIMAL(30,8),
    deviation_pct DECIMAL(10,6),
    deviation_abs DECIMAL(20,8),
    latency_seconds INTEGER,
    round_id VARCHAR(100),
    answer_timestamp TIMESTAMPTZ,
    recorded_at TIMESTAMPTZ DEFAULT NOW(),
    is_stress_event BOOLEAN DEFAULT FALSE,
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oracle_readings_addr_time
    ON oracle_price_readings (oracle_address, recorded_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oracle_readings_asset_time
    ON oracle_price_readings (asset_symbol, recorded_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oracle_readings_stress
    ON oracle_price_readings (is_stress_event) WHERE is_stress_event = TRUE;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oracle_readings_time
    ON oracle_price_readings (recorded_at DESC);


CREATE TABLE IF NOT EXISTS oracle_stress_events (
    id SERIAL PRIMARY KEY,
    oracle_address VARCHAR(42) NOT NULL,
    oracle_name VARCHAR(200),
    asset_symbol VARCHAR(20) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    event_type VARCHAR(50),
    event_start TIMESTAMPTZ NOT NULL,
    event_end TIMESTAMPTZ,
    duration_seconds INTEGER,
    max_deviation_pct DECIMAL(10,6),
    max_latency_seconds INTEGER,
    reading_count INTEGER DEFAULT 1,
    concurrent_sii_score DECIMAL(6,2),
    concurrent_psi_scores JSONB,
    affected_protocols JSONB,
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oracle_stress_asset_time
    ON oracle_stress_events (asset_symbol, event_start DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oracle_stress_type
    ON oracle_stress_events (event_type);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_oracle_stress_start
    ON oracle_stress_events (event_start DESC);


CREATE TABLE IF NOT EXISTS oracle_registry (
    id SERIAL PRIMARY KEY,
    oracle_address VARCHAR(42) NOT NULL,
    oracle_name VARCHAR(200) NOT NULL,
    oracle_provider VARCHAR(50) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    asset_symbol VARCHAR(20) NOT NULL,
    quote_symbol VARCHAR(20) NOT NULL DEFAULT 'usd',
    decimals INTEGER NOT NULL DEFAULT 8,
    read_function VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    entity_type VARCHAR(20),
    entity_slug VARCHAR(100),
    added_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (oracle_address, chain, asset_symbol)
);


-- Seed known oracle feeds
INSERT INTO oracle_registry
    (oracle_address, oracle_name, oracle_provider, chain, asset_symbol, quote_symbol,
     decimals, read_function, entity_type, entity_slug)
VALUES
    ('0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419', 'Chainlink ETH/USD', 'chainlink',
     'ethereum', 'ETH', 'usd', 8, 'latestRoundData', 'stablecoin', NULL),
    ('0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6', 'Chainlink USDC/USD', 'chainlink',
     'ethereum', 'USDC', 'usd', 8, 'latestRoundData', 'stablecoin', 'usdc'),
    ('0x3E7d1eAB13ad0104d2750B8863b489D65364e32D', 'Chainlink USDT/USD', 'chainlink',
     'ethereum', 'USDT', 'usd', 8, 'latestRoundData', 'stablecoin', 'usdt'),
    ('0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9', 'Chainlink DAI/USD', 'chainlink',
     'ethereum', 'DAI', 'usd', 8, 'latestRoundData', 'stablecoin', 'dai'),
    ('0xF4030086522a5bEEa4988F8cA5B36dbC97BeE88c', 'Chainlink BTC/USD', 'chainlink',
     'ethereum', 'BTC', 'usd', 8, 'latestRoundData', NULL, NULL),
    ('0x86392dC19c0b719886221c78AB11eb8Cf5c52812', 'Chainlink stETH/ETH', 'chainlink',
     'ethereum', 'stETH', 'eth', 18, 'latestRoundData', 'stablecoin', 'steth'),
    ('0x8250f4aF4B972684F7b336503E2D6dFeDeB1487a', 'Pyth USDC/USD', 'pyth',
     'base', 'USDC', 'usd', 6, 'getPrice', 'stablecoin', 'usdc')
ON CONFLICT (oracle_address, chain, asset_symbol) DO NOTHING;


INSERT INTO migrations (name) VALUES ('073_oracle_behavioral_record') ON CONFLICT DO NOTHING;
