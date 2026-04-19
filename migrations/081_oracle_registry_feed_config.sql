-- Migration 081: Per-feed oracle configuration
-- Adds deviation_threshold_pct and heartbeat_seconds to oracle_registry
-- so the stress detector uses feed-specific thresholds instead of global constants.

ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS deviation_threshold_pct DECIMAL(10,4);
ALTER TABLE oracle_registry ADD COLUMN IF NOT EXISTS heartbeat_seconds INTEGER;

-- Seed Chainlink feed configs (from Chainlink documentation)
-- USDC/USD Ethereum: 0.25% deviation, 86400s (24h) heartbeat
UPDATE oracle_registry SET deviation_threshold_pct = 0.25, heartbeat_seconds = 86400
WHERE oracle_address = '0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6' AND chain = 'ethereum';

-- USDT/USD Ethereum: 0.25% deviation, 86400s heartbeat
UPDATE oracle_registry SET deviation_threshold_pct = 0.25, heartbeat_seconds = 86400
WHERE oracle_address = '0x3E7d1eAB13ad0104d2750B8863b489D65364e32D' AND chain = 'ethereum';

-- DAI/USD Ethereum: 0.25% deviation, 3600s heartbeat
UPDATE oracle_registry SET deviation_threshold_pct = 0.25, heartbeat_seconds = 3600
WHERE oracle_address = '0xAed0c38402a5d19df6E4c03F4E2DceD6e29c1ee9' AND chain = 'ethereum';

-- ETH/USD Ethereum: 0.5% deviation, 3600s heartbeat
UPDATE oracle_registry SET deviation_threshold_pct = 0.5, heartbeat_seconds = 3600
WHERE oracle_address = '0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419' AND chain = 'ethereum';

-- stETH/ETH Ethereum: 0.5% deviation, 86400s heartbeat
UPDATE oracle_registry SET deviation_threshold_pct = 0.5, heartbeat_seconds = 86400
WHERE oracle_address = '0x86392dC19c0b719886221c78AB11eb8Cf5c52812' AND chain = 'ethereum';

-- ETH/USD Base: 0.15% deviation, 1200s heartbeat (L2 feeds update faster)
UPDATE oracle_registry SET deviation_threshold_pct = 0.15, heartbeat_seconds = 1200
WHERE oracle_address = '0x71041dddad3595F9CEd3DcCFBe3D1F4b0a16Bb70' AND chain = 'base';

-- Pyth USDC/USD: 0.1% deviation, 60s heartbeat (Pyth updates much faster)
UPDATE oracle_registry SET deviation_threshold_pct = 0.1, heartbeat_seconds = 60
WHERE oracle_address = '0xff1a0f4744e8582DF1aE09D5611b887B6a12925C' AND chain = 'ethereum';

INSERT INTO migrations (name) VALUES ('081_oracle_registry_feed_config') ON CONFLICT DO NOTHING;
