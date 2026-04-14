-- Migration 069: Provenance source registry + health alerts
--
-- Enables hub-managed provenance source registry that the external prover
-- reads from instead of a local YAML file. Supports self-expanding provenance
-- (new collectors auto-register) and self-healing (health state persists).

-- Source registry: one row per data source the prover should notarize
CREATE TABLE IF NOT EXISTS provenance_sources (
    id TEXT PRIMARY KEY,                          -- e.g. "coingecko_price"
    entity TEXT NOT NULL,                         -- e.g. "coingecko", "defillama", "etherscan"
    component TEXT NOT NULL,                      -- e.g. "price", "tvl", "holders"
    source_type TEXT NOT NULL,                    -- "live_api", "static_github", "static_docs", "etherscan_api", "html_docs", "protocol_api"
    url TEXT NOT NULL,
    schedule TEXT NOT NULL DEFAULT 'hourly',       -- "hourly" or "weekly"
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    -- health fields (written by the prover, read by the hub for monitoring)
    consecutive_failures INT DEFAULT 0,
    last_success TIMESTAMPTZ,
    last_failure TIMESTAMPTZ,
    last_error TEXT,
    disabled_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_provenance_sources_enabled ON provenance_sources (enabled);
CREATE INDEX IF NOT EXISTS idx_provenance_sources_entity ON provenance_sources (entity);

-- Health alerts: written by the prover when sources are disabled/healed
CREATE TABLE IF NOT EXISTS provenance_health_alerts (
    id SERIAL PRIMARY KEY,
    source_id TEXT NOT NULL,
    event TEXT NOT NULL,                          -- "domain_change_detected", "dns_failure", "auto_healed", "re_enabled"
    old_url TEXT,
    redirect_url TEXT,
    details JSONB,
    reviewed BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_provenance_health_alerts_source ON provenance_health_alerts (source_id);
CREATE INDEX IF NOT EXISTS idx_provenance_health_alerts_created ON provenance_health_alerts (created_at DESC);

-- Seed provenance_sources with the 14 known sources from provenance_scaling.py
-- Uses ON CONFLICT DO NOTHING so re-running is safe
INSERT INTO provenance_sources (id, entity, component, source_type, url, schedule)
VALUES
    ('coingecko_price',        'coingecko',  'price',          'live_api',      'https://pro-api.coingecko.com/api/v3/coins/usd-coin',                                    'hourly'),
    ('defillama_tvl',          'defillama',  'tvl',            'live_api',      'https://api.llama.fi/tvl/aave',                                                           'hourly'),
    ('etherscan_holders',      'etherscan',  'holders',        'etherscan_api', 'https://api.etherscan.io/api?module=token&action=tokenholdercount',                        'hourly'),
    ('snapshot_governance',    'snapshot',   'governance',     'protocol_api',  'https://hub.snapshot.org/graphql',                                                         'hourly'),
    ('geckoterminal_dex',      'coingecko',  'dex_pools',      'live_api',      'https://pro-api.coingecko.com/api/v3/onchain/networks/eth/tokens/multi/pools',              'hourly'),
    ('coingecko_tickers',      'coingecko',  'tickers',        'live_api',      'https://pro-api.coingecko.com/api/v3/coins/usd-coin/tickers',                              'hourly'),
    ('defillama_yields',       'defillama',  'yields',         'live_api',      'https://yields.llama.fi/pools',                                                            'hourly'),
    ('defillama_bridges',      'defillama',  'bridges',        'live_api',      'https://bridges.llama.fi/bridges',                                                         'weekly'),
    ('coingecko_exchanges',    'coingecko',  'exchanges',      'live_api',      'https://pro-api.coingecko.com/api/v3/exchanges/binance',                                   'hourly'),
    ('coingecko_market_chart', 'coingecko',  'market_chart',   'live_api',      'https://pro-api.coingecko.com/api/v3/coins/usd-coin/market_chart?vs_currency=usd&days=1',  'hourly'),
    ('etherscan_tokentx',      'etherscan',  'token_transfers','etherscan_api', 'https://api.etherscan.io/api?module=account&action=tokentx',                               'hourly'),
    ('etherscan_sourcecode',   'etherscan',  'source_code',    'etherscan_api', 'https://api.etherscan.io/api?module=contract&action=getsourcecode',                        'weekly'),
    ('blockscout_balances',    'blockscout', 'balances',       'protocol_api',  'https://eth.blockscout.com/api/v2/addresses/{address}/token-balances',                     'hourly'),
    ('tally_governance',       'tally',      'governance',     'protocol_api',  'https://api.tally.xyz/query',                                                              'hourly'),
    ('cda_issuer_pdf',         'issuer',     'cda_pdf',        'static_docs',   'dynamic:cda_source_urls',                                                                  'weekly')
ON CONFLICT (id) DO NOTHING;

INSERT INTO migrations (name) VALUES ('069_provenance_sources') ON CONFLICT DO NOTHING;
