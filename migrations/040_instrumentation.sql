-- Migration 040: Instrumentation tables for seed metrics tracking
-- Tracks API consumption, MCP tool calls, oracle reads, and keeper publishes.

-- Daily metrics rollup (one row per day)
CREATE TABLE IF NOT EXISTS metrics_daily_rollup (
    id SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    total_api_requests INT DEFAULT 0,
    external_api_requests INT DEFAULT 0,
    internal_api_requests INT DEFAULT 0,
    unique_ips INT DEFAULT 0,
    unique_external_ips INT DEFAULT 0,
    unique_api_keys INT DEFAULT 0,
    mcp_requests INT DEFAULT 0,
    mcp_tool_calls INT DEFAULT 0,
    avg_response_time_ms FLOAT DEFAULT 0,
    error_count INT DEFAULT 0,           -- status_code >= 400
    top_endpoints JSONB DEFAULT '[]',    -- [{endpoint, count}]
    top_user_agents JSONB DEFAULT '[]',  -- [{user_agent, count}]
    jsonld_requests INT DEFAULT 0,       -- Accept: application/ld+json
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- MCP tool-level call log
CREATE TABLE IF NOT EXISTS mcp_tool_calls (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    tool_name VARCHAR(100) NOT NULL,
    args_summary JSONB,                  -- first 500 chars of args, for debugging
    response_time_ms INT,
    success BOOLEAN DEFAULT TRUE,
    caller_ip VARCHAR(45),
    caller_user_agent VARCHAR(500)
);
CREATE INDEX IF NOT EXISTS idx_mcp_tool_calls_ts ON mcp_tool_calls (timestamp);
CREATE INDEX IF NOT EXISTS idx_mcp_tool_calls_tool ON mcp_tool_calls (tool_name, timestamp);

-- Oracle read events (polled from on-chain)
CREATE TABLE IF NOT EXISTS oracle_reads_log (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    chain VARCHAR(20) NOT NULL,          -- 'base' or 'arbitrum'
    contract_address VARCHAR(42) NOT NULL,
    reader_address VARCHAR(42),          -- the address that called getScore
    token_address VARCHAR(42),
    function_name VARCHAR(50),           -- 'getScore', 'getAllScores', etc.
    tx_hash VARCHAR(66),
    block_number BIGINT
);
CREATE INDEX IF NOT EXISTS idx_oracle_reads_chain ON oracle_reads_log (chain, timestamp);

-- Keeper publish log
CREATE TABLE IF NOT EXISTS keeper_publish_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    chain VARCHAR(20) NOT NULL,
    scores_published INT DEFAULT 0,
    gas_used BIGINT,
    tx_hash VARCHAR(66),
    success BOOLEAN DEFAULT TRUE,
    error_message TEXT
);

-- Add columns to existing api_request_log
ALTER TABLE api_request_log ADD COLUMN IF NOT EXISTS accept_header VARCHAR(255);
ALTER TABLE api_request_log ADD COLUMN IF NOT EXISTS referer VARCHAR(500);
ALTER TABLE api_request_log ADD COLUMN IF NOT EXISTS is_internal BOOLEAN DEFAULT FALSE;
ALTER TABLE api_request_log ADD COLUMN IF NOT EXISTS entity_type VARCHAR(20);  -- 'stablecoin', 'wallet', 'protocol', 'mcp'
ALTER TABLE api_request_log ADD COLUMN IF NOT EXISTS entity_id VARCHAR(100);   -- the coin symbol, wallet address, protocol slug

INSERT INTO migrations (name) VALUES ('040_instrumentation') ON CONFLICT DO NOTHING;
