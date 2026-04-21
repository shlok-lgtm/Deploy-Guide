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

CREATE INDEX IF NOT EXISTS idx_ae_wallet ON assessment_events(wallet_address, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ae_severity ON assessment_events(severity, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ae_broadcast ON assessment_events(broadcast, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ae_created ON assessment_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ae_trigger ON assessment_events(trigger_type);

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
