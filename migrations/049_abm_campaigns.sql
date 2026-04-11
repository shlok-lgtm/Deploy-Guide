-- Migration 049: ABM Campaign Engine
-- Personalized Comply experiences with drip sequences and state tracking

CREATE TABLE IF NOT EXISTS abm_campaigns (
    id SERIAL PRIMARY KEY,
    mode TEXT NOT NULL DEFAULT 'icp',          -- 'named' or 'icp'
    icp_type TEXT NOT NULL,                     -- exchange_eu, exchange_us, lending_protocol, dao_treasury, etc.
    org TEXT NOT NULL,
    person TEXT,
    title TEXT,
    stablecoins JSONB NOT NULL DEFAULT '[]',
    lenses JSONB NOT NULL DEFAULT '[]',
    pain_points JSONB NOT NULL DEFAULT '[]',
    entry_piece TEXT,
    state INTEGER NOT NULL DEFAULT 0,           -- 0=Unaware through 8=Committed
    named_target_id INTEGER REFERENCES ops_targets(id),  -- link to existing ops target if applicable
    report_hash TEXT,                           -- hash of the pre-generated Comply report
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS abm_drip_touches (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES abm_campaigns(id) ON DELETE CASCADE,
    day INTEGER NOT NULL,
    channel TEXT NOT NULL,                      -- email, linkedin, twitter, forum
    subject TEXT NOT NULL,
    description TEXT,
    is_gate BOOLEAN DEFAULT FALSE,
    status TEXT DEFAULT 'pending',              -- pending, sent, skipped
    sent_at TIMESTAMP,
    response TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS abm_touch_log (
    id SERIAL PRIMARY KEY,
    campaign_id INTEGER NOT NULL REFERENCES abm_campaigns(id) ON DELETE CASCADE,
    note TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_abm_campaigns_state ON abm_campaigns(state);
CREATE INDEX IF NOT EXISTS idx_abm_drip_campaign ON abm_drip_touches(campaign_id);
CREATE INDEX IF NOT EXISTS idx_abm_log_campaign ON abm_touch_log(campaign_id);

INSERT INTO migrations (name, applied_at)
VALUES ('049_abm_campaigns', NOW())
ON CONFLICT DO NOTHING;
