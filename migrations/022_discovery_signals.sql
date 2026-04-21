BEGIN;

CREATE SCHEMA IF NOT EXISTS discovery;

CREATE TABLE IF NOT EXISTS discovery_signals (
    id BIGSERIAL PRIMARY KEY,

    -- What was found
    signal_type VARCHAR(50) NOT NULL,
    domain VARCHAR(30) NOT NULL,

    -- Signal details
    title VARCHAR(200) NOT NULL,
    description TEXT,
    entities JSONB,

    -- Quantification
    novelty_score DOUBLE PRECISION,
    direction VARCHAR(10),
    magnitude DOUBLE PRECISION,
    baseline DOUBLE PRECISION,

    -- Context
    detail JSONB,
    methodology_version VARCHAR(20) DEFAULT 'discovery-v0.1.0',

    -- Lifecycle
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    acknowledged BOOLEAN DEFAULT FALSE,
    published BOOLEAN DEFAULT FALSE,
    content_url VARCHAR(255)
);

CREATE INDEX IF NOT EXISTS idx_ds_type ON discovery_signals(signal_type);
CREATE INDEX IF NOT EXISTS idx_ds_domain ON discovery_signals(domain);
CREATE INDEX IF NOT EXISTS idx_ds_novelty ON discovery_signals(novelty_score DESC);
CREATE INDEX IF NOT EXISTS idx_ds_detected ON discovery_signals(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_ds_unacked ON discovery_signals(acknowledged, detected_at DESC);

INSERT INTO migrations (name) VALUES ('022_discovery_signals') ON CONFLICT DO NOTHING;

COMMIT;
