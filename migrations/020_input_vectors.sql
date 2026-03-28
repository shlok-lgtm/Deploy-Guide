BEGIN;

CREATE TABLE IF NOT EXISTS assessment_input_vectors (
    id BIGSERIAL PRIMARY KEY,
    assessment_id UUID NOT NULL REFERENCES assessment_events(id),

    -- The full input vector
    wallet_address VARCHAR(42) NOT NULL,
    holdings JSONB NOT NULL,
    stablecoin_scores JSONB NOT NULL,
    formula_version VARCHAR(20) NOT NULL,
    computed_at TIMESTAMPTZ DEFAULT NOW(),

    -- Verification
    inputs_hash VARCHAR(66) NOT NULL,

    CONSTRAINT uq_input_vector_assessment UNIQUE (assessment_id)
);

CREATE INDEX IF NOT EXISTS idx_iv_wallet ON assessment_input_vectors(wallet_address);
CREATE INDEX IF NOT EXISTS idx_iv_hash ON assessment_input_vectors(inputs_hash);

INSERT INTO migrations (name) VALUES ('020_input_vectors') ON CONFLICT DO NOTHING;

COMMIT;
