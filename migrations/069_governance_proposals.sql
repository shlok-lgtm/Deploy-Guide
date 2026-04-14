-- Migration 069: Governance Proposal Corpus (Pipeline 8)
-- Captures every governance proposal at publication time as permanent attested state.
-- Proposals get deleted, edited, and migrated — text must be captured at discovery.

CREATE TABLE IF NOT EXISTS governance_proposals (
    id SERIAL PRIMARY KEY,
    protocol_slug VARCHAR(100) NOT NULL,
    protocol_id INTEGER,
    proposal_id VARCHAR(200) NOT NULL,
    proposal_source VARCHAR(50) NOT NULL,
    title TEXT,
    body TEXT,
    body_hash VARCHAR(66),
    author_address VARCHAR(42),
    author_ens VARCHAR(200),
    state VARCHAR(50),
    vote_start TIMESTAMPTZ,
    vote_end TIMESTAMPTZ,
    scores_total DECIMAL(30,8),
    scores_for DECIMAL(30,8),
    scores_against DECIMAL(30,8),
    scores_abstain DECIMAL(30,8),
    quorum DECIMAL(30,8),
    choices JSONB,
    votes JSONB,
    ipfs_hash VARCHAR(100),
    discussion_url TEXT,
    captured_at TIMESTAMPTZ DEFAULT NOW(),
    body_changed BOOLEAN DEFAULT FALSE,
    first_capture_body_hash VARCHAR(66),
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ,
    UNIQUE (proposal_source, proposal_id)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gov_proposals_protocol_captured
    ON governance_proposals (protocol_slug, captured_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gov_proposals_vote_end
    ON governance_proposals (vote_end DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gov_proposals_state
    ON governance_proposals (state);


CREATE TABLE IF NOT EXISTS governance_proposal_snapshots (
    id SERIAL PRIMARY KEY,
    proposal_db_id INTEGER REFERENCES governance_proposals(id),
    body_hash VARCHAR(66) NOT NULL,
    state VARCHAR(50),
    scores_total DECIMAL(30,8),
    checked_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gov_snapshots_proposal_checked
    ON governance_proposal_snapshots (proposal_db_id, checked_at DESC);


INSERT INTO migrations (name) VALUES ('069_governance_proposals') ON CONFLICT DO NOTHING;
