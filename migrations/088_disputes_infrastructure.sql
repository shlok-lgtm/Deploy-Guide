-- Migration 088: Disputes infrastructure
-- Score dispute lifecycle: submission, counter-evidence, resolution
-- On-chain anchoring columns for dispute transitions

CREATE TABLE IF NOT EXISTS disputes (
    dispute_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_slug TEXT NOT NULL,
    disputed_score_content_hash TEXT,
    submitter_identifier TEXT NOT NULL,
    submitter_type TEXT NOT NULL CHECK (submitter_type IN ('issuer', 'third_party', 'regulator', 'anonymous')),
    submission_text TEXT NOT NULL,
    submission_evidence_url TEXT,
    status TEXT NOT NULL DEFAULT 'submitted' CHECK (status IN ('submitted', 'under_review', 'counter_evidence_issued', 'resolved', 'withdrawn')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    resolution_category TEXT CHECK (resolution_category IN ('accepted', 'partially_accepted', 'rejected')),
    resolution_narrative TEXT
);

CREATE TABLE IF NOT EXISTS dispute_transitions (
    transition_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dispute_id UUID NOT NULL REFERENCES disputes(dispute_id) ON DELETE CASCADE,
    transition_index INTEGER NOT NULL,
    transition_kind TEXT NOT NULL CHECK (transition_kind IN ('submission', 'counter_evidence', 'resolution')),
    transition_payload JSONB NOT NULL,
    content_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    committed_on_chain_base BOOLEAN DEFAULT FALSE,
    committed_on_chain_arbitrum BOOLEAN DEFAULT FALSE,
    on_chain_tx_hash_base TEXT,
    on_chain_tx_hash_arbitrum TEXT,
    committed_at TIMESTAMPTZ,
    UNIQUE(dispute_id, transition_index)
);

CREATE INDEX IF NOT EXISTS idx_disputes_entity_created
    ON disputes(entity_slug, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_disputes_status_open
    ON disputes(status)
    WHERE status != 'resolved';

CREATE INDEX IF NOT EXISTS idx_dispute_transitions_dispute_idx
    ON dispute_transitions(dispute_id, transition_index);

CREATE INDEX IF NOT EXISTS idx_dispute_transitions_pending_base
    ON dispute_transitions(committed_on_chain_base)
    WHERE committed_on_chain_base = FALSE;

CREATE INDEX IF NOT EXISTS idx_dispute_transitions_pending_arb
    ON dispute_transitions(committed_on_chain_arbitrum)
    WHERE committed_on_chain_arbitrum = FALSE;

INSERT INTO migrations (name) VALUES ('088_disputes_infrastructure') ON CONFLICT DO NOTHING;
