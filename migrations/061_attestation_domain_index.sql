-- Covering index for coherence sweep queries that look up latest
-- attestation timestamps and record counts per domain.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_state_attestations_domain_ts
    ON state_attestations (domain, cycle_timestamp DESC);
