-- Migration 063: Contagion Event Archive (Pipeline 16)
-- Permanently archives every contagion propagation event with full graph state at detection time.

CREATE TABLE IF NOT EXISTS contagion_events (
    id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,
    source_entity_type VARCHAR(20) NOT NULL,
    source_entity_id INTEGER,
    source_entity_symbol VARCHAR(20),
    trigger_metric VARCHAR(50),
    trigger_value_before DECIMAL(10,4),
    trigger_value_after DECIMAL(10,4),
    severity VARCHAR(20),
    affected_entities JSONB,
    graph_state_snapshot JSONB,
    propagation_summary JSONB,
    detected_at TIMESTAMPTZ DEFAULT NOW(),
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contagion_source
    ON contagion_events (source_entity_type, source_entity_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contagion_detected_at
    ON contagion_events (detected_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contagion_event_type
    ON contagion_events (event_type);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contagion_severity
    ON contagion_events (severity);


INSERT INTO migrations (name) VALUES ('063_contagion_event_archive') ON CONFLICT DO NOTHING;
