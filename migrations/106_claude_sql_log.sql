-- Migration 106: Claude SQL log table
-- Tracks every query executed via /api/admin/sql for audit trail.

CREATE TABLE IF NOT EXISTS claude_sql_log (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    sql_text TEXT NOT NULL,
    row_count INTEGER DEFAULT 0,
    elapsed_ms INTEGER,
    success BOOLEAN NOT NULL DEFAULT TRUE,
    error_msg TEXT
);

CREATE INDEX IF NOT EXISTS idx_claude_sql_log_ts ON claude_sql_log (ts DESC);

INSERT INTO migrations (name) VALUES ('106_claude_sql_log') ON CONFLICT DO NOTHING;
