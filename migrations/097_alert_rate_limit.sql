-- Migration 097: Alert Rate Limit (daily cap tracking)

CREATE TABLE IF NOT EXISTS alert_rate_limit (
    day DATE PRIMARY KEY,
    count INTEGER NOT NULL DEFAULT 0,
    last_sent_at TIMESTAMPTZ
);

INSERT INTO migrations (name) VALUES ('097_alert_rate_limit') ON CONFLICT DO NOTHING;
