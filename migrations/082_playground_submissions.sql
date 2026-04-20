-- Migration 082: Composition playground submissions
--
-- Retention policy: Submissions are retained indefinitely for product analytics.
-- Emails are retained until the user requests deletion via shlok@basisprotocol.xyz.
-- Portfolios are never shared externally.

CREATE TABLE IF NOT EXISTS playground_submissions (
    submission_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    submitted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    submitter_ip_hash TEXT NOT NULL,
    submitter_email TEXT,
    portfolio JSONB NOT NULL,
    computed_cqi JSONB,
    computed_stress JSONB,
    report_requested BOOLEAN DEFAULT FALSE,
    report_request_at TIMESTAMPTZ,
    report_link_token TEXT,
    report_link_expires TIMESTAMPTZ,
    email_sent_at TIMESTAMPTZ,
    email_verified_at TIMESTAMPTZ,
    report_accessed_at TIMESTAMPTZ,
    report_access_count INT DEFAULT 0,
    content_hash TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pg_sub_ip_time
    ON playground_submissions (submitter_ip_hash, submitted_at DESC);
CREATE INDEX IF NOT EXISTS idx_pg_sub_email
    ON playground_submissions (submitter_email) WHERE submitter_email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pg_sub_expiry
    ON playground_submissions (report_link_expires) WHERE report_requested = TRUE;
CREATE INDEX IF NOT EXISTS idx_pg_sub_time
    ON playground_submissions (submitted_at DESC);

INSERT INTO migrations (name) VALUES ('082_playground_submissions') ON CONFLICT DO NOTHING;
