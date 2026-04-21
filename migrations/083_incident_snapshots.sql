-- Migration 083 — incident_snapshots + incident_subscribers
--
-- incident_snapshots stores pinned, frozen component readings for public
-- incident evidence pages (e.g. /incident/rseth-2026-04-18). Values are
-- written once at snapshot time and are NOT re-queried at render time,
-- so the page shows the same data to everyone from the moment it is pinned.
--
-- incident_subscribers is a first-class, incident-specific email-capture
-- table. No existing newsletter table existed to extend; playground_submissions
-- is playground-specific. See audits/lsti_rseth_audit_2026-04-20.md.

CREATE TABLE IF NOT EXISTS incident_snapshots (
    slug              TEXT PRIMARY KEY,
    event_date        DATE NOT NULL,
    title             TEXT NOT NULL,
    summary           TEXT NOT NULL,
    captured_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    components_json   JSONB NOT NULL,
    metadata_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_incident_snapshots_event_date
    ON incident_snapshots (event_date DESC);

CREATE TABLE IF NOT EXISTS incident_subscribers (
    id            BIGSERIAL PRIMARY KEY,
    email         TEXT NOT NULL,
    source        TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (email, source)
);

CREATE INDEX IF NOT EXISTS idx_incident_subscribers_source
    ON incident_subscribers (source);
