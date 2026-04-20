# Session Summary: Track Record Ledger (2026-04-19)

## What was built

Internal-only track record ledger that auto-logs risk calls from existing signal pipelines, supports manual "featured call" curation, and runs automated 30/60/90-day outcome checks.

### Components

1. **Migrations 079-080**: `track_record_entries` + `track_record_followups` tables
2. **app/track_record.py**: Auto-entry writer with 3 active trigger rules
3. **app/track_record_followups.py**: Follow-up evaluator with conservative outcome classification
4. **API routes**: 6 endpoints under `/api/ops/track-record/*`
5. **Ops dashboard**: TrackRecordPanel (11th section)

### Auto-trigger rules

| Rule | Trigger | Source Table | Status |
|------|---------|-------------|--------|
| A | Material score change (>=10pts/7d) | score_history, psi_scores | Active |
| B | Divergence signal (critical/alert) | divergence_signals | Active |
| C | Coherence drop (issues > 0) | coherence_reports | Active |
| D | Oracle stress event | oracle_stress_events | Deferred (table missing) |
| E | Governance proposal edit | governance_proposal_snapshots | Deferred (table missing) |
| F | Contract upgrade | contract_upgrade_history | Deferred (table missing) |

### Outcome classification

- **validated**: signal direction confirmed by score movement (>5pt in flagged direction)
- **mixed**: partial confirmation, ambiguous, or minimal movement (<=5pt)
- **not_borne_out**: opposite of flagged direction (>5pt opposite)
- **insufficient_data**: entity dropped from coverage, data gaps

### Backfill decision

**Skipped.** Cannot cleanly determine state_root values from 30 days ago — the state_root is a composite from daily_pulses.summary which may not have been consistently populated. Accepting an empty dashboard for the first week. First auto entries will appear on the next slow cycle that detects a qualifying signal (score change >=10pts, divergence alert, or coherence issue).

### What's deferred to the public /track-record session

- Public route (`/track-record`) with SSR rendering
- robots.txt / sitemap entry
- JSON-LD structured data for featured entries
- Social share assets (OG image generation per featured call)
- Rules D, E, F (pending source table creation)

### Calibration baseline

Empty. First 30-day followup window opens ~May 19, 2026. Until then, calibration section shows no data.

### API endpoints

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | /api/ops/track-record/entries | admin | List with filters |
| GET | /api/ops/track-record/entries/{id} | admin | Full detail + followups |
| POST | /api/ops/track-record/entries/{id}/feature | admin | Promote to featured |
| POST | /api/ops/track-record/entries | admin | Create manual entry |
| POST | /api/ops/track-record/followups/{id}/narrative | admin | Add narrative |
| GET | /api/ops/track-record/summary | admin | Dashboard section data |

### Link to ops dashboard section

`/ops/state-growth` → scroll to "TRACK RECORD" section (11th panel)
