# Table Inventory — Rules D, E, F Source Tables

## Rule D: Oracle Stress Events

- **Table**: `oracle_stress_events`
- **Migration**: `073_oracle_behavioral_record.sql`
- **Primary key**: `id` (SERIAL)
- **Timestamp**: `event_start` (TIMESTAMPTZ) — when the stress event began
- **End marker**: `event_end` (TIMESTAMPTZ, nullable) — NULL = still open
- **Entity linkage**: `asset_symbol` (VARCHAR(20)) → lowercase = SII entity_slug
- **Key columns for trigger_detail**:
  - `oracle_address`, `oracle_name`, `asset_symbol`, `chain`
  - `event_type`, `event_start`, `event_end`
  - `max_deviation_pct`, `max_latency_seconds`
  - `content_hash`

## Rule E: Governance Proposal Edits

- **Table**: `governance_proposals` (with `body_changed` column)
- **Snapshots table**: `governance_proposal_snapshots` (body_hash diffs)
- **Migration**: `069_governance_proposals.sql`
- **Primary key**: `id` (SERIAL)
- **Timestamp**: `captured_at` (TIMESTAMPTZ)
- **Edit flag**: `body_changed` (BOOLEAN, default FALSE)
- **Entity linkage**: `protocol_slug` (VARCHAR(100)) → direct entity_slug
- **Key columns for trigger_detail**:
  - `proposal_id`, `protocol_slug`, `proposal_source`
  - `body_hash`, `first_capture_body_hash`
  - `captured_at`, `title`
  - `content_hash`

## Rule F: Contract Upgrades

- **Table**: `contract_upgrade_history`
- **Migration**: `062_contract_upgrade_history.sql`
- **Primary key**: `id` (SERIAL)
- **Timestamp**: `upgrade_detected_at` (TIMESTAMPTZ)
- **Entity linkage**: `entity_symbol` (VARCHAR(20)) → lowercase = entity_slug; also `entity_type` + `entity_id` for DB joins
- **Key columns for trigger_detail**:
  - `contract_address`, `chain`
  - `previous_bytecode_hash`, `current_bytecode_hash`
  - `previous_implementation`, `current_implementation`
  - `block_number`, `upgrade_detected_at`
  - `slither_queued`, `content_hash`
