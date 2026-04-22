-- Migration 095: Widen oracle_update_cadence columns for Chainlink uint80/uint256 range
-- BIGINT max is 9.2e18; Chainlink round_id is uint80 (max 1.2e24)

ALTER TABLE oracle_update_cadence ALTER COLUMN round_id TYPE NUMERIC(78,0);
ALTER TABLE oracle_update_cadence ALTER COLUMN updated_at_block TYPE NUMERIC(78,0);

INSERT INTO migrations (name) VALUES ('095_oracle_cadence_widen_columns') ON CONFLICT DO NOTHING;
