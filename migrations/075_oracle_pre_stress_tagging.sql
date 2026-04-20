-- Migration 075: Pre-stress reading tagging for Pipeline 10 (Oracle Behavioral Record)
--
-- Adds retroactive tagging of the 72 hours of oracle_price_readings that
-- preceded a stress event. Lets the triptych endpoint render
-- before / during / after context without scanning the whole readings table.
--
-- No backfill. Rows written before this migration stay untagged; only
-- stress events opened after deploy get pre-stress context.

ALTER TABLE oracle_price_readings
    ADD COLUMN IF NOT EXISTS pre_stress_event_id BIGINT NULL
        REFERENCES oracle_stress_events(id);

CREATE INDEX IF NOT EXISTS idx_oracle_readings_pre_stress
    ON oracle_price_readings(pre_stress_event_id)
    WHERE pre_stress_event_id IS NOT NULL;

ALTER TABLE oracle_stress_events
    ADD COLUMN IF NOT EXISTS pre_stress_window_hours INTEGER NULL DEFAULT 72,
    ADD COLUMN IF NOT EXISTS pre_stress_readings_tagged INTEGER NULL;
