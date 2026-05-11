-- Migration 108: Replay pg_dump drift for Pipeline 6 / 17 / 19 / 21 tables
--
-- 2026-05-11 substrate audit (Wave 9c):
--   The `migrations` table records 064/065/067/068/070 as applied, but
--   the corresponding tables do not exist on the owned-Neon substrate:
--     migration 064 → validator_performance_snapshots, validator_slashing_events
--     migration 065 → sanctions_screening_results, sanctions_screen_targets
--     migration 067 → parent_company_registry, parent_company_financials
--     migration 068 → (sanctions_screen_targets seeds — no DDL)
--     migration 070 → contract_dependencies, dependency_graph_snapshots
--
-- Same root cause as the 2026-05-10 schema_heal incident: the Replit→
-- owned-Neon pg_dump preserved the `migrations` tracking table but
-- silently skipped DDL for tables created outside Replit's UI. The
-- inline run_migrations() step in main.py reads `migrations` and
-- skips already-applied entries, so the gap never auto-heals.
--
-- This migration replays the canonical CREATE TABLE / INSERT
-- statements from the original migrations, gated by IF NOT EXISTS
-- and ON CONFLICT DO NOTHING. Safe to re-run; safe on prod where one
-- or more tables happen to already exist.
--
-- Verification queries surfaced 4 boot-time `gate CHECK FAILED` log
-- lines from app/enrichment_worker.py:_db_gate against:
--   - validator_performance_snapshots  (rated_validators collector)
--   - contract_dependency_graph        (NAME MISMATCH — see PR notes)
--   - parent_company_financials        (parent financials collector)
--   - sanctions_screening_results      (sanctions screening collector)
--
-- The `contract_dependency_graph` gate-check is fixed in this PR's
-- companion edit to enrichment_worker.py — the actual table name is
-- `contract_dependencies`.

DO $$
BEGIN
    -- ============================================================
    -- Migration 064: Validator Performance Snapshots
    -- ============================================================
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'validator_performance_snapshots'
    ) THEN
        CREATE TABLE validator_performance_snapshots (
            id SERIAL PRIMARY KEY,
            snapshot_date DATE NOT NULL,
            operator_name VARCHAR(200),
            operator_id VARCHAR(200),
            entity_type VARCHAR(50),
            validators_count INTEGER,
            effectiveness_score DECIMAL(6,4),
            attestation_effectiveness DECIMAL(6,4),
            proposal_luck DECIMAL(8,4),
            slashing_penalty_eth DECIMAL(20,8) DEFAULT 0,
            avg_validator_age_days INTEGER,
            network_penetration DECIMAL(8,6),
            lsti_entity_slug VARCHAR(100),
            content_hash VARCHAR(66),
            attested_at TIMESTAMPTZ,
            UNIQUE (snapshot_date, operator_id)
        );
        CREATE INDEX IF NOT EXISTS idx_validator_lsti_date
            ON validator_performance_snapshots (lsti_entity_slug, snapshot_date DESC);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'validator_slashing_events'
    ) THEN
        CREATE TABLE validator_slashing_events (
            id SERIAL PRIMARY KEY,
            detected_at TIMESTAMPTZ DEFAULT NOW(),
            operator_id VARCHAR(200),
            lsti_entity_slug VARCHAR(100),
            validators_slashed INTEGER,
            estimated_penalty_eth DECIMAL(20,8),
            epoch BIGINT,
            content_hash VARCHAR(66),
            attested_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_slashing_lsti
            ON validator_slashing_events (lsti_entity_slug, detected_at DESC);
    END IF;

    -- ============================================================
    -- Migration 065: Sanctions Screening
    -- ============================================================
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'sanctions_screening_results'
    ) THEN
        CREATE TABLE sanctions_screening_results (
            id SERIAL PRIMARY KEY,
            screened_at TIMESTAMPTZ DEFAULT NOW(),
            entity_type VARCHAR(20),
            entity_id INTEGER,
            entity_symbol VARCHAR(20),
            screen_target VARCHAR(200),
            screen_target_type VARCHAR(20),
            is_match BOOLEAN DEFAULT FALSE,
            match_score DECIMAL(5,4),
            match_dataset VARCHAR(100),
            match_entity_id VARCHAR(200),
            match_details JSONB,
            content_hash VARCHAR(66),
            attested_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_sanctions_entity
            ON sanctions_screening_results (entity_type, entity_id);
        CREATE INDEX IF NOT EXISTS idx_sanctions_is_match
            ON sanctions_screening_results (is_match) WHERE is_match = TRUE;
        CREATE INDEX IF NOT EXISTS idx_sanctions_screened_at
            ON sanctions_screening_results (screened_at DESC);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'sanctions_screen_targets'
    ) THEN
        CREATE TABLE sanctions_screen_targets (
            id SERIAL PRIMARY KEY,
            entity_type VARCHAR(20),
            entity_id INTEGER,
            entity_symbol VARCHAR(20),
            target_name VARCHAR(200),
            target_type VARCHAR(20),
            active BOOLEAN DEFAULT TRUE,
            added_at TIMESTAMPTZ DEFAULT NOW()
        );
    END IF;

    -- ============================================================
    -- Migration 067: Parent Company Financials
    -- ============================================================
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'parent_company_registry'
    ) THEN
        CREATE TABLE parent_company_registry (
            id SERIAL PRIMARY KEY,
            entity_type VARCHAR(20),
            entity_id INTEGER,
            entity_symbol VARCHAR(20),
            company_name VARCHAR(200),
            sec_cik VARCHAR(20),
            relationship_type VARCHAR(50),
            active BOOLEAN DEFAULT TRUE
        );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'parent_company_financials'
    ) THEN
        CREATE TABLE parent_company_financials (
            id SERIAL PRIMARY KEY,
            cik VARCHAR(20) NOT NULL,
            company_name VARCHAR(200),
            fiscal_period VARCHAR(10),
            fiscal_year INTEGER,
            period_end_date DATE,
            total_assets_usd DECIMAL(20,2),
            total_liabilities_usd DECIMAL(20,2),
            total_equity_usd DECIMAL(20,2),
            revenue_usd DECIMAL(20,2),
            net_income_usd DECIMAL(20,2),
            cash_and_equivalents_usd DECIMAL(20,2),
            debt_to_equity DECIMAL(10,4),
            current_ratio DECIMAL(10,4),
            captured_at TIMESTAMPTZ DEFAULT NOW(),
            content_hash VARCHAR(66),
            attested_at TIMESTAMPTZ,
            UNIQUE (cik, fiscal_year, fiscal_period)
        );
        CREATE INDEX IF NOT EXISTS idx_parent_financials_cik_date
            ON parent_company_financials (cik, period_end_date DESC);
    END IF;

    -- ============================================================
    -- Migration 070: Contract Dependency Graph
    -- ============================================================
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'contract_dependencies'
    ) THEN
        CREATE TABLE contract_dependencies (
            id SERIAL PRIMARY KEY,
            entity_type VARCHAR(20) NOT NULL,
            entity_id INTEGER NOT NULL,
            entity_slug VARCHAR(100),
            source_contract VARCHAR(42) NOT NULL,
            source_chain VARCHAR(20) NOT NULL,
            depends_on_address VARCHAR(42) NOT NULL,
            depends_on_chain VARCHAR(20) NOT NULL,
            depends_on_label VARCHAR(200),
            depends_on_type VARCHAR(50),
            call_type VARCHAR(50),
            detected_via VARCHAR(50),
            first_seen_at TIMESTAMPTZ DEFAULT NOW(),
            last_confirmed_at TIMESTAMPTZ DEFAULT NOW(),
            removed_at TIMESTAMPTZ,
            content_hash VARCHAR(66),
            attested_at TIMESTAMPTZ,
            UNIQUE (source_contract, source_chain, depends_on_address, depends_on_chain)
        );
        CREATE INDEX IF NOT EXISTS idx_contract_deps_entity
            ON contract_dependencies (entity_type, entity_id);
        CREATE INDEX IF NOT EXISTS idx_contract_deps_reverse
            ON contract_dependencies (depends_on_address);
        CREATE INDEX IF NOT EXISTS idx_contract_deps_first_seen
            ON contract_dependencies (first_seen_at DESC);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name = 'dependency_graph_snapshots'
    ) THEN
        CREATE TABLE dependency_graph_snapshots (
            id SERIAL PRIMARY KEY,
            entity_type VARCHAR(20),
            entity_id INTEGER,
            entity_slug VARCHAR(100),
            snapshot_date DATE NOT NULL,
            dependency_count INTEGER,
            dependency_addresses JSONB,
            dependency_hashes JSONB,
            content_hash VARCHAR(66),
            attested_at TIMESTAMPTZ,
            UNIQUE (entity_type, entity_id, snapshot_date)
        );
        CREATE INDEX IF NOT EXISTS idx_dep_snapshots_date
            ON dependency_graph_snapshots (snapshot_date DESC);
    END IF;
END $$;

-- ================================================================
-- Re-seed sanctions_screen_targets (migrations 065 + 068)
-- ================================================================
-- Issuer companies (065)
INSERT INTO sanctions_screen_targets (entity_type, entity_symbol, target_name, target_type) VALUES
    ('stablecoin_issuer', 'usdt', 'Tether Limited', 'company'),
    ('stablecoin_issuer', 'usdt', 'iFinex Inc', 'company'),
    ('stablecoin_issuer', 'usdc', 'Circle Internet Financial', 'company'),
    ('stablecoin_issuer', 'fdusd', 'First Digital Labs', 'company'),
    ('stablecoin_issuer', 'fdusd', 'First Digital Trust', 'company'),
    ('stablecoin_issuer', 'pyusd', 'Paxos Trust Company', 'company'),
    ('stablecoin_issuer', 'tusd', 'TrueUSD', 'company'),
    ('stablecoin_issuer', 'tusd', 'Archblock', 'company'),
    ('stablecoin_issuer', 'tusd', 'Techteryx', 'company'),
    ('stablecoin_issuer', 'usd1', 'World Liberty Financial', 'company'),
    ('stablecoin_issuer', 'dai', 'MakerDAO', 'company'),
    ('stablecoin_issuer', 'frax', 'Frax Finance', 'company'),
    ('stablecoin_issuer', 'usde', 'Ethena Labs', 'company'),
    ('stablecoin_issuer', 'usdd', 'TRON DAO Reserve', 'company')
ON CONFLICT DO NOTHING;

-- Tether/Bitfinex wallet addresses (068)
INSERT INTO sanctions_screen_targets (entity_type, entity_symbol, target_name, target_type) VALUES
    ('stablecoin_issuer', 'usdt', '0x5754284f345afc66a98fbb0a0afe71e0f007b949', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0xc6cde7c39eb2f0f0095f41570af89efc2c1ea828', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0x742d35cc6634c0532925a3b844bc454e4438f44e', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0xab7c74abc0c4d48d1bdad5dcb26153fc8780f83e', 'wallet_address'),
    ('stablecoin_issuer', 'usdt', '0xf4b51b14b9ee30dc37ec970b50a486f37686e2a8', 'wallet_address'),
    ('stablecoin_issuer', 'usd1', '0x5be9a4959308a0d0c7bc0870e319314d8d957dbb', 'wallet_address')
ON CONFLICT DO NOTHING;

-- Parent company registry seeds (067)
INSERT INTO parent_company_registry
    (entity_type, entity_symbol, company_name, sec_cik, relationship_type)
VALUES
    ('stablecoin_issuer', 'pyusd', 'PayPal Holdings', '0001633917', 'parent'),
    ('custodian', 'usdc', 'Bank of New York Mellon', '0001390777', 'custodian'),
    ('custodian', 'usdc', 'BlackRock', '0001364742', 'banking_partner')
ON CONFLICT DO NOTHING;

INSERT INTO migrations (name) VALUES ('108_replay_pg_dump_drift_pipeline_tables') ON CONFLICT DO NOTHING;
