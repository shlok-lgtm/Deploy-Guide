-- Migration 067: Parent Company Financials (Pipeline 21)
-- SEC EDGAR XBRL parent company health data for stablecoin issuer parents.

CREATE TABLE IF NOT EXISTS parent_company_registry (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(20),
    entity_id INTEGER,
    entity_symbol VARCHAR(20),
    company_name VARCHAR(200),
    sec_cik VARCHAR(20),
    relationship_type VARCHAR(50),
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS parent_company_financials (
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

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_parent_financials_cik_date
    ON parent_company_financials (cik, period_end_date DESC);


-- Seed known parent company mappings
INSERT INTO parent_company_registry
    (entity_type, entity_symbol, company_name, sec_cik, relationship_type)
VALUES
    ('stablecoin_issuer', 'pyusd', 'PayPal Holdings', '0001633917', 'parent'),
    ('custodian', 'usdc', 'Bank of New York Mellon', '0001390777', 'custodian'),
    ('custodian', 'usdc', 'BlackRock', '0001364742', 'banking_partner')
ON CONFLICT DO NOTHING;


INSERT INTO migrations (name) VALUES ('067_parent_company_financials') ON CONFLICT DO NOTHING;
