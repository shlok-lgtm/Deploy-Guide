-- Migration 018: API Budget Allocator
-- Tracks daily API call budgets across SII, PSI, and wallet indexer processes.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE ops.api_budget (
    id SERIAL PRIMARY KEY,
    budget_date DATE NOT NULL DEFAULT CURRENT_DATE,
    provider VARCHAR(50) NOT NULL DEFAULT 'etherscan',
    daily_limit INTEGER NOT NULL DEFAULT 100000,

    -- Running totals, updated atomically by each process
    sii_calls_used INTEGER NOT NULL DEFAULT 0,
    psi_calls_used INTEGER NOT NULL DEFAULT 0,
    wallet_refresh_calls_used INTEGER NOT NULL DEFAULT 0,
    wallet_expansion_calls_used INTEGER NOT NULL DEFAULT 0,

    -- Timestamps for ordering and debugging
    sii_started_at TIMESTAMPTZ,
    sii_completed_at TIMESTAMPTZ,
    psi_started_at TIMESTAMPTZ,
    psi_completed_at TIMESTAMPTZ,
    wallet_refresh_started_at TIMESTAMPTZ,
    wallet_refresh_completed_at TIMESTAMPTZ,
    wallet_expansion_started_at TIMESTAMPTZ,
    wallet_expansion_completed_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE(budget_date, provider)
);

CREATE INDEX idx_api_budget_date ON ops.api_budget(budget_date);
