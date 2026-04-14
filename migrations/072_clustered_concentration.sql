-- Migration 072: Graph-Clustered Holder Concentration (Pipeline 14)
-- Computes true holder concentration by merging wallets that share graph
-- counterparties into single economic entities.

CREATE TABLE IF NOT EXISTS holder_clusters (
    id SERIAL PRIMARY KEY,
    stablecoin_symbol VARCHAR(20) NOT NULL,
    stablecoin_id INTEGER,
    snapshot_date DATE NOT NULL,
    cluster_id INTEGER NOT NULL,
    seed_wallet VARCHAR(42),
    member_wallets JSONB,
    member_count INTEGER,
    total_balance_usd DECIMAL(20,2),
    pct_of_supply DECIMAL(8,4),
    cluster_type VARCHAR(50),
    actor_labels JSONB,
    UNIQUE (stablecoin_symbol, snapshot_date, cluster_id)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clusters_symbol_date
    ON holder_clusters (stablecoin_symbol, snapshot_date DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_clusters_date
    ON holder_clusters (snapshot_date DESC);


CREATE TABLE IF NOT EXISTS concentration_snapshots (
    id SERIAL PRIMARY KEY,
    stablecoin_symbol VARCHAR(20) NOT NULL,
    stablecoin_id INTEGER,
    snapshot_date DATE NOT NULL,
    nominal_gini DECIMAL(8,6),
    clustered_gini DECIMAL(8,6),
    nominal_top10_pct DECIMAL(8,4),
    clustered_top10_pct DECIMAL(8,4),
    nominal_top50_pct DECIMAL(8,4),
    clustered_top50_pct DECIMAL(8,4),
    cluster_count INTEGER,
    singleton_count INTEGER,
    exchange_cluster_pct DECIMAL(8,4),
    whale_cluster_pct DECIMAL(8,4),
    divergence_score DECIMAL(8,4),
    total_wallets_analyzed INTEGER,
    total_supply_tracked_usd DECIMAL(20,2),
    content_hash VARCHAR(66),
    attested_at TIMESTAMPTZ,
    UNIQUE (stablecoin_symbol, snapshot_date)
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_concentration_date
    ON concentration_snapshots (snapshot_date DESC);


INSERT INTO migrations (name) VALUES ('072_clustered_concentration') ON CONFLICT DO NOTHING;
