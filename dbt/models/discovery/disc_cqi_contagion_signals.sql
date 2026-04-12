-- CQI contagion concentration signals
-- Fires when a protocol's pool wallets have dense graph connectivity
-- to low-risk-score wallets (potential contagion vectors).
WITH pool_edge_stats AS (
    SELECT
        ppw.protocol_slug,
        ppw.stablecoin_symbol,
        COUNT(DISTINCT ppw.wallet_address) AS pool_wallet_count,
        COUNT(DISTINCT we.to_address) FILTER (
            WHERE we.to_address != ppw.wallet_address
        ) + COUNT(DISTINCT we.from_address) FILTER (
            WHERE we.from_address != ppw.wallet_address
        ) AS connected_wallets,
        AVG(we.weight) AS avg_edge_weight,
        SUM(we.total_value_usd) AS total_edge_value
    FROM {{ source('basis', 'protocol_pool_wallets') }} ppw
    LEFT JOIN {{ source('wallet_graph', 'wallet_edges') }} we
        ON (we.from_address = ppw.wallet_address OR we.to_address = ppw.wallet_address)
        AND we.weight > 0.05
    GROUP BY ppw.protocol_slug, ppw.stablecoin_symbol
),
pool_risk_stats AS (
    SELECT
        ppw.protocol_slug,
        ppw.stablecoin_symbol,
        AVG(wrs.risk_score) AS avg_pool_risk,
        MIN(wrs.risk_score) AS min_pool_risk,
        COUNT(*) FILTER (WHERE wrs.risk_grade IN ('D+', 'D', 'D-', 'F')) AS low_grade_wallets,
        COUNT(*) FILTER (WHERE wrs.risk_score IS NOT NULL) AS scored_wallets
    FROM {{ source('basis', 'protocol_pool_wallets') }} ppw
    LEFT JOIN LATERAL (
        SELECT risk_score, risk_grade
        FROM {{ source('wallet_graph', 'wallet_risk_scores') }}
        WHERE wallet_address = ppw.wallet_address
        ORDER BY computed_at DESC LIMIT 1
    ) wrs ON true
    GROUP BY ppw.protocol_slug, ppw.stablecoin_symbol
),
concentration_signals AS (
    SELECT
        'cqi_contagion_concentration' as signal_type,
        'cqi' as domain,
        pes.protocol_slug || '/' || pes.stablecoin_symbol || ': ' || pes.connected_wallets || ' connected wallets from ' || pes.pool_wallet_count || ' pool holders' as title,
        'Pool wallets in ' || pes.protocol_slug || ' ' || pes.stablecoin_symbol || ' pool have ' || pes.connected_wallets || ' graph connections. Avg pool risk: ' || COALESCE(ROUND(prs.avg_pool_risk::numeric, 1)::text, 'N/A') || '. Low-grade wallets: ' || prs.low_grade_wallets as description,
        jsonb_build_array(pes.protocol_slug, pes.stablecoin_symbol) as entities,
        CASE
            WHEN prs.low_grade_wallets > 0 AND prs.scored_wallets > 0
            THEN LEAST((prs.low_grade_wallets::float / prs.scored_wallets) * 10, 5.0)
            WHEN pes.connected_wallets > 100
            THEN 2.0
            ELSE 0.5
        END as novelty_score,
        CASE
            WHEN prs.low_grade_wallets > 0 THEN 'risk'
            WHEN pes.connected_wallets > 50 THEN 'shift'
            ELSE 'neutral'
        END as direction,
        COALESCE(pes.connected_wallets, 0)::float as magnitude,
        0 as baseline,
        jsonb_build_object(
            'pool_wallet_count', pes.pool_wallet_count,
            'connected_wallets', pes.connected_wallets,
            'avg_edge_weight', pes.avg_edge_weight,
            'total_edge_value', pes.total_edge_value,
            'avg_pool_risk', prs.avg_pool_risk,
            'min_pool_risk', prs.min_pool_risk,
            'low_grade_wallets', prs.low_grade_wallets,
            'scored_wallets', prs.scored_wallets
        ) as detail
    FROM pool_edge_stats pes
    JOIN pool_risk_stats prs USING (protocol_slug, stablecoin_symbol)
    WHERE pes.pool_wallet_count > 0
)

SELECT * FROM concentration_signals
WHERE novelty_score > 0.5
