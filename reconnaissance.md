# Reconnaissance: Oracle Stress Detector False Positives

## Current Threshold Logic

File: `app/collectors/oracle_behavior.py`, lines 48-49 and 485-488.

```python
DEVIATION_STRESS_THRESHOLD = 0.5   # percent
LATENCY_STRESS_THRESHOLD = 3600    # seconds (1 hour)

is_stress = (
    abs(deviation_pct) > DEVIATION_STRESS_THRESHOLD    # 0.5%
    or latency > LATENCY_STRESS_THRESHOLD              # 1 hour
)
```

**The `or` is the bug.** Any feed with latency > 1 hour triggers stress, regardless of deviation. Chainlink stablecoin feeds have 24-hour heartbeats and only update on deviation > 0.25%. During calm markets, 10+ hours of latency with 0.01% deviation is healthy behavior.

## Oracle Registry Schema

Table: `oracle_registry` (migration 073)

| Column | Type | Has per-feed config? |
|--------|------|---------------------|
| oracle_address | VARCHAR(42) | — |
| oracle_name | VARCHAR(200) | — |
| oracle_provider | VARCHAR(50) | — |
| chain | VARCHAR(20) | — |
| asset_symbol | VARCHAR(20) | — |
| decimals | INTEGER | — |
| read_function | VARCHAR(100) | — |
| entity_slug | VARCHAR(100) | — |
| **deviation_threshold_pct** | — | **MISSING** |
| **heartbeat_seconds** | — | **MISSING** |

The registry has no per-feed deviation threshold or heartbeat. The detector applies one global threshold to all feeds.

## Seeded Oracles (7 feeds)

1. Chainlink ETH/USD (0x5f4e...) — entity_slug: NULL
2. Chainlink USDC/USD (0x8fFf...) — entity_slug: usdc
3. Chainlink USDT/USD (0x3E7d...) — entity_slug: usdt
4. Chainlink DAI/USD (0xAed0...) — entity_slug: dai
5. Chainlink stETH/ETH (0x8639...) — entity_slug: steth
6. Pyth USDC/USD (0xff1a...) — entity_slug: usdc
7. Chainlink ETH/USD Base (0x7100...) — entity_slug: NULL

## Why the Three Events False-Fired

- USDC/USD: Chainlink heartbeat is 86400s (24h). Deviation threshold is 0.25%. During calm markets, the feed doesn't update for 10-20h. The detector sees latency > 3600s and flags it as stress, even though deviation is 0.01%.
- USDT/USD: Same mechanism. 24h heartbeat, 0.25% deviation threshold.
- stETH/ETH: 24h heartbeat, 0.5% deviation threshold.

All three are working correctly. The detector's latency threshold is too aggressive for deviation-based feeds.

## Chosen Fix: Path B

Add `deviation_threshold_pct` and `heartbeat_seconds` columns to oracle_registry. Seed with real Chainlink values. Update detector to use per-feed config instead of global constants.

New stress logic:
```python
is_stress = (
    latency > (heartbeat_seconds * 1.1)      # past heartbeat + 10% buffer
    or abs(deviation_pct) > deviation_threshold_pct  # past feed's deviation trigger
)
```

This means: a feed is stressed if it missed its heartbeat OR its deviation exceeds its configured threshold. For Chainlink USDC/USD, this would be: latency > 95,040s (26.4h) OR deviation > 0.25%.
