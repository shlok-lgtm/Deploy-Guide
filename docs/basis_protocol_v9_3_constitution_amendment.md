# Constitution Amendment v9.3 — Bridge Flow Collector Deferral

**Date:** 2026-04-17
**Status:** Approved
**Supersedes:** V9.2 bridge_flows data layer specification

## Context

DeFiLlama moved all bridge-related API endpoints behind their Pro tier ($300/month, separate from the standard DeFiLlama Pro subscription) circa April 2026. All known free endpoints return 402 (Payment Required) or 404 (Not Found):

- `bridges.llama.fi/bridges` → 402
- `api.llama.fi/bridges` → 404
- `api.llama.fi/bridgedaystats` → 404
- `api.llama.fi/v1/bridges` → 404

## Alternatives Evaluated

| Option | Cost | Feasibility | Decision |
|--------|------|-------------|----------|
| DeFiLlama Pro API | $300/month | Immediate | Rejected — expensive for single data source |
| Dune Analytics | $349/month | Immediate | Rejected — same cost tier |
| Direct contract monitoring | $0 (existing Etherscan quota) | Phase 2 build | **Selected** |

## Decision

Defer the bridge_flows collector to Phase 2. The `bridge_flows` table schema is retained for future backfill. No bridge data will be collected until direct contract monitoring is implemented.

## Impact Assessment

- **BRI (Bridge Risk Index):** Currently scores 9 bridges using DeFiLlama TVL and protocol data, NOT bridge_flows. BRI is unaffected.
- **CXRI (CEX Risk Index):** Scores 8 exchanges. Bridge flows were planned as an enrichment signal, not a scoring dependency. CXRI is unaffected.
- **Divergence signals:** Bridge flow divergence detection is deferred. No current dependency.
- **Dashboard:** bridge_flows moved to "deferred" category. Removed from staleness checks.

## Kill Signal

Reactivate as priority if:
1. Any CXRI-scored exchange has >30% bridge-dependent risk exposure
2. BRI accuracy degrades without flow volume data
3. A free alternative data source becomes available

## Phase 2 Roadmap — Direct Contract Monitoring

Target bridges (by cumulative volume):
1. **Polygon PoS Bridge** — canonical bridge, highest volume
2. **Stargate (LayerZero)** — cross-chain messaging + bridge
3. **Across Protocol** — optimistic bridge
4. **Hop Protocol** — rollup bridge
5. **Wormhole** — generic message passing
6. **Celer cBridge** — liquidity network bridge

Implementation: monitor bridge contract events (deposits/withdrawals) on Ethereum mainnet using existing Etherscan V2 quota (~8K unused calls/day). Parse Transfer events to/from bridge contracts. Store in `bridge_flows` table using existing schema.

Timeline: after Blockscout V2 migration completes (reduces Etherscan dependency, frees quota for bridge monitoring).
