# Basis Protocol — Integration Guide

**Version**: 1.0 · April 2026
**Audience**: Protocol teams, risk committees, integration engineers
**Worked example**: Aave V4

---

## What This Guide Covers

Basis produces deterministic, versioned risk scores for stablecoins (SII), protocols (PSI), and composed pairs (CQI). This guide describes how protocols consume Basis data — not philosophically, but with endpoints, code, and concrete integration architecture.

Every consumer uses Basis in one or more of four modes:

| Mode | What it does | Primary consumers |
|------|-------------|-------------------|
| **Watchtower** | Continuous monitoring with threshold alerts | Exchanges, treasuries, risk teams |
| **Parameter Input** | Component-level data feeding risk parameters | Lending protocols, risk committees |
| **Governance Citation** | Verifiable references in proposals | DAOs, governance forums |
| **Enforcement Surface** | On-chain or off-chain checks at transaction time | Smart contracts, agents, Safe modules |

---

## Mode 1: Watchtower — Continuous Monitoring

### What it does

Watch scored entities for threshold crossings, divergence signals, or classification boundary proximity. Alert before a collateral stablecoin degrades, not after.

### Endpoints

| Endpoint | Returns |
|----------|---------|
| `GET /api/scores` | All SII scores with category breakdowns |
| `GET /api/scores/{coin}` | Full SII detail: 5 categories, structural breakdown, individual components |
| `GET /api/psi/scores` | All protocol solvency scores |
| `GET /api/psi/scores/{slug}` | Single protocol PSI detail |
| `GET /api/discovery/latest` | Top 20 cross-domain divergence signals from last 7 days |
| `GET /api/divergence/assets` | Asset-level quality/flow divergence signals |
| `GET /api/pulse/latest` | Daily system pulse with state root |

### Integration pattern

Poll SII for monitored stablecoins on a schedule. Alert when scores cross thresholds.

```python
import httpx, time

WATCHED_COINS = ["usdt", "usdc", "dai", "frax", "pyusd"]
ALERT_THRESHOLD = 75.0
BASIS_API = "https://api.basisprotocol.com"

def check_scores():
    """Poll SII scores and alert on threshold crossings."""
    resp = httpx.get(f"{BASIS_API}/api/scores")
    scores = resp.json().get("scores", [])

    for s in scores:
        coin_id = s.get("stablecoin_id", "")
        if coin_id not in WATCHED_COINS:
            continue

        overall = float(s.get("overall_score", 0))

        if overall < ALERT_THRESHOLD:
            send_alert(
                f"SII ALERT: {coin_id} dropped to {overall}. "
                f"Threshold: {ALERT_THRESHOLD}. "
                f"Peg: {s.get('peg_score')}, Liquidity: {s.get('liquidity_score')}"
            )

# Run hourly — matches the Basis scoring cycle
while True:
    check_scores()
    time.sleep(3600)
```

### Divergence monitoring

Divergence signals detect when capital flows and quality metrics disagree — e.g., capital flowing into an asset whose score is deteriorating. These are early warning indicators.

```python
def check_divergence():
    resp = httpx.get(f"{BASIS_API}/api/discovery/latest")
    signals = resp.json().get("signals", [])
    for sig in signals:
        if sig.get("novelty_score", 0) > 0.7:
            send_alert(
                f"DIVERGENCE: {sig['title']} — {sig['description']} "
                f"(novelty: {sig['novelty_score']:.2f})"
            )
```

### What this replaces

For Aave: this replaces the continuous monitoring component of the Chaos Labs engagement. Instead of a proprietary dashboard with opaque methodology, the watchtower reads from a public scoring system with verifiable derivation.

---

## Mode 2: Parameter Input — Component-Level Risk Data

### What it does

Risk committees consume individual SII components to calibrate protocol parameters — LTV, liquidation threshold, supply caps — instead of relying solely on the composite score.

### The "too coarse" response

The composite SII is a comparability layer. It tells you which stablecoins are broadly sound. But the product is the components.

The `GET /api/scores/{coin}` endpoint returns the composite score **and** all individual components organized by category:

**5 category scores** (each 0-100, weighted into the composite):
- `peg` (30% weight) — price deviation, volatility, depeg duration
- `liquidity` (25% weight) — DEX depth, CEX volume, Curve pool balance
- `flows` (15% weight) — mint/burn activity, flow patterns
- `distribution` (10% weight) — holder concentration, whale exposure
- `structural` (20% weight) — reserves, smart contract, oracle, governance, network

**5 structural sub-scores**:
- `reserves` — collateral quality, attestation frequency, reserve ratio
- `contract` — audit coverage, upgrade risk, proxy patterns
- `oracle` — price feed diversity, freshness, deviation handling
- `governance` — multisig structure, timelock, governance activity
- `network` — chain diversity, bridge exposure, deployment breadth

**Individual component readings**: Each component has a `component_id`, `category`, `raw_value`, `normalized_score` (0-100), `data_source`, and `collected_at` timestamp with source attribution (`live_api`, `cda_extraction`, or `static_config`).

### Parameter mapping

A lending protocol maps components to parameters:

| Basis Component | Protocol Parameter | Mapping Logic |
|----------------|-------------------|---------------|
| `peg` score | `liquidationThreshold` | Higher peg stability → higher LT (less likely to depeg during liquidation) |
| `liquidity` score | `ltv` | Deeper liquidity → higher LTV (easier to liquidate at fair price) |
| `flows` score | Supply/borrow cap adjustments | Abnormal flow patterns → tighter caps |
| `distribution` score | Concentration risk flag | Low distribution → manual review trigger |
| `structural` composite | Collateral tier classification | Below 60 → restricted tier, below 40 → delist candidate |

### Concrete example for Aave

```python
def map_sii_to_aave_params(coin_id: str) -> dict:
    """Map SII components to Aave V4 risk parameters."""
    resp = httpx.get(f"{BASIS_API}/api/scores/{coin_id}")
    data = resp.json()

    peg = data["categories"]["peg"]["score"]
    liq = data["categories"]["liquidity"]["score"]
    flows = data["categories"]["flows"]["score"]
    dist = data["categories"]["distribution"]["score"]
    structural = data["categories"]["structural"]["score"]

    # Peg stability → liquidation threshold
    # High peg (>85) → LT at 95%. Low peg (<60) → LT at 80%.
    lt = 80 + min(15, max(0, (peg - 60) * 0.6))

    # Liquidity depth → LTV
    # Deep liquidity (>85) → LTV at 80%. Thin (<50) → LTV at 60%.
    ltv = 60 + min(20, max(0, (liq - 50) * 0.57))

    # Flows → supply cap multiplier
    # Normal flows (>70) → 1.0x. Abnormal (<40) → 0.5x.
    cap_mult = max(0.5, min(1.0, flows / 70))

    return {
        "coin": coin_id,
        "liquidation_threshold": round(lt, 1),
        "ltv": round(ltv, 1),
        "supply_cap_multiplier": round(cap_mult, 2),
        "structural_tier": (
            "full" if structural >= 70 else
            "restricted" if structural >= 40 else
            "delist_candidate"
        ),
        "needs_review": dist < 50,
        "basis_score": data["score"],
        "basis_score": data["score"],
        "computed_at": data["computed_at"],
    }
```

### Composite vs. components

| Use case | Consume | Why |
|----------|---------|-----|
| Binary accept/reject | Composite SII | "Is this stablecoin broadly sound?" |
| Parameter tuning | Category scores | "How should we set LTV for this specific asset?" |
| Deep risk analysis | Individual components | "What exactly is driving this score change?" |
| Cross-protocol comparison | CQI | "How risky is USDT specifically in Aave vs. in Compound?" |

### Custom index definitions

The index definition schema lets anyone define a custom weighting over the same components. A "Lending Collateral Quality Index" that weights peg at 40% and liquidity at 30% is a lens configuration, not new engineering:

```
GET /api/lenses
```

Returns available regulatory and analytical lenses. Custom lenses can be created via `POST /api/lenses` with a JSON schema defining category weights, thresholds, and classification rules.

CQI already demonstrates composition — it combines SII and PSI into protocol-specific stablecoin risk via `GET /api/compose/cqi?asset={coin}&protocol={slug}`.

---

## Mode 3: Governance Citation — Verifiable References

### What it does

Governance proposals link to Basis reports as evidence. The citation is verifiable — the score derivation is public, the attestation chain links to on-chain hashes, anyone can audit.

### Report endpoints

| Endpoint | Returns |
|----------|---------|
| `GET /api/reports/stablecoin/{symbol}` | Full compliance report (HTML or JSON) |
| `GET /api/reports/stablecoin/{symbol}?lens=MICA67` | Lens-specific compliance report |
| `GET /api/reports/protocol/{slug}` | Protocol risk report |
| `GET /api/reports/verify/{report_hash}` | Report verification |
| `GET /api/provenance/verify/{attestation_hash}` | Attestation verification |
| `GET /api/state-root/latest` | Latest state root with all attestation domains |

Reports are generated with attestation hashes that can be verified against on-chain state roots published by the oracle.

### How to cite in a governance proposal

1. Generate the report: `GET /api/reports/stablecoin/usdc?lens=SCO60&format=json`
2. The response includes a `report_hash` — a SHA-256 hash of the scored data + methodology version
3. The hash is published on-chain by the oracle via `publishReportHash(entityId, reportHash, lensId)`
4. Reference the report hash in your proposal — anyone can verify by calling `getReportHash(entityId)` on the oracle

### Draft governance proposal template

```markdown
# [ARFC] Adopt Basis SII as Collateral Quality Input

## Summary

This proposal integrates the Basis Stablecoin Integrity Index (SII) as a
standardized input for collateral risk parameter decisions in [Protocol].

## Motivation

Collateral stablecoin risk assessment currently relies on [ad-hoc analysis /
vendor reports / committee judgment]. This creates:
- No standardized comparison across stablecoin collateral
- No continuous monitoring with automated threshold alerts
- No verifiable, on-chain audit trail for risk decisions

## Specification

1. **Watchtower**: Monitor SII scores for all collateral stablecoins via
   the Basis API. Alert the risk committee when any score drops below 70.

2. **Parameter input**: Map SII category scores to risk parameters:
   - `peg` score → liquidation threshold adjustments
   - `liquidity` score → LTV calibration
   - `structural` score → collateral tier classification

3. **Governance citation**: All future collateral proposals must reference
   the current Basis report with attestation hash for the asset under
   consideration.

4. **On-chain enforcement** (Phase 2): Deploy a BasisCollateralChecker
   that reads from the Basis Oracle before accepting new collateral.

## Evidence

Current SII scores for [Protocol] collateral stablecoins:

| Asset | SII Score | Peg | Liquidity | Structural |
|-------|-----------|-----|-----------|------------|
| USDC  | [score]   | [peg] | [liq] | [struct] |
| USDT  | [score]   | [peg] | [liq] | [struct] |
| DAI   | [score]   | [peg] | [liq] | [struct] |

Report attestation hash: `[hash]`
Verification: `GET /api/reports/verify/[hash]`
On-chain: `oracle.getReportHash([entityId])` on Base ([oracle_address])

## Cost

Basis API access for continuous monitoring and reporting: $[amount]/year
vs. current risk vendor engagement at $[amount]/year.
```

---

## Mode 4: Enforcement Surface — On-Chain and Off-Chain Checks

### What it does

Contracts or modules read Basis state before allowing transactions. Below threshold: block, warn, or require override.

### On-chain: Basis Oracle

The Basis Oracle is deployed on Base and Arbitrum at:

```
Base:     0x1651d7b2e238a952167e51a1263ffe607584db83
Arbitrum: 0x1651d7b2e238a952167e51a1263ffe607584db83
```

**Read functions:**

```solidity
// SII score for a stablecoin token
function getScore(address token) external view returns (
    uint16 score,      // 0-10000 (divide by 100 for 0-100 scale)
    bytes2 grade,      // reserved (unused — no letter grades)
    uint48 timestamp,  // when score was computed
    uint16 version     // methodology version
);

// PSI score for a protocol
function getPsiScore(string calldata protocolSlug) external view returns (
    uint16 score, bytes2 grade, uint48 timestamp, uint16 version
);

// CQI — composed quality of a stablecoin within a specific protocol
function getCqi(
    address siiToken,
    string calldata psiProtocolSlug
) external view returns (uint16 cqiScore);

// Staleness check
function isStale(address token, uint256 maxAge) external view returns (bool);

// Report attestation
function getReportHash(bytes32 entityId) external view returns (
    bytes32 reportHash, bytes4 lensId, uint48 timestamp
);

// State root (aggregate attestation of all scoring domains)
function latestStateRoot() external view returns (bytes32);
```

**Note on scoring precision**: On-chain scores are uint16 (0-10000) for gas efficiency. Divide by 100 to get the 0-100 scale used in the API. A threshold of 7500 on-chain equals 75.0 in the API.

### Solidity: Collateral quality checker

```solidity
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {IBasisSIIOracle} from "./interfaces/IBasisSIIOracle.sol";

/// @title BasisCollateralChecker
/// @notice Checks SII score before accepting collateral deposits.
contract BasisCollateralChecker {
    IBasisSIIOracle public immutable oracle;
    uint16 public minScore;       // e.g., 7500 = 75.0
    uint256 public maxScoreAge;   // e.g., 7200 = 2 hours

    constructor(address _oracle, uint16 _minScore, uint256 _maxScoreAge) {
        oracle = IBasisSIIOracle(_oracle);
        minScore = _minScore;
        maxScoreAge = _maxScoreAge;
    }

    modifier onlyQualityCollateral(address token) {
        (uint16 score, , uint48 ts, ) = oracle.getScore(token);
        require(score >= minScore, "Basis: score below threshold");
        require(!oracle.isStale(token, maxScoreAge), "Basis: score stale");
        _;
    }

    // Example: wrap a deposit function
    function deposit(address token, uint256 amount)
        external
        onlyQualityCollateral(token)
    {
        // ... deposit logic
    }
}
```

### Safe Guard module

The BasisSafeGuard is a Safe Guard module that checks SII and CQI before any Safe multisig transaction executes. When a Safe transaction targets a known protocol contract and involves a monitored stablecoin token, the Guard:

1. Checks SII score for the token against `threshold` (default minimum)
2. If the target address is a registered protocol (`protocolSlugs` mapping), also checks CQI against `cqiThreshold`
3. Reverts the transaction if either score is below threshold or stale

**Status**: Contract written and tested (Foundry test suite), pending first production deployment.

### Agent enforcement via MCP

The Basis MCP server exposes 11 tools for AI agent integration:

| Tool | Purpose |
|------|---------|
| `get_stablecoin_scores` | All SII rankings |
| `get_stablecoin_detail` | Full score breakdown for a coin |
| `get_wallet_risk` | Wallet risk profile |
| `get_wallet_holdings` | Holdings with per-asset SII scores |
| `get_riskiest_wallets` | Highest-risk wallets by capital exposure |
| `get_scoring_backlog` | Unscored assets ranked by exposure |
| `check_transaction_risk` | Pre-transaction risk assessment |
| `get_methodology` | Current formula and weights |
| `get_divergence_signals` | Cross-domain anomaly signals |
| `query_template` | Pre-built analytical queries |
| `get_treasury_events` | Treasury behavioral events |

An agent uses `check_transaction_risk` before executing a stablecoin swap to verify that both the asset and counterparty meet minimum quality thresholds.

### x402 agent payments

For agent-to-agent commerce, Basis endpoints are available via x402 (HTTP 402 payment protocol, USDC on Base):

| Endpoint | Price |
|----------|-------|
| `GET /api/paid/sii/rankings` | $0.005 |
| `GET /api/paid/sii/{coin}` | $0.001 |
| `GET /api/paid/psi/scores` | $0.005 |
| `GET /api/paid/psi/scores/{slug}` | $0.001 |
| `GET /api/paid/cqi` | $0.001 |
| `GET /api/paid/pulse/latest` | $0.002 |
| `GET /api/paid/discovery/latest` | $0.005 |
| `GET /api/paid/wallets/{address}/profile` | $0.005 |
| `GET /api/paid/report/{entity_type}/{entity_id}` | $0.01 |

---

## Worked Example: Aave V4 Integration

### Context

Aave's Chaos Labs engagement ($5-8M/year) ended. The protocol needs continuous collateral quality monitoring, parameter input for V4 risk settings, governance-quality evidence for collateral decisions, and optionally automated enforcement for collateral onboarding.

### What Aave needs vs. what Basis provides

| Need | Current State | Basis Solution |
|------|--------------|----------------|
| Continuous collateral monitoring | Manual / ad-hoc | **Watchtower**: poll `/api/scores` hourly, alert on threshold crossings |
| Parameter input for LTV/LT/caps | Committee judgment | **Parameter input**: map `peg` → LT, `liquidity` → LTV, `structural` → tier |
| Evidence for governance proposals | Vendor reports | **Governance citation**: report with attestation hash, on-chain verifiable |
| Automated enforcement | None | **Enforcement**: oracle read in collateral pipeline + Safe Guard |

### Aave collateral stablecoins

Aave V3/V4 lists the following stablecoins as collateral. Each gets an SII score:

- **USDC** — highest liquidity, strong peg, full reserves attestation
- **USDT** — highest market cap, peg stable, reserves methodology evolving
- **DAI** — decentralized, MakerDAO governance, multi-collateral backing
- **FRAX** — algorithmic/collateralized hybrid, governance active
- **PYUSD** — PayPal-backed, newer, growing liquidity
- **LUSD** — Liquity V1, pure ETH-backed, immutable contract

### Integration architecture

```
┌─────────────────────────────────────────────────┐
│                  Aave V4 Stack                   │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │
│  │ Risk     │  │ Governor │  │ Pool Manager │  │
│  │ Committee│  │ Forum    │  │ (on-chain)   │  │
│  └────┬─────┘  └────┬─────┘  └──────┬───────┘  │
│       │              │               │           │
│  Parameters     Citations      Enforcement      │
│       │              │               │           │
└───────┼──────────────┼───────────────┼───────────┘
        │              │               │
   ┌────┴────┐   ┌─────┴────┐   ┌─────┴─────┐
   │ Basis   │   │ Basis    │   │ Basis     │
   │ API     │   │ Reports  │   │ Oracle    │
   │ /scores │   │ /reports │   │ on-chain  │
   └─────────┘   └──────────┘   └───────────┘
```

**Frequency**:
- Watchtower polls: every 60 minutes (matches Basis scoring cycle)
- Parameter review: weekly or on threshold crossing
- Governance citations: per proposal
- Oracle reads: per transaction (enforcement mode)

### Draft ARFC for Aave

```markdown
# [ARFC] Integrate Basis Protocol SII for Stablecoin Collateral Risk

## Summary

Integrate the Basis Stablecoin Integrity Index (SII) as a continuous,
verifiable input for Aave V4 stablecoin collateral risk management,
replacing ad-hoc assessment with deterministic, versioned scoring.

## Motivation

Following the conclusion of the Chaos Labs risk engagement, Aave lacks a
standardized, continuous collateral quality monitoring system. Current
collateral decisions rely on snapshot analysis without ongoing scoring,
making it difficult to detect gradual degradation or respond to rapid
deterioration.

The Basis SII provides:
- Deterministic scoring across 5 categories and 37 components
- Hourly refresh cycle with on-chain attestation
- Public methodology (v1.0.0) with versioned weights
- Component-level granularity for parameter tuning
- Cross-protocol composition via CQI

## Specification

### Phase 1: Monitoring + Parameter Input

1. Deploy a Basis watchtower that polls SII scores for all Aave
   stablecoin collateral assets every 60 minutes.

2. Configure alert thresholds:
   - SII < 75: notify risk committee
   - SII < 60: trigger emergency parameter review
   - SII < 40: auto-propose collateral freeze

3. Establish parameter mapping for quarterly review:
   - `peg` category → liquidation threshold calibration
   - `liquidity` category → LTV ceiling
   - `structural` composite → collateral tier (full / restricted / frozen)

4. All collateral proposals must include Basis report with attestation hash.

### Phase 2: On-Chain Enforcement

5. Deploy BasisCollateralChecker that reads from the Basis Oracle
   (Base: 0x1651d7b2e238a952167e51a1263ffe607584db83) before accepting
   new collateral supply.

6. Configure minimum SII threshold of 7500 (75.0) with 2-hour staleness
   window.

### Phase 3: Safe Guard

7. Enable BasisSafeGuard on the Aave governance Safe, checking CQI for
   protocol-specific stablecoin risk before executing treasury operations.

## Evidence

Current SII scores for Aave collateral stablecoins are available at:
- API: `GET /api/scores`
- Reports: `GET /api/reports/stablecoin/{symbol}?lens=SCO60`
- Oracle: `getScore(tokenAddress)` on Base

## Cost

Annual Basis Protocol subscription for enterprise-tier access
(continuous monitoring, component API, attested reports, oracle reads)
vs. previous risk vendor engagement.

## References

- Basis Methodology: `GET /api/methodology`
- SII Formula v1.0.0: 0.30*Peg + 0.25*Liquidity + 0.15*Flows + 0.10*Distribution + 0.20*Structural
- Oracle contract: Base 0x1651d7b2e238a952167e51a1263ffe607584db83
```

---

## Appendix A: API Quick Reference

### Public endpoints (no auth required)

```
GET  /api/health                              System health
GET  /api/scores                              All SII scores
GET  /api/scores/{coin}                       SII detail + components
GET  /api/scores/{coin}/history?days=90       Historical SII
GET  /api/scores/{coin}/recent                Recent score snapshots
GET  /api/psi/scores                          All PSI scores
GET  /api/psi/scores/{slug}                   PSI detail
GET  /api/psi/definition                      PSI methodology
GET  /api/compose/cqi?asset={}&protocol={}    Composed Quality Index
GET  /api/compose/cqi/matrix                  Full CQI matrix
GET  /api/methodology                         SII formula + weights
GET  /api/lenses                              Available regulatory lenses
GET  /api/lenses/{lens_id}                    Lens definition
GET  /api/discovery/latest                    Divergence signals
GET  /api/divergence/assets                   Asset divergence
GET  /api/divergence/wallets                  Wallet divergence
GET  /api/pulse/latest                        Daily system pulse
GET  /api/reports/{type}/{id}                 Attested report (HTML/JSON)
GET  /api/reports/{type}/{id}?lens=MICA67     Lens-specific report
GET  /api/reports/verify/{hash}               Report hash verification
GET  /api/provenance/verify/{hash}            Attestation verification
GET  /api/state-root/latest                   Latest state root
GET  /api/treasury/events                     Treasury flow events
```

### On-chain (Base + Arbitrum)

```
Oracle: 0x1651d7b2e238a952167e51a1263ffe607584db83

getScore(address token) → (uint16 score, bytes2 _reserved, uint48 timestamp, uint16 version)
getPsiScore(string slug) → (uint16 score, bytes2 _reserved, uint48 timestamp, uint16 version)
getCqi(address token, string slug) → uint16
isStale(address token, uint256 maxAge) → bool
getReportHash(bytes32 entityId) → (bytes32 hash, bytes4 lensId, uint48 timestamp)
latestStateRoot() → bytes32
```

## Appendix B: Component Inventory

The SII scores each stablecoin across 5 categories. Each category aggregates individual component readings from live API sources, CDA (Continuous Disclosure Analysis) extraction, and static configuration.

### Categories and weights (v1.0.0)

| Category | Weight | What it measures |
|----------|--------|-----------------|
| Peg Stability | 30% | Price deviation from $1.00, volatility, depeg duration, recovery speed |
| Liquidity Depth | 25% | DEX pool depth, CEX volume, Curve pool balances, slippage estimates |
| Mint/Burn Dynamics | 15% | Issuance/redemption activity, flow patterns, velocity |
| Holder Distribution | 10% | Concentration (Gini), whale exposure, holder count trends |
| Structural Risk | 20% | Reserves, smart contract, oracle, governance, network (5 sub-scores) |

### Structural sub-scores

| Sub-score | Weight (of structural) | What it measures |
|-----------|----------------------|-----------------|
| Reserves/Collateral | 30% | Reserve ratio, attestation frequency, collateral quality |
| Smart Contract | 20% | Audit coverage, upgrade patterns, proxy risk |
| Oracle Integrity | 15% | Price feed diversity, freshness, deviation handling |
| Governance | 20% | Multisig structure, timelocks, governance activity |
| Network/Chain | 15% | Multi-chain deployment, bridge exposure, chain diversity |

Each component reading includes source attribution, so consumers can trace any score change back to the specific data point that caused it.
