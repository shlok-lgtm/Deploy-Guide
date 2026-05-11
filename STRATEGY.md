# Basis Protocol — Strategic Context

> This file gives Claude Code the "why" behind every build decision.
> Read this once at session start. Refer back when making architectural choices.

## One-Line Thesis

Basis is the shared risk state of the network — a canonical risk registry where every wallet-exposed asset has a quality score.

## Strategic Principle

**"Privileged decisions require privileged data."**

AI models can generate risk analysis. Agents can execute decisions. Neither can verify their own inputs. Basis sits at the verification layer between them.

## What Basis Is

Decision integrity infrastructure. Not data. Not analytics. Not a rating agency.

Basis owns the **standard for risk surfaces** — the BRSS (Basis Risk Scoring Standard). Other systems consume scores from Basis the way they consume identity from ENS or prices from Chainlink.

Category analogs: ENS (registry), Okta (identity), Sigstore (software provenance).

**What Basis is NOT:** a wallet, a DEX, an aggregator, a portfolio manager, a consulting firm, a compliance tool. Infrastructure only.

## The V4 Evolution: Shared Risk State

Five compressions have shaped the thesis:

1. **v1 (supply-push):** Post to governance forums, wait for adoption
2. **v2 (Carfax consumer-pull):** Permissionless distribution channels
3. **v3 (agent-first):** Machine-verifiable trust layer, institutions buy so agents act safely
4. **v4 (shared state):** Basis = shared risk state of the network

### What changed in V4

The **Wallet Risk Graph** is the new primitive. Every Ethereum address gets a risk profile based on the assets it holds. The wallet is the universal join key — new risk surfaces (PSI, TTI, CVI) enrich the same graph. You don't build separate databases for each surface; you build one graph that every surface writes into.

Architecture: **BRSS spec → Basis Indices (SII/PSI/CVI/TTI) → Wallet Risk Graph**

The graph is on-chain, permissionless, and accumulates state over time. Historical state is unreplicable — that's the moat.

### Why "ENS for risk" not "Chainlink for risk"

Chainlink delivers prices (point-in-time facts). ENS is a registry (accumulated state that others build on). Basis is a risk registry — accumulated, composable, persistent state. The wallet graph makes this concrete, not theoretical.

## What's Live Today

- **SII v1.0.0** scoring 10 stablecoins (USDC, USDT, DAI, FRAX, PYUSD, FDUSD, TUSD, USDD, USDe, USD1)
- **Public dashboard** at basisprotocol.xyz
- **REST API** with 18+ endpoints
- **61 automated components** across peg stability, liquidity, flows, distribution, structural risk
- **Neon Postgres** with daily scoring, history, provenance tracking
- **Governance crawler** monitoring protocol forums for stablecoin mentions

## What Gets Built Next (Priority Order)

### 1. Wallet Risk Graph (THE priority)
- Index wallets permissionlessly via Etherscan/Alchemy
- Profile every address by its stablecoin holdings
- Compute wallet-level risk scores from existing SII scores
- Pre-compute so surfaces pull from the graph, not the other way around
- ~$500/mo infrastructure cost, 2-4 week build
- This is what makes V4 real

### 2. Pitch Deck Rebuild
- Full rebuild for V4 framing (wallet graph, proof discipline, earned optionality)
- Target: raising $4M seed at $25-40M pre by June 1, 2026
- Lead: Variant. Strategic: Dragonfly/Polychain, Coinbase Ventures, Village Global

### 3. On-Chain Infrastructure
- Oracle contract on Base/Arbitrum
- Keeper script for automated score publishing

### 4. Distribution Channels
- Twitter/X, Telegram, Discord bots (thin API wrappers)
- MCP server for agent framework listings (AgentKit, ElizaOS, Olas)
- MetaMask Snap, Safe Guard Module

### 5. BRSS Open Spec
- Publishes ~Day 30 of content arc (after SII proves demand)
- SII proves demand → BRSS reframes Basis from product to protocol

## Proof Discipline (V4.1)

Token = earned optionality, not assumed. Revenue projections use bear/base/bull, not "conservative."

**Six falsifiable assumptions ("what must be true"):**
1. Wallet users care about asset quality scores
2. Protocol teams reference external risk signals
3. Consumer adoption creates institutional pull
4. Methodology trust builds through stability
5. Free-to-paid upgrade conversion works
6. VCs fund consumer+protocol at $25M+ pre

**Success metrics prioritize adoption over infrastructure:**
- External lookups (not internal queries)
- Repeat users (not total users)
- % of volume that didn't originate from us (not volume we generated)

**Kill signals defined at Month 6/9/12/18.** If no protocol references SII by Month 6, reassess thesis.

## Competitive Positioning

**Neutrality is the moat.** Every capable incumbent has structural conflicts:
- Gauntlet/Chaos Labs: consulting revenue tied to score outcomes
- S&P/Moody's: opacity is the business model
- Credora: feeds (competitive — Basis is state, not feeds)
- Chainalysis/TRM: AML-focused (orthogonal — they score addresses for fraud, not assets for quality)
- ERC-8004: identity layer (complementary)

**Moats:**
- State accumulation: historical wallet risk graph is unreplicable once built
- Neutrality: no consulting conflicts, no customer-specific methodology
- Coordination lock-in: once hard-coded into contracts, switching requires audits + migrations + governance

## Expansion Order

Crypto → Cloud security → Software supply chain → Payments → Supply chains

Each domain adds a new risk surface to the same wallet graph. The wallet is the universal join key.

## Key Principles for Building

- **Legibility over accuracy.** First-mover indices win through adoptability, not methodological perfection.
- **Disclosure ≠ meaning.** Regulations mandate transparency; markets need comparability. The gap between disclosure and interpretation is the business.
- **On-chain state from day one.** Crypto re-fuses standards and implementations — state accumulation must begin immediately.
- **Agents amplify, not wedge.** Agents amplify need for verified inputs; they're not the primary sales motion.
- **SII first, BRSS second.** SII proves demand → BRSS reframes Basis as protocol. Never lead with BRSS before SII has traction.
- **Never say "rating."** Always: score, index, surface.
- **Honest framing.** "Validation" not "traction." Softened projections over inflated claims.

## Founder Context

Shlok Vaidya. 20+ years normalizing fragmented regulatory data (Quorum: $250K+ ASPs, $3B+ exits). Building Basis on the side until leave conditions are met. Based in Austin. Target: raising by June 1, 2026.

## Terminology

| Use | Don't Use |
|-----|-----------|
| Score | Rating |
| Index | Indicator |
| Surface | Product |
| Decision integrity infrastructure | Data analytics platform |
| Validation | Traction |
| Bear/base/bull | Conservative |
