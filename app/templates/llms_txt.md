# Basis Protocol

> Computed, attested risk surfaces for on-chain finance. Standardized
> scoring and evidence for stablecoins, protocols, LSTs, bridges, DAOs,
> vaults, exchanges, and tokenized treasuries.

## Data surfaces

- [Rankings](https://basisprotocol.xyz/rankings): Live entity scores across 9 indices.
- [Entity pages](https://basisprotocol.xyz/sitemap.xml): Individual risk profiles for 113+ scored entities. Each entity page has a .md alternate at {entity_url}.md.
- [API](https://basisprotocol.xyz/api/scores): Machine-readable score, evidence, and composition endpoints.
- [Paid endpoints](https://basisprotocol.xyz/.well-known/x402): 12 x402-gated endpoints for agent consumption.

## Indices

- **SII** (Stablecoin Integrity Index): 36 stablecoins scored hourly. Formula: 0.30×Peg + 0.25×Liquidity + 0.15×MintBurn + 0.10×Distribution + 0.20×Structural.
- **PSI** (Protocol Solvency Index): 13 protocols scored hourly.
- **RPI** (Risk Posture Index): 13 protocols scored weekly.
- **LSTI** (Liquid Staking Token Index): LSTs above $10M TVL.
- **BRI** (Bridge Risk Index): Bridges above $50M TVL.
- **DOHI** (DAO Operational Health Index): DAOs above $50M TVL.
- **VSRI** (Vault/Yield Strategy Risk Index): Vaults above $10M TVL.
- **CXRI** (Centralized Exchange Reserve Index): Exchanges with CoinGecko trust scores.
- **TTI** (Tokenized Treasury Index): RWA products above $10M AUM.
- **CQI** (Composite Quality Index): Geometric mean of SII × PSI per protocol-stablecoin pair.

## Methodology

- [Methodology](https://basisprotocol.xyz/api/methodology): Open formula and weights. Deterministic. Version-controlled.
- [Proof pages](https://basisprotocol.xyz/proof/sii/usdc): Computation provenance per entity — input hashes, formula version, score derivation.
- [Witness](https://basisprotocol.xyz/witness): Evidence archive — CDA attestation documents with extraction provenance.

## Key API endpoints

- `GET /api/scores` — All SII scores
- `GET /api/scores/{coin}` — Single stablecoin detail
- `GET /api/psi/scores` — All PSI scores
- `GET /api/compose/cqi` — Composite quality index
- `GET /api/wallets/{address}` — Wallet risk profile
- `GET /api/divergence` — Capital flow / quality mismatches
- `GET /api/pulse/latest` — Daily risk surface snapshot

## Agent integration

- [MCP tools](https://basisprotocol.xyz/mcp): Model Context Protocol tools for AI agents. 18 tools across oracle, CQI, witness, and divergence domains.
- [Agent card](https://basisprotocol.xyz/.well-known/agent-card.json): Identity, capabilities, payment protocol discovery.

## Machine-readable alternates

Every canonical HTML page has a markdown alternate at `{url}.md`. Content negotiation via `Accept: text/markdown` is also supported.

## Contact

shlok@basisprotocol.xyz
