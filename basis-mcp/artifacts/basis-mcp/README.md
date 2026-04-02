# @basis-protocol/mcp-server

<!-- mcp-name: io.github.shlok-lgtm/basis-mcp -->

MCP server for Basis Protocol — verifiable risk intelligence for on-chain finance.

Query stablecoin integrity scores (SII), DeFi protocol solvency scores (PSI), composite risk (CQI), wallet risk profiles, and transaction risk assessment across 44,000+ wallets tracking $67B+ in stablecoin value and 13 scored protocols on Ethereum and Solana.

## Install

```bash
npx @basis-protocol/mcp-server
```

## Tools

### SII — Stablecoin Integrity Index

| Tool | Description |
|------|-------------|
| `get_stablecoin_scores` | All scored stablecoins with SII scores and grades |
| `get_stablecoin_detail` | Full score breakdown for a specific stablecoin |

### PSI — Protocol Solvency Index

| Tool | Description |
|------|-------------|
| `get_protocol_score` | PSI score + grade + category breakdown for a protocol |
| `get_protocol_rankings` | All scored protocols ranked by PSI |
| `get_protocol_exposure` | Protocol stablecoin exposure with SII cross-reference |

### CQI — Composite Quality Index

| Tool | Description |
|------|-------------|
| `get_cqi` | Composite risk for stablecoin x protocol pairs (e.g. USDC in Drift) |

### Wallet Risk

| Tool | Description |
|------|-------------|
| `get_wallet_risk` | Risk profile for any Ethereum wallet |
| `get_wallet_holdings` | Per-asset holdings breakdown with SII scores |
| `get_riskiest_wallets` | Wallets with most at-risk capital |
| `get_scoring_backlog` | Unscored assets ranked by capital exposure |
| `check_transaction_risk` | Composite risk assessment: asset + sender + receiver |

### Analysis & Methodology

| Tool | Description |
|------|-------------|
| `get_drift_exploit_analysis` | Structured analysis of the Drift Protocol exploit (April 2026) |
| `get_methodology` | Current SII formula, weights, and version |

## Usage with Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "basis-protocol": {
      "command": "npx",
      "args": ["-y", "@basis-protocol/mcp-server"]
    }
  }
}
```

## Usage with HTTP transport

```bash
npx @basis-protocol/mcp-server --http
# Listens on port 3000, endpoint: /mcp
```

## Data

- 17 stablecoins scored (SII v1.0.0)
- 13 DeFi protocols scored (PSI v0.1.0) — Ethereum + Solana
- 44,000+ wallets indexed on Ethereum mainnet
- $67B+ in stablecoin value tracked
- CQI composition: stablecoin x protocol risk pairs
- Risk scores, concentration analysis (HHI), coverage quality
- Deterministic, version-controlled methodology

## Links

- [Live Dashboard](https://basisprotocol.xyz)
- [GitHub](https://github.com/shlok-lgtm/basis-mcp)
- [Basis Protocol](https://basisprotocol.xyz)
