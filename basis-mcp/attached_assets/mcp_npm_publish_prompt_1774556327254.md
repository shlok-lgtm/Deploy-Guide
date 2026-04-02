# Publish MCP Server to npm + Prepare Registry Submission

## What & Why

The Basis MCP server is built and working. It needs to be published to npm so it can be listed on MCP registries (Official MCP Registry, Smithery, Glama, PulseMCP). The npm package is the gate — every registry references it.

## Done looks like

* `@basis-protocol/mcp-server` is live on npmjs.com as a public package
* Running `npx @basis-protocol/mcp-server` starts the server in stdio mode
* Running `npx @basis-protocol/mcp-server --http` starts the HTTP server
* `.mcp/server.json` exists in the repo root with correct metadata
* README.md in `artifacts/basis-mcp/` has clear install + usage instructions

## Tasks

### 1. Verify the build compiles clean

```bash
cd artifacts/basis-mcp
npm install
npm run build
```

Fix any TypeScript errors. The build output goes to `artifacts/basis-mcp/build/`.

### 2. Verify the API URL is correct

Check `artifacts/basis-mcp/src/config.ts` — `BASE_URL` must be `https://basis-deploy-guide.replit.app`. If it's anything else, fix it and rebuild.

### 3. Test the server runs

```bash
cd artifacts/basis-mcp
echo '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | node build/index.js
```

It should output a JSON response listing all 8 tools. If it hangs waiting for stdin, that's expected for stdio mode — the `tools/list` test confirms it starts.

### 4. Update the README

Replace the existing `artifacts/basis-mcp/README.md` with a clear, concise README:

```markdown
# @basis-protocol/mcp-server

MCP server for Basis Protocol — verifiable risk intelligence for on-chain finance.

Query stablecoin integrity scores, wallet risk profiles, transaction risk assessment, and scoring methodology across 44,000+ wallets tracking $67B+ in stablecoin value.

## Install

```bash
npx @basis-protocol/mcp-server
```

## Tools

| Tool | Description |
|------|-------------|
| `get_stablecoin_scores` | All scored stablecoins with SII scores and grades |
| `get_stablecoin_detail` | Full score breakdown for a specific stablecoin |
| `get_wallet_risk` | Risk profile for any Ethereum wallet |
| `get_riskiest_wallets` | Wallets with most at-risk capital |
| `get_scoring_backlog` | Unscored assets ranked by capital exposure |
| `check_transaction_risk` | Composite risk assessment: asset + sender + receiver |
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
- 44,000+ wallets indexed on Ethereum mainnet
- $67B+ in stablecoin value tracked
- Risk scores, concentration analysis (HHI), coverage quality
- Deterministic, version-controlled methodology

## Links

- [Live Dashboard](https://basis-deploy-guide.replit.app)
- [GitHub](https://github.com/shlok-lgtm/basis-mcp)
- [Basis Protocol](https://basisprotocol.xyz)
```

### 5. Publish to npm

```bash
cd artifacts/basis-mcp
npm login
npm publish --access public
```

Verify the package is live at `https://www.npmjs.com/package/@basis-protocol/mcp-server`.

### 6. Create `.mcp/server.json` in repo root

Create the file at the ROOT of the repo (not inside artifacts), at `.mcp/server.json`:

```json
{
  "name": "io.github.shlok-lgtm/basis-mcp",
  "description": "Verifiable risk intelligence for on-chain finance. Query stablecoin integrity scores, wallet risk profiles, transaction risk assessment, and methodology transparency.",
  "repository": {
    "url": "https://github.com/shlok-lgtm/basis-mcp",
    "source": "https://github.com/shlok-lgtm/basis-mcp",
    "homepage": "https://basis-deploy-guide.replit.app"
  },
  "version_detail": {
    "version": "1.0.0",
    "release_date": "2026-03-26",
    "is_latest": true
  },
  "packages": [
    {
      "registry_name": "npm",
      "name": "@basis-protocol/mcp-server",
      "version": "1.0.0"
    }
  ],
  "remotes": [
    {
      "transport_type": "streamable-http",
      "url": "https://basis-deploy-guide.replit.app/mcp"
    }
  ]
}
```

### 7. Push everything to GitHub

```bash
git add -A
git commit -m "Publish @basis-protocol/mcp-server v1.0.0 + registry metadata"
git push
```

## Do NOT

* Change any tool implementations or API call logic
* Modify the server entry point behavior
* Deploy anything to the hub (Deploy-Guide) repo
* Trigger the wallet indexer
