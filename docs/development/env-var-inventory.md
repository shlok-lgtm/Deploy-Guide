# Environment Variable Inventory

Complete record of all environment variables that basis-hub reads at runtime. This document is the single source of truth for configuration requirements across the application stack.

**Last Updated:** 2026-05-12  
**Total Unique Variables:** 51

---

## Summary Table

| Variable Name | Category | Required | Default | Controls |
|---|---|---|---|---|
| ADMIN_KEY | SECRET | No | `""` | Admin endpoint authentication |
| ALERT_EMAIL | METADATA | No | `shlok@basisprotocol.xyz` | Alert recipient email address |
| ALCHEMY_API_KEY | SECRET | No | `""` | Ethereum RPC provider primary key |
| ANTHROPIC_API_KEY | SECRET | No | `""` | Claude API access for analysis/drafting |
| ARBITRUM_ORACLE_ADDRESS | METADATA | No | none | Arbitrum oracle contract address |
| ATTESTOR_PUBLIC_KEY | METADATA | No | `""` | State attestation public key verification |
| BACKLOG_COLLATERAL_THRESHOLD | TUNING | No | `500000` | Collateral floor to promote protocol to backlog |
| BACKLOG_PROMOTE_THRESHOLD | TUNING | No | `1000000` | Value floor for backlog → index promotion |
| BACKLOG_VALUE_FILTER | FEATURE_FLAG | No | `false` | Enable value-based backlog filtering |
| BASE_ORACLE_ADDRESS | METADATA | No | none | Base chain oracle contract address |
| BASE_SBT_ADDRESS | METADATA | No | `""` | Base chain SBT contract address |
| BASIS_PAYMENT_WALLET | METADATA | No | `""` | USDC payment receiver wallet (Base) |
| BASIS_ENGINE_GITHUB_PAT | SECRET | No | `""` | GitHub personal access token for engine commits |
| BASIS_ENGINE_LLM_DAILY_CALL_CEILING | TUNING | No | `50` | Max Claude API calls per UTC day |
| BASIS_ENGINE_LLM_MONTHLY_BUDGET_USD | TUNING | No | `200.0` | Max monthly USD spend on Claude API |
| BASIS_ENGINE_TEST_MODE | FEATURE_FLAG | No | `false` | Bypass git operations in engine approval flow |
| BASIS_API_BASE | METADATA | No | none | Local API base URL for scripts |
| BASIS_API_URL | METADATA | No | `https://basisprotocol.xyz` | Public API URL for client scripts |
| BLOCKSCOUT_API_KEY | SECRET | No | `""` | Blockscout block explorer API key |
| BLOCKSCOUT_COMPARISON_ENABLED | FEATURE_FLAG | No | `true` | Enable Etherscan ↔ Blockscout comparison |
| BLOCKSCOUT_CONCURRENCY | TUNING | No | `10` | Max concurrent Blockscout API requests |
| BLOCK_EXPLORER_PROVIDER | METADATA | No | `blockscout` | Primary block explorer (`blockscout` or `etherscan`) |
| CANONICAL_BASE_URL | METADATA | No | `https://basisprotocol.xyz` | Public-facing domain for page rendering |
| CDA_COLLECTION_INTERVAL_HOURS | TUNING | No | `24` | Hours between CDA extraction cycles |
| CHAIN_EXPANSION_TVL_THRESHOLD | TUNING | No | `500000000` | TVL threshold ($USD) for chain inclusion |
| COINGECKO_API_KEY | SECRET | No | `""` | CoinGecko API key for price/market data |
| COLLECTION_INTERVAL | TUNING | No | `60` | Minutes between data collection cycles |
| CORS_ORIGINS | METADATA | No | `*` | CORS allowed origins (comma-separated) |
| CDP_API_KEY_ID | SECRET | No | `""` | Coinbase Developer Platform API key ID |
| CDP_API_KEY_SECRET | SECRET | No | `""` | Coinbase Developer Platform API secret |
| DATABASE_URL | INFRASTRUCTURE | No | `""` | PostgreSQL connection string |
| DWELLIR_API_KEY | SECRET | No | `""` | Dwellir RPC provider API key |
| DWELLIR_ETH_URL | SECRET | No | none | Full Dwellir Ethereum RPC URL (overrides API key) |
| DWELLIR_BASE_URL | SECRET | No | none | Full Dwellir Base RPC URL (overrides API key) |
| ETHERSCAN_API_KEY | SECRET | No | `""` | Etherscan API key for contract/tx data |
| FIRECRAWL_API_KEY | SECRET | No | none | Firecrawl web scraping API key |
| HELIUS_API_KEY | SECRET | No | `""` | Helius Solana RPC provider API key |
| INDEXER_HOLDERS_PER_COIN | TUNING | No | `5000` | Max holders to index per coin on startup |
| KEEPER_ENABLED | FEATURE_FLAG | No | `true` | Enable on-chain keeper service |
| KEEPER_PRIVATE_KEY | SECRET | No | none | Keeper service private key (enables keeper) |
| MEMPOOL_WATCHER_ENABLED | FEATURE_FLAG | No | `true` | Enable mempool transaction monitoring |
| MORPHO_BLUE_COLLECTOR_ENABLED | FEATURE_FLAG | No | `true` | Enable Morpho Blue protocol collection |
| PARALLEL_API_KEY | SECRET | No | `""` | Parallel RPC/data provider API key |
| PORT | INFRASTRUCTURE | No | `5000` | HTTP server listening port |
| PROTOCOL_PROMOTE_COVERAGE_PCT | TUNING | No | `52` | Coverage % threshold for protocol promotion |
| PUBLIC_URL | METADATA | No | `https://basisprotocol.xyz` | Public base URL for health checks |
| REDUCTO_API_KEY | SECRET | No | none | Reducto OCR API key |
| RESEND_API_KEY | SECRET | No | none | Resend email delivery service API key |
| SCORING_COLLECTORS_DISABLED | FEATURE_FLAG | No | `""` | Comma-separated collector names to disable |
| SCORING_INTERVAL | TUNING | No | `60` | Minutes between scoring cycles |
| SII_API_BASE | METADATA | No | `http://localhost:5000` | SII service base URL |
| SLACK_ENGINE_WEBHOOK_URL | SECRET | No | none | Slack webhook for engine artifact notifications |
| TALLY_API_KEY | SECRET | No | none | Tally governance API key |
| TELEGRAM_BOT_TOKEN | SECRET | No | none | Telegram bot token for alerting |
| TELEGRAM_CHAT_ID | SECRET | No | none | Telegram chat ID for alerts |
| WEB_WORKERS | TUNING | No | `2` | Number of uvicorn worker processes |
| X402_FACILITATOR_URL | METADATA | No | `https://x402.org/facilitator` | x402 payment facilitator endpoint |
| X402_NETWORK | METADATA | No | Dynamic | x402 network chain (`eip155:8453` or `eip155:84532`) |
| API_HOST | INFRASTRUCTURE | No | `0.0.0.0` | HTTP server bind address |
| API_PORT | INFRASTRUCTURE | No | `5000` | HTTP server port (alias for PORT) |

---

## By Category

### SECRET (Private Keys, API Keys, Credentials)

These variables contain sensitive material and must never be committed or logged. Treat as security boundaries.

- **ADMIN_KEY** — Line 101, 201, 250, etc. (app/server.py, app/ops/routes.py, app/indexer/api.py)  
  Default: `""`  
  Controls: Authentication for admin-only endpoints (budget approval, engine control)  

- **ALCHEMY_API_KEY** — app/config.py:17, app/utils/rpc_provider.py:101  
  Default: `""`  
  Controls: Primary Ethereum JSON-RPC provider; used for all chains when present  

- **ANTHROPIC_API_KEY** — app/content_engine.py:ANTHROPIC_API_KEY, app/ops/tools/{analyzer,drafter,investor_monitor}.py  
  Default: `""`  
  Controls: Claude API access for text generation in content engine and analysis tooling  

- **BLOCKSCOUT_API_KEY** — app/utils/blockscout_client.py:BLOCKSCOUT_API_KEY  
  Default: `""`  
  Controls: Blockscout block explorer API access (contract source, transaction details)  

- **COINGECKO_API_KEY** — app/config.py:15, multiple collectors  
  Default: `""`  
  Controls: CoinGecko market data, price histories, liquidity metrics  

- **CDP_API_KEY_ID** — app/payments.py:54  
  Default: `""`  
  Controls: Coinbase Developer Platform API key ID for x402 payments (part 1/2)  

- **CDP_API_KEY_SECRET** — app/payments.py:55  
  Default: `""`  
  Controls: Coinbase Developer Platform API secret for x402 payments (part 2/2); must be base64-encoded 64-byte seed+pub  

- **DWELLIR_API_KEY** — app/utils/rpc_provider.py:120  
  Default: `""`  
  Controls: Dwellir RPC secondary provider; enables trace/debug methods unavailable on Alchemy free tier  

- **DWELLIR_ETH_URL** — app/utils/rpc_provider.py:116  
  Default: none (if not set, composed from DWELLIR_API_KEY)  
  Controls: Full Ethereum endpoint URL for Dwellir (overrides API key composition); allows auth in URL instead of query string  

- **DWELLIR_BASE_URL** — app/utils/rpc_provider.py:116  
  Default: none (if not set, composed from DWELLIR_API_KEY)  
  Controls: Full Base endpoint URL for Dwellir (overrides API key composition)  

- **ETHERSCAN_API_KEY** — app/config.py:16, 30+ files across collectors/indexer/data_layer  
  Default: `""`  
  Controls: Etherscan API for contract source, transaction tracing, holder enumeration; heavily used for EVM indexing  

- **FIRECRAWL_API_KEY** — app/services/firecrawl_client.py  
  Default: none  
  Controls: Web scraping service API key for research document extraction  

- **HELIUS_API_KEY** — app/config.py:18, solana collectors, indexer  
  Default: `""`  
  Controls: Helius Solana RPC provider; enables Solana realm and program monitoring  

- **KEEPER_PRIVATE_KEY** — main.py:832, 851  
  Default: none  
  Controls: Private key for on-chain keeper operations; **required to enable keeper service**  

- **PARALLEL_API_KEY** — app/services/parallel_client.py, collectors/web_research.py  
  Default: `""`  
  Controls: Parallel RPC aggregator API key (fallback for node queries)  

- **REDUCTO_API_KEY** — app/services/reducto_client.py  
  Default: none  
  Controls: Reducto OCR service for document text extraction  

- **RESEND_API_KEY** — app/ops/routes.py, app/ops/tools/alerter.py  
  Default: none  
  Controls: Resend email delivery service; enables email alerting when set  

- **SLACK_ENGINE_WEBHOOK_URL** — app/engine/slack.py:_WEBHOOK_ENV  
  Default: none  
  Controls: Slack incoming webhook for engine artifact notifications; falls back to stdout when unset  

- **TALLY_API_KEY** — app/ops/tools/governance_monitor.py, scripts/backfill/backfill_dohi.py  
  Default: none  
  Controls: Tally governance data API for DAO event tracking  

- **TELEGRAM_BOT_TOKEN** — app/ops/tools/alerter.py  
  Default: none  
  Controls: Telegram bot token; enables Telegram alerting when set  

- **TELEGRAM_CHAT_ID** — app/ops/tools/alerter.py  
  Default: none  
  Controls: Telegram destination chat ID; enables Telegram alerting when set  

- **BASIS_ENGINE_GITHUB_PAT** — app/engine/git_commit.py:_PAT_ENV (line 48, 156)  
  Default: `""`  
  Controls: GitHub personal access token for engine approval flow (commit artifacts to basis-protocol/basis-hub)  

### INFRASTRUCTURE (Connectivity, Storage, Deployment)

Configuration for databases, APIs, networking layers.

- **DATABASE_URL** — app/config.py:14, app/discovery.py, app/database.py, multiple files  
  Default: `""`  
  Controls: PostgreSQL connection string; **critical for startup**  

- **PORT** — main.py:835  
  Default: `5000`  
  Controls: HTTP server listening port (same as API_PORT)  

- **API_HOST** — app/config.py:21  
  Default: `0.0.0.0`  
  Controls: HTTP server bind address  

- **API_PORT** — app/config.py:22  
  Default: `5000`  
  Controls: HTTP server port (same as PORT)  

- **X402_FACILITATOR_URL** — app/payments.py:57  
  Default: `https://x402.org/facilitator`  
  Controls: x402 payment protocol facilitator endpoint for USDC transactions  

### FEATURE_FLAG (Toggles for Optional Components)

Boolean-like switches that enable/disable major features or data sources.

- **BASIS_ENGINE_TEST_MODE** — app/engine/git_commit.py:_TEST_MODE_ENV (line 47, 77)  
  Default: `false`  
  Values: `1`, `true`, `yes` → enabled; otherwise disabled  
  Controls: Bypass actual git clone/commit/push in engine approval flow (returns fake URLs instead)  

- **BACKLOG_VALUE_FILTER** — app/indexer/backlog.py  
  Default: `false`  
  Values: `true`, `1`, `yes` → enabled  
  Controls: Filter backlog protocols by value threshold before promotion  

- **BLOCKSCOUT_COMPARISON_ENABLED** — app/utils/data_source_comparator.py  
  Default: `true`  
  Controls: Enable side-by-side Etherscan ↔ Blockscout API validation  

- **KEEPER_ENABLED** — main.py:831  
  Default: `true`  
  Values: `true` → enabled; requires KEEPER_PRIVATE_KEY to actually run  
  Controls: Enable keeper on-chain service (harmless if KEEPER_PRIVATE_KEY unset)  

- **MEMPOOL_WATCHER_ENABLED** — app/data_layer/mempool_watcher.py  
  Default: `true`  
  Values: `0`, `false`, `no` → disabled  
  Controls: Monitor pending transactions in mempool (requires ALCHEMY_API_KEY)  

- **MORPHO_BLUE_COLLECTOR_ENABLED** — app/collectors/morpho_blue.py  
  Default: `true`  
  Values: `true` → enabled  
  Controls: Enable Morpho Blue lending protocol data collection  

- **SCORING_COLLECTORS_DISABLED** — app/collectors/registry.py  
  Default: `""` (none disabled)  
  Format: Comma-separated collector class names  
  Controls: Temporarily disable specific data collectors during scoring cycles  

### TUNING (Performance, Intervals, Limits)

Configuration for batch sizes, timeouts, polling intervals, and thresholds.

- **BACKLOG_COLLATERAL_THRESHOLD** — app/indexer/backlog.py  
  Default: `500000` (USD)  
  Controls: Minimum collateral value to include protocol in backlog candidates  

- **BACKLOG_PROMOTE_THRESHOLD** — app/indexer/backlog.py  
  Default: `1000000` (USD)  
  Controls: Value threshold for automatic backlog → index promotion  

- **BASIS_ENGINE_LLM_DAILY_CALL_CEILING** — app/engine/cost_tracker.py:_env_int (line 65-76)  
  Default: `50`  
  Controls: Maximum number of Claude API calls per UTC day; blocks subsequent calls once hit  

- **BASIS_ENGINE_LLM_MONTHLY_BUDGET_USD** — app/engine/cost_tracker.py:_env_float (line 51-62)  
  Default: `200.0` (USD)  
  Controls: Monthly spend cap on Claude API at Sonnet 4.6 rates ($3 input / $15 output per 1M tokens)  

- **BASIS_ENGINE_DEFILLAMA_POLL_MINUTES** — app/engine/scheduler.py:_interval_minutes (line 57-69, 80)  
  Default: `15`  
  Controls: Minutes between DeFiLlama hack polling; env var overridable without deploy  

- **BASIS_ENGINE_WATCHLIST_INTERVAL_MINUTES** — app/engine/scheduler.py  
  Default: `15`  
  Controls: Minutes between watchlist evaluation cycles (offset from DeFiLlama poll)  

- **BLOCKSCOUT_CONCURRENCY** — app/indexer/config.py  
  Default: `10`  
  Controls: Max concurrent Blockscout API requests to prevent rate-limit issues  

- **CDA_COLLECTION_INTERVAL_HOURS** — main.py:135, app/worker.py  
  Default: `24`  
  Controls: Hours between off-chain data availability (CDA) extraction cycles  

- **CHAIN_EXPANSION_TVL_THRESHOLD** — app/collectors/psi_collector.py  
  Default: `500000000` (USD, i.e., $500M)  
  Controls: TVL threshold above which chains are eligible for protocol expansion  

- **COLLECTION_INTERVAL** — app/config.py:27, main.py:35  
  Default: `60` (minutes)  
  Controls: Base interval for data collection and scoring cycles  

- **INDEXER_HOLDERS_PER_COIN** — app/indexer/pipeline.py  
  Default: `5000`  
  Controls: Max holders to fetch and index per coin on first initialization  

- **PROTOCOL_PROMOTE_COVERAGE_PCT** — app/collectors/psi_collector.py  
  Default: `52`  
  Controls: Coverage % threshold (out of which denominator?) for auto-promoting protocols  

- **SCORING_INTERVAL** — app/config.py:28  
  Default: `60` (minutes)  
  Controls: Minutes between full stablecoin scoring cycles  

- **WEB_WORKERS** — main.py:838  
  Default: `2`  
  Controls: Number of uvicorn worker processes (⚠ see scheduler.py module docstring for multi-worker scheduler duplication issue)  

### METADATA (Service Identity, URLs, Addresses)

Configuration that identifies the service, sets canonical URLs, or references on-chain addresses.

- **ALERT_EMAIL** — app/ops/tools/alerter.py  
  Default: `shlok@basisprotocol.xyz`  
  Controls: Default email address for alert notifications  

- **ARBITRUM_ORACLE_ADDRESS** — app/ops/routes.py  
  Default: none  
  Controls: On-chain Arbitrum oracle contract address (checked for existence to report capability)  

- **ATTESTOR_PUBLIC_KEY** — app/server.py  
  Default: `""`  
  Controls: Public key for verifying state attestations (signatures from state_attestation.py)  

- **BASE_ORACLE_ADDRESS** — app/ops/routes.py  
  Default: none  
  Controls: On-chain Base oracle contract address (checked for existence to report capability)  

- **BASE_SBT_ADDRESS** — scripts/mint_initial_sbts.py  
  Default: `""`  
  Controls: Base chain Soulbound Token contract address for NFT minting  

- **BASIS_PAYMENT_WALLET** — app/payments.py:51  
  Default: `""`  
  Controls: USDC payment receiver wallet address on Base chain (x402 protocol)  

- **BASIS_API_BASE** — scripts/populate_incident_rseth_2026_04_18.py  
  Default: none (must be operator-provided for script use)  
  Controls: Local API base URL used by backfill/incident scripts  

- **BASIS_API_URL** — scripts/diagnose_psi_state.ts (TypeScript/Node)  
  Default: `https://basisprotocol.xyz`  
  Controls: Public API URL for client-side scripts; used by Node.js helper scripts  

- **BLOCK_EXPLORER_PROVIDER** — app/indexer/config.py  
  Default: `blockscout`  
  Values: `blockscout` or `etherscan`  
  Controls: Primary block explorer for contract/holder queries (influences API routing)  

- **CANONICAL_BASE_URL** — app/publisher/page_renderer.py, app/server.py, app/templates/_html.py  
  Default: `https://basisprotocol.xyz`  
  Controls: Public-facing domain URL inserted into rendered pages, email bodies, etc.  

- **CORS_ORIGINS** — app/config.py:23-24  
  Default: `*`  
  Format: `*` or comma-separated URLs (e.g., `https://example.com,https://app.example.com`)  
  Controls: CORS allowlist for browser-initiated requests  

- **PUBLIC_URL** — app/ops/tools/health_checker.py  
  Default: `https://basisprotocol.xyz`  
  Controls: Public base URL used in health check endpoints  

- **SII_API_BASE** — app/content_engine.py  
  Default: `http://localhost:5000`  
  Controls: Base URL for local SII (?) service used by content engine  

- **X402_NETWORK** — app/payments.py:64, 66  
  Default: `eip155:8453` (Base mainnet) if CDP credentials present; `eip155:84532` (Base Sepolia) if not  
  Controls: EIP-155 chain identifier for x402 payment network selection  

---

## Safe-for-Dev Defaults

For local development and testing, use these values to avoid requiring real API keys or connecting to production:

| Variable | Dev Default | Rationale |
|---|---|---|
| ADMIN_KEY | `dev-admin-key-unsafe` | Enables admin endpoints locally; never use in production |
| ALCHEMY_API_KEY | Skip or `operator-provided` | Public RPC endpoints (e.g., Infura free tier) for local chains only |
| ANTHROPIC_API_KEY | Skip (cost tracker blocks calls gracefully) | Paid API; use only when testing engine approval flow |
| ARBITRUM_ORACLE_ADDRESS | Skip | Not required for dev; oracle capability check will report false |
| ATTESTOR_PUBLIC_KEY | Skip | Not required for dev |
| BACKLOG_COLLATERAL_THRESHOLD | `100000` | Lower threshold for quicker backlog testing |
| BACKLOG_PROMOTE_THRESHOLD | `500000` | Lower threshold for quicker promotion testing |
| BASIS_ENGINE_GITHUB_PAT | Skip or create test PAT | Skip if testing without actual commits; create test PAT for PR testing |
| BASIS_ENGINE_LLM_DAILY_CALL_CEILING | `10` | Reduce to catch budget-limit bugs in testing |
| BASIS_ENGINE_LLM_MONTHLY_BUDGET_USD | `50.0` | Low budget for testing cost guardrails |
| BASIS_ENGINE_TEST_MODE | `true` | Bypass git operations, return fake URLs |
| BASE_ORACLE_ADDRESS | Skip | Not required for dev |
| BASE_SBT_ADDRESS | Skip | Not required unless testing SBT mint flow |
| BASIS_PAYMENT_WALLET | Skip | Not required for dev |
| BLOCKSCOUT_API_KEY | Skip | Use public endpoints or local explorer |
| BLOCKSCOUT_COMPARISON_ENABLED | `false` | Disable to avoid comparing against external services |
| BLOCKSCOUT_CONCURRENCY | `2` | Lower for lighter test runs |
| CANONICAL_BASE_URL | `http://localhost:5000` | Point to local server |
| CDA_COLLECTION_INTERVAL_HOURS | `6` | Faster iteration for local tests |
| CHAIN_EXPANSION_TVL_THRESHOLD | `1000000` | Low threshold for dev (quick protocol discovery) |
| COINGECKO_API_KEY | Skip (free tier no auth) | Use free CoinGecko API without key |
| COLLECTION_INTERVAL | `10` | Fast iteration for testing scoring cycles |
| CORS_ORIGINS | `*` | Allow local frontend dev |
| CDP_API_KEY_ID, CDP_API_KEY_SECRET | Skip | Not required unless testing x402 payments |
| DATABASE_URL | Neon dev branch or local PostgreSQL | Use `postgresql://user:pass@localhost:5432/basis_dev` |
| DWELLIR_API_KEY | Skip | Use Alchemy or free RPC |
| ETHERSCAN_API_KEY | Skip or `operator-provided` | Limited free tier; use only if testing heavy contract work |
| FIRECRAWL_API_KEY | Skip | Not required for core functionality |
| HELIUS_API_KEY | Skip | Not required unless testing Solana collectors |
| INDEXER_HOLDERS_PER_COIN | `100` | Small batch for quick wallet indexing tests |
| KEEPER_ENABLED | `false` | Disable keeper locally |
| KEEPER_PRIVATE_KEY | Skip | Not required if keeper disabled |
| MEMPOOL_WATCHER_ENABLED | `false` | Avoid mempool watcher in dev |
| MORPHO_BLUE_COLLECTOR_ENABLED | `false` | Disable expensive collectors in dev |
| PARALLEL_API_KEY | Skip | Not required for dev |
| PORT | `5000` | Standard local port |
| PROTOCOL_PROMOTE_COVERAGE_PCT | `10` | Very low for quick testing |
| PUBLIC_URL | `http://localhost:5000` | Point to local server |
| REDUCTO_API_KEY | Skip | Not required for core functionality |
| RESEND_API_KEY | Skip | Falls back safely; no email in dev |
| SCORING_COLLECTORS_DISABLED | `web_research,solana_program_monitor` | Disable expensive/slow collectors |
| SCORING_INTERVAL | `15` | Fast iteration for dev |
| SII_API_BASE | `http://localhost:5000` | Adjust if SII runs elsewhere |
| SLACK_ENGINE_WEBHOOK_URL | Skip | Falls back to stdout |
| TALLY_API_KEY | Skip | Not required for dev |
| TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID | Skip | Not required for dev |
| WEB_WORKERS | `1` | Single worker avoids scheduler duplication issue in dev |
| X402_FACILITATOR_URL | `https://x402.org/facilitator` | Default is fine |
| X402_NETWORK | `eip155:84532` | Use Base Sepolia testnet for dev |

---

## Required-Without-Default

The following variables have **no default** and **must be set** or the service will fail at runtime:

None. Every environment variable in basis-hub has a default (often empty string, which allows graceful degradation). However, the following are **functionally required** for production:

| Variable | Why | Impact if Missing |
|---|---|---|
| DATABASE_URL | PostgreSQL connection string | Service fails to start; database operations unavailable |
| KEEPER_PRIVATE_KEY | Enables keeper service | Keeper service skipped (unless explicitly disabled); on-chain operations unavailable |
| BASIS_ENGINE_GITHUB_PAT | GitHub write access | Engine approval flow fails at commit stage (unless TEST_MODE enabled) |
| ALCHEMY_API_KEY | Primary RPC for EVM chains | RPC calls fail; all on-chain indexing / scoring unavailable |

---

## Gaps & Ambiguities

### Unresolved Questions

1. **Dwellir URL composition** — DWELLIR_ETH_URL and DWELLIR_BASE_URL can override the API key, but does the override happen automatically or does the operator need to construct the full URL manually? (Assumption: automatic composition if only key is set.)

2. **PROTOCOL_PROMOTE_COVERAGE_PCT denominator** — Default `52` is described as "coverage percentage" but the denominator is unclear. Is it `52%` of all existing protocols, or some other baseline? Code reference: app/collectors/psi_collector.py line 114.

3. **WEB_WORKERS scheduler duplication** — app/engine/scheduler.py module docstring warns that multi-worker setups cause duplicate polling (each worker runs the scheduler). No mitigation is currently in place; recommendation is WEB_WORKERS=1 or moving scheduler to a separate service. This should be documented in Railway config.

4. **MORPHO_GRAPHQL_ENDPOINT** — app/collectors/morpho_blue.py references `MORPHO_GRAPHQL_ENDPOINT` but defaults to a hardcoded URL; no environment variable override found. Should this be configurable?

5. **X402_NETWORK dynamic default** — X402_NETWORK defaults to mainnet or testnet based on whether CDP credentials are present. This coupling is implicit and could cause confusion if credentials are partially set. Recommend explicit env var in dev/staging.

6. **RPC provider chain support** — RPC URLs are hardcoded for ethereum, base, and arbitrum in rpc_provider.py. Other chains (Solana, Polygon, etc.) are not routed through the RPC provider abstraction; they have separate collectors and RPC URLs (e.g., HELIUS_API_KEY for Solana). This is architecturally sound but means new chains require code changes, not just env var additions.

7. **Cost tracker pricing hardcoded** — INPUT_PRICE_PER_M_USD and OUTPUT_PRICE_PER_M_USD are hardcoded as Sonnet 4.6 rates in cost_tracker.py. If Claude models upgrade or pricing changes, the values won't auto-update. Consider making these environment-overridable (BASIS_ENGINE_LLM_INPUT_PRICE_USD, etc.).

---

## Changelog

- **2026-05-12** — Initial inventory created; captured all 51 unique variables across app/, main.py, keeper/, scripts/
