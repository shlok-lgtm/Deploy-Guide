# Solana Oracle — Post-Raise Specification

## Current State
- EVM oracle: Solidity/Foundry, 53 tests, deploy-ready for Base/Arbitrum
- Solana: no oracle exists

## What's Needed
- Anchor/Rust program storing SII + PSI scores on Solana
- Keeper service (TypeScript or Python) pushing scores from hub API
- Verification: on-chain score must match hub API score at publish time
- Deployment: Solana mainnet (program deploy + rent)

## Architecture Decision
- Scores are computed off-chain (hub API), published on-chain (oracle)
- Same pattern as EVM oracle — keeper reads API, writes to contract
- Solana-specific: uses Anchor framework, PDAs for score storage

## Dependencies
- Anchor CLI + Rust toolchain (not in current Replit environment)
- Funded Solana wallet for deployment + rent
- Separate repo: basis-oracle-solana (spoke)

## Timeline
- Post-seed, Month 4-6 range
- Prerequisite: Solana protocols actively consuming scores via API first
