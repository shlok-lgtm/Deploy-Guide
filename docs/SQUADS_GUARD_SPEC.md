# Squads Guard — Solana Enforcement Surface Specification

## What is Squads?

Squads (squads.so) is the Solana equivalent of Safe (formerly Gnosis Safe). It's a multisig
and smart account protocol used by Solana teams to manage treasury, program upgrades, and
governance. Drift uses Squads for their multisig operations.

Key differences from Safe:
- Squads is a Solana program (Rust/Anchor), not an EVM contract (Solidity)
- Squads v4 uses "Smart Accounts" — programmable accounts with custom execution logic
- Transaction proposals are on-chain, not off-chain like Safe's Transaction Service
- Squads has a "Sub-Accounts" model for role-based access

## What a Squads Guard would do

Same function as the Safe Guard Module, adapted for Squads:

1. Before a Squads multisig executes a transaction involving stablecoins,
   the Guard checks Basis scores
2. If the stablecoin's SII score is below the configured threshold,
   the Guard blocks or flags the transaction
3. The Guard reads scores from the Basis Solana Oracle (when deployed)
   or via an off-chain API call during transaction simulation

## Architecture options

### Option A: Squads v4 "Time Lock" + Off-Chain Checker
- Squads v4 supports time-delayed execution
- An off-chain bot monitors proposed transactions during the delay
- Bot reads Basis API, flags transactions involving low-SII stablecoins
- Pro: No on-chain program needed. Con: Not enforceable on-chain.

### Option B: Custom Squads Program Extension
- Build an Anchor program that acts as a Squads "sub-account" with custom execution rules
- The program reads from the Basis Solana Oracle before approving transactions
- Pro: On-chain enforcement. Con: Requires Basis Solana Oracle first.

### Option C: Squads Webhook Integration
- Squads has webhook/notification support for transaction proposals
- Integrate Basis scoring into the webhook handler
- Score evaluation happens off-chain, notification sent to multisig members
- Pro: Fastest to build. Con: Advisory only, not blocking.

## Recommended approach for Drift conversation

**Start with Option C (webhook), offer Option B as the roadmap.**

Option C can be built in a day — it's a webhook handler that calls the Basis API
and posts a score summary to the Squads proposal. Drift's multisig members see
"Basis SII: USDC 88.6 (B) — above threshold" or "WARNING: SII below threshold"
alongside every transaction proposal involving stablecoins.

This is the same GTM pattern as the Safe Guard: advisory first, enforcement later.

## Dependencies

- Option C: Basis API (live), Squads webhook docs
- Option B: Basis Solana Oracle (not built), Anchor/Rust toolchain, Squads v4 SDK
- Option A: Basis API (live), Squads v4 time-lock feature

## Build estimate

- Option C (webhook): 1 Claude Code session, ~2-4 hours
- Option A (off-chain checker): 1-2 sessions, ~1 day
- Option B (on-chain guard): requires Solana Oracle first, then 2-3 sessions

## What to tell Drift

"We have a Safe Guard module for EVM treasuries that checks stablecoin risk scores
before transactions execute. For Solana/Squads, we can ship a webhook integration
that scores every transaction proposal your multisig sees — within a week. The
on-chain enforcement version requires our Solana Oracle, which is on our post-seed
roadmap."

## Relevant context

- Safe Guard code: basis-safe repo (spoke)
- Basis Oracle (EVM): basis-oracle repo, 53 Foundry tests
- Squads docs: https://docs.squads.so
- Squads v4 SDK: @sqds/multisig (npm)
