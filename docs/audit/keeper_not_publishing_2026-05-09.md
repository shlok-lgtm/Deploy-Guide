# Keeper Not Publishing On-Chain (2026-05-09)

## Observed

- Keeper cycles: 1,332 total, 45 in last 24h (running normally)
- SII Base publishes: 0
- SII Arbitrum publishes: 0
- State root: No
- keeper/publisher.ts logs: "DRY RUN — would publish updates"

## Root Cause

**`DRY_RUN` environment variable is set to `"true"` on the Railway Scoring-Worker service.**

`keeper/config.ts:72`: `dryRun: process.env["DRY_RUN"] === "true"`

When `dryRun` is true, every publish function in `keeper/publisher.ts` short-circuits:
- `publishUpdates` (line 120): returns null
- `publishPsiScores` (line 257): returns null
- `publishReportHashes` (line 391): returns null
- `publishStateRoot` (line 463): returns null

The keeper computes diffs, logs what it WOULD publish, then discards.

## Fix

**Remove or set `DRY_RUN=false` in Railway environment variables for the Scoring-Worker service.**

Steps:
1. Railway dashboard → Scoring-Worker → Variables
2. Find `DRY_RUN` — either delete it or set to `false`
3. Restart the service

## Pre-flight checks before enabling

Before flipping DRY_RUN off, verify:
1. `KEEPER_PRIVATE_KEY` is set and the corresponding wallet has ETH on both Base and Arbitrum
2. `BASE_ORACLE_ADDRESS` and `ARBITRUM_ORACLE_ADDRESS` point to the correct deployed oracle contracts
3. `BASE_RPC_URL` and `ARBITRUM_RPC_URL` are responsive
4. `MAX_GAS_PRICE_GWEI` (default 1.0) is reasonable for current gas conditions

## Risk

Low. The keeper has been computing correct diffs for 1,332 cycles. Flipping the flag just enables the actual on-chain write. First publish will send all current scores (catch-up), then subsequent cycles will only publish deltas above `SCORE_CHANGE_THRESHOLD` (default 10 basis points).

## Safe to apply now?

Yes, after the pre-flight checks pass. No code change needed.
