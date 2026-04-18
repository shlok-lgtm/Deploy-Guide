# Oracle Option C — Keeper Integration Runbook

**Status:** runbook, not yet executed.
**Depends on:** `docs/oracle_option_c_routing.md` (authoritative for
lens scheme, entityId construction, collision analysis, read-path
semantics). This runbook does NOT restate those; it cites them.
**Scope:** everything a human operator needs to do in Replit to
route Bucket A commits (track record, dispute, methodology) through
the existing deployed oracle at
`0x1651d7b2e238a952167e51a1263ffe607584db83` on Base + Arbitrum.
**No new contract. No address change.**

---

## 0. Pre-conditions

Before starting any step, confirm:

1. Branch `claude/track-record-disputes-5RhQP` is checked out with
   commit `a617fb6` or later present (the Q&A'd routing doc).
2. Migrations 074–077 are applied to the production DB. Verify with
   `SELECT MAX(version) FROM schema_migrations;` → ≥ 77.
3. The detectors in `app/track_record.py` are running in production
   (i.e. `track_record_commitments` rows exist with
   `committed_on_chain = false`). Verify with
   `SELECT COUNT(*) FROM track_record_commitments WHERE committed_on_chain = false;`.
4. Keeper currently running on Replit uses commit `bef82a0` or later.

If any of (1)–(4) is false, stop; fix that first.

---

## 1. Replit Secrets

All values live in the Replit Secrets pane for the keeper Repl. None
go in `.env` files, none are committed. Required:

| Secret | Purpose | Notes |
|---|---|---|
| `KEEPER_PRIVATE_KEY` | signs all oracle transactions | **must** already be authorized via `setKeeper` on both chains. The deployed oracle has a single `keeper` slot; do not rotate as part of this runbook. |
| `BASE_RPC_URL` | Base mainnet RPC | Alchemy or Infura endpoint; free-tier works for the commit volume in this runbook. |
| `ARBITRUM_RPC_URL` | Arbitrum One mainnet RPC | same. |
| `BASE_ORACLE_ADDRESS` | `0x1651d7b2e238a952167e51a1263ffe607584db83` | hard-coded fallback in `keeper/config.ts` — setting the secret is optional but preferred so it appears in logs. |
| `ARBITRUM_ORACLE_ADDRESS` | same address on 42161 | same. |
| `ADMIN_KEY` | production API admin key | needed to fetch pending commits from the hub and to mark them committed. |
| `API_URL` | `https://basisprotocol.xyz` | default in code; override only if testing against a staging hub. |
| `DRY_RUN` | `true` for offline dry-run; `false` for mainnet | see Section 4. |
| `MAX_GAS_PRICE_GWEI` | upper bound before the keeper refuses to submit | 5 on Base, 1 on Arbitrum are sane defaults. |

Funding check: the keeper EOA must hold ≥ 0.01 ETH on Base and
≥ 0.005 ETH on Arbitrum before Section 5. Each `publishReportHash`
call costs roughly 50k gas. Budget 100 calls per chain across the
first-week backfill.

---

## 2. Keeper code changes

Three files change. All changes are confined to `keeper/`; no
Solidity, no Python, no migration. See `docs/oracle_option_c_routing.md`
§ 11 Q2 and Q3 for the authoritative entityId and lens definitions;
do not inline duplicates here.

### 2.1 `keeper/publisher.ts` — trim the ABI

The current `ORACLE_ABI` (lines 7–17) declares `publishTrackRecord`
and `publishDisputeHash`. Neither exists on the deployed bytecode.
Calls against them revert with "function selector was not recognized."

**Change:** drop those two ABI entries. The remaining ABI is:

```typescript
const ORACLE_ABI = [
  "function batchUpdateScores(address[] calldata tokens, uint16[] calldata scores, bytes2[] calldata grades, uint48[] calldata timestamps, uint16[] calldata versions) external",
  "function batchUpdatePsiScores(string[] calldata slugs, uint16[] calldata scores, bytes2[] calldata grades, uint48[] calldata timestamps, uint16[] calldata versions) external",
  "function isStale(address token, uint256 maxAge) external view returns (bool)",
  "function publishReportHash(bytes32 entityId, bytes32 reportHash, bytes4 lensId) external",
  "function publishStateRoot(bytes32 stateRoot) external",
  "function getReportHash(bytes32 entityId) external view returns (bytes32, bytes4, uint48)",
  "function reportTimestamps(bytes32 entityId) external view returns (uint48)",
  "function stateRootTimestamp() external view returns (uint48)",
];
```

`getReportHash` is added because Section 2.2 needs it for the
off-chain write-once guard.

### 2.2 `keeper/publisher.ts` — three new entry helpers

Add three helpers alongside the existing `publishReportHashes`
(line 286). Each computes `entityId` per `docs/oracle_option_c_routing.md`
§ 11 Q2, uses the lens from § 11 Q3, checks the write-once guard,
and calls the same underlying `publishReportHash`.

Sketch (not full code — final implementation follows this shape):

```typescript
// Track-record: lens 0x00000100
export async function publishTrackRecordCompanion(
  u: TrackRecordUpdate, provider, wallet, oracleAddress, chainKey, config
): Promise<string | null> {
  const entityId = computeTrackRecordEntityId(u);  // per routing doc § 11 Q2
  const existing = await oracle.getReportHash(entityId);
  if (existing[0] !== ethers.ZeroHash && existing[0] !== u.eventHash) {
    logger.warn("track_record: entityId collision with different hash — refusing");
    return null;
  }
  if (existing[0] === u.eventHash) {
    return existing[0];   // idempotent no-op
  }
  return await publishReportHash(entityId, u.eventHash, "0x00000100", ...);
}

// Dispute: lens 0x00000200
export async function publishDisputeCompanion(...)    // lens 0x00000200
// Methodology: lens 0x00000300
export async function publishMethodologyCompanion(...) // lens 0x00000300
```

The legacy `publishTrackRecords` (line 386) and `publishDisputeCommitments`
(line 437) helpers are **deleted** — they call non-existent functions.

### 2.3 `keeper/index.ts` — rewire steps 8 and 9

Current steps 8 (line 308) and 9 (line 342) call the deleted
helpers. Replace with calls to the new `*Companion` helpers from
2.2. Step 6 (report hashes, line 280) is unchanged — it continues
to use lens `0x00000000` for SII/PSI reports.

Methodology commits are a new step 10: on keeper boot, read
`methodology_hashes` table from the hub (new admin endpoint,
see 2.4), publish any rows where `on_chain_tx_hash IS NULL`.

### 2.4 Hub-side admin endpoints

These exist or need to be added for the keeper to fetch pending
work:

- `GET /api/admin/track-record/pending` — exists.
- `POST /api/admin/track-record/{id}/mark-committed` — exists.
- `GET /api/admin/disputes/pending-commits` — exists.
- `POST /api/admin/disputes/commit/{id}/published` — exists.
- `GET /api/admin/methodology/pending` — **needs adding**. Returns
  rows from `methodology_hashes` where `on_chain_tx_hash IS NULL`.
- `POST /api/admin/methodology/{id}/mark-committed` — **needs adding**.

The methodology admin endpoints are ~30 lines each, mirror the
track-record pattern, and live in `app/ops/routes.py`.

---

## 3. Hashing helpers — a single file, single source of truth

Add `keeper/optionC_keys.ts` that exports pure functions for:

```typescript
computeTrackRecordEntityId(u: TrackRecordInput): string   // bytes32 hex
computeDisputeEntityId(u: DisputeInput): string            // bytes32 hex
computeMethodologyEntityId(methodologyId: string): string  // bytes32 hex
```

The preimage for each is defined in `docs/oracle_option_c_routing.md`
§ 11 Q2. Do not restate the scheme inline in this runbook or in
code comments beyond a single-line pointer to the doc. If the
scheme changes, only the doc and this file are updated together.

This file is covered by a unit test (`keeper/optionC_keys.test.ts`)
with at least one golden vector per function — values committed to
the repo before any mainnet call is made. Tests run on every CI
keeper build.

---

## 4. Offline dry run (no Sepolia oracle exists)

There is no Option C deployment on any testnet. The deployed oracle
lives only on Base 8453 and Arbitrum 42161. Dry-run is therefore
offline-only:

1. Set Replit Secret `DRY_RUN=true`. The keeper's existing
   `config.dryRun` path (publisher.ts:103, 202, 294) logs what
   would be submitted without signing or broadcasting.
2. Run `npm test` under `keeper/` — the hashing golden vectors
   from Section 3 must pass.
3. Start the keeper in Replit with `DRY_RUN=true`. Let one cycle
   complete. Inspect logs for:
   - one "DRY RUN — would publish report hashes" line per pending
     SII/PSI entity,
   - one "DRY RUN — would publish track record" line per pending
     row in `track_record_commitments`,
   - one "DRY RUN — would publish dispute commitment" line per
     pending row in `dispute_commitments`,
   - zero calls to the deleted `publishTrackRecord` /
     `publishDisputeHash` ABIs,
   - zero calls with `lensId = 0x00000000` on track-record or
     dispute queues.
4. Only after (1)–(3) are clean, proceed to Section 5.

---

## 5. Mainnet sequence — two anchoring commits first

The first two on-chain calls after Section 4 passes are **not**
backfill. They are anchoring commits chosen so the system's first
visible act has defensible meaning.

### 5.1 Commit 1 — lens registry (methodology hash)

**Rationale.** The first call makes the rules for every later
commit self-documenting. After this tx, any reviewer can fetch the
methodology hash at the registry's `entityId` and confirm on chain
that the lens scheme in `docs/oracle_option_c_routing.md` § 11 Q3
was the scheme in force from block N onward.

**Inputs (all per routing doc § 11 Q2/Q3, not duplicated here):**

- `methodologyId = "lens_registry_v1"`
- `entityId`: computed via `computeMethodologyEntityId("lens_registry_v1")`
- `reportHash`: `sha256(contents of docs/oracle_option_c_routing.md § 11 Q3 table, canonical form)`
- `lensId`: `0x00000300`

**Procedure.**

1. Keeper operator pauses the cycle loop (set `WORKER_ENABLED=false`
   on the keeper Repl temporarily, or run step manually via a
   `scripts/commit_lens_registry.ts` one-shot).
2. With `DRY_RUN=true`, run the one-shot against both chains.
   Inspect logs for the exact `entityId`, `reportHash`, and
   `lensId=0x00000300`. Sanity-check the reportHash matches what
   `sha256` of the canonical Q3 table produces locally.
3. Flip `DRY_RUN=false`. Run the one-shot against Base. Record
   `(blockNumber, txHash, entityId, reportHash)`. Confirm:
   `cast call` or `ethers` readback of
   `getReportHash(entityId)` returns
   `(reportHash, 0x00000300, blockTimestamp)`.
4. If (3) is clean, repeat against Arbitrum. Same readback check.
5. Append both readback proofs to `docs/oracle_option_c_routing.md`
   as an "Anchored" note under § 11 Q3 with tx hashes. This is the
   only time the runbook edits the routing doc.

### 5.2 Commit 2 — SVB/USDC depeg (March 2023) as first track-record event

**Rationale.** The V6.3.5 crisis-replay corpus already uses the
March 10–13, 2023 USDC depeg as a validation case. Committing it
as the first track-record anchor ties the Option C rollout to a
publicly verifiable historical event with unambiguous ground
truth (USDC traded to ~$0.87 on March 11, recovered by March 13
after SVB receivership announcement). The first real track-record
commit is therefore self-evidencing rather than a synthetic test.

**Inputs.**

- Event backfill row: insert into `track_record_commitments` via a
  one-shot script that reconstructs the canonical payload using
  the existing `app/track_record.py::canonical_event_hash(...)`
  helper. Do NOT hand-construct the hash — invoke the library so
  the hash is identical to what the production detector would
  emit.
- `event_type = "divergence"` (peg divergence; maps to bytes4 tag
  `"DIVG"` in `keeper/index.ts:388`).
- `entity_slug = "usdc"`.
- `event_timestamp`: 2023-03-11T05:25:00Z (the timestamp of the
  first-hour depeg below $0.95 per the public CoinGecko tick data
  already cached in production; confirm against
  `historical_prices` table before finalizing).
- `event_payload`: minimum required fields per
  `docs/methodology_track_record_outcomes.md`:
  `{ score_before, score_after, direction: "down",
     state_root_at_event, magnitude, observed_price_low,
     recovery_timestamp, source: "SVB receivership — historical backfill" }`.

**Procedure.**

1. Dry-run the backfill script locally first. Verify the inserted
   row's `event_hash` matches an independently-computed
   `sha256(canonical(payload))`.
2. With the row pending, run the keeper's track-record step once
   with `DRY_RUN=true`. Confirm the log shows
   `lensId=0x00000100` and the correct `entityId`.
3. Flip `DRY_RUN=false`. Publish on Base first. Readback via
   `getReportHash(entityId)` must return
   `(event_hash, 0x00000100, blockTimestamp)`.
4. Publish on Arbitrum. Same readback.
5. Record both tx hashes in the hub row's `on_chain_tx_hash`.

### 5.3 Commit 3+ — 30-day backfill loop

With 5.1 and 5.2 anchored, re-enable the keeper cycle. Its
track-record step will iterate every row in
`track_record_commitments` where `committed_on_chain = false`
and publish in chronological order. Rate-limit one tx per
10 seconds per chain to stay below public-RPC abuse thresholds.

Enumerate pending rows with:
```sql
SELECT id, event_type, entity_slug, event_timestamp, event_hash
  FROM track_record_commitments
 WHERE committed_on_chain = false
   AND event_timestamp > NOW() - INTERVAL '30 days'
 ORDER BY event_timestamp ASC;
```

Expected count at go-live: depends on detector output. If the
count exceeds 200, pause and review — the first full loop should
be a tractable, auditable batch. Large counts probably indicate
detector tuning noise, not legitimate events.

---

## 6. Rollback

Every step in Section 5 is reversible at the keeper level. No
deployed contract state can be rolled back (on-chain writes are
permanent), but keeper behavior can be:

1. **Freeze publishing.** Set `WORKER_ENABLED=false` on the keeper
   Repl, or flip `DRY_RUN=true`. Either halts all new submissions
   within one cycle.
2. **Revert keeper code.** Roll back to commit `bef82a0` (pre-Option C
   keeper). The keeper then falls back to only emitting SII/PSI
   `batchUpdateScores` and `publishReportHash` with lens
   `0x00000000` — the behavior the deployed contract has been
   seeing all along. Track-record and dispute steps become no-ops
   because the old code's ABI entries for
   `publishTrackRecord` / `publishDisputeHash` would still be
   dead in the deployed bytecode (the fact that forced Option C
   in the first place).
3. **Mark hub rows un-committed.** If a batch was submitted but is
   later judged faulty, the on-chain record stays, but the hub
   can re-mark `committed_on_chain = false` and re-publish with a
   corrected `event_hash`. Because `publishReportHash` overwrites
   (routing doc § 11 Q2), the on-chain slot is updated on the next
   cycle — at the cost of two commit records in the event log for
   that `entityId`. Document publicly if this happens.

What rollback does **not** undo:

- The lens-registry commit (5.1). The methodology hash is now
  anchored; any later scheme change must be published as a new
  methodology version (`lens_registry_v2`) with its own `entityId`.
- The SVB/USDC anchor (5.2). Same reasoning; it is meant to be
  permanent.

These are design features, not bugs. Both commits are chosen in
Section 5 precisely because their permanence is desirable.

---

## 7. Post-flight verification

Once 5.1, 5.2, and at least five 5.3 commits are on chain, run
these checks and record results in
`docs/bucket_a_post_flight_report.md` (to be created by that
session):

1. **Per-key readback.** For every tx in 5.1–5.3, call
   `getReportHash(entityId)` from a fresh RPC connection.
   Returned `(hash, lens, timestamp)` must match expected.
2. **Cross-chain parity.** For 5.1 and 5.2, confirm both Base and
   Arbitrum return byte-identical `(hash, lens)` pairs. Timestamps
   will differ (different block times per chain).
3. **Indexer dispatch.** After adding the lens-tag switch to
   `app/ops/tools/oracle_monitor.py` (routing doc § 6), confirm
   one ReportPublished event per commit is routed to the correct
   hub table (`track_record_commitments`, `dispute_commitments`,
   or `methodology_hashes`).
4. **Ratification gate 1.** With 1–3 green, `docs/basis_protocol_v9_3_constitution_amendment.md`
   ratification gate 1 can be marked "closed — Option C integration
   live" with the tx hashes as evidence.

Steps 1–3 are non-destructive and can be run from any reviewer's
machine with only `BASE_RPC_URL` and `ARBITRUM_RPC_URL` — they do
not require the keeper key.

---

## 8. What this runbook does NOT cover

- Bucket B commits (reserved range `0x00000400+` in routing doc
  § 11 Q3). A separate runbook when Bucket B ships.
- Oracle deprecation or migration to a new address. Out of scope;
  Option C's entire point is to avoid this.
- Contract upgrade path. The deployed oracle is non-upgradeable;
  see `docs/bucket_a_go_live_preflight_report.md` § 2 for why.
- Key rotation for `KEEPER_PRIVATE_KEY`. If needed, follow the
  existing `setKeeper` procedure; it is independent of Option C.

---

## 9. Honest limitations

Repeating from routing doc § 9 because operators should re-read
them before Section 5:

- No on-chain write-once. Keeper-side guard only (Section 2.2).
- No typed per-domain event. All commits emit
  `ReportPublished(entityId indexed, hash, lens, ts)`; `lens` is
  not indexed. Indexer must scan + filter.
- `eventTimestamp` for track records is stored only inside the
  payload (bound by `event_hash`), not as a separate on-chain
  field.
- On-chain enumeration is not supported at scale. Postgres is the
  source of truth for "show me all X"; on-chain is per-key
  integrity check only.

These are accepted costs of Option C versus Option A (new
contract, new address, integration-guide churn). Net assessment
in routing doc § 10 stands.
