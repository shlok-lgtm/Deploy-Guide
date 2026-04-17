# Oracle Option-C Routing — Design

**Status:** design, not yet executed.
**Session:** oracle companion contract design.
**Verdict:** Option C is viable. No new contract needed. Tasks 2 and
Task 3-as-specified (deploy new contract) are moot — replaced with a
keeper integration plan.

---

## 1. What we found

The live oracle at `0x1651d7b2e238a952167e51a1263ffe607584db83` on
**both** Base (chain 8453) and Arbitrum (42161) was deployed from
`31451b2:src/BasisSIIOracle.sol`. Byte-identical bytecode on both
chains: length 19,406 hex chars (9,703 bytes), confirmed by diffing
the broadcast artifacts:

- Base tx:     `0xa983bd6e0326f91b39fb1f3b12017bc3c7f50e48b711838797274f56afe48ac5`
- Arbitrum tx: `0xcbde12434a289923bec207881b35c14cd4b82a9000c39d07d666cb5e7175b433`

The deployed contract already exposes a **generic hash-commit surface**:

```solidity
function publishReportHash(
    bytes32 entityId,
    bytes32 reportHash,
    bytes4  lensId
) external onlyKeeper whenNotPaused;
```

Selector `e8e35f79` verified present in both deployed bytecodes. The
function writes to `reportHashes[entityId]`, `reportLenses[entityId]`,
`reportTimestamps[entityId]`, and emits
`ReportPublished(entityId, reportHash, lensId, timestamp)`.

All three Bucket A commit types can be routed through this function
via a domain-prefix scheme that keeps them segregated from real SII
report hashes, from each other, and from future commit types.

## 2. Domain-prefix routing

### Tagging convention

Every new commit type gets a **4-byte lens tag** (the `lensId`
parameter) and a **domain-prefixed entityId** computed off-chain as:

```
entityId = keccak256(abi.encodePacked(domain_string, ...unique_fields...))
```

The `domain_string` prevents collision with SII/PSI report entity
IDs (which use `keccak256(symbol)` or `keccak256(slug)`) and with
other commit types.

### Track-record commitments

```
lensId    = bytes4("TRCK")   // 0x5452434B
entityId  = keccak256(abi.encodePacked(
                "basis:track_record:v1",
                eventHash,
                eventType             // bytes4: "DIVG" | "RPID" | "COHD" | "SCRC"
            ))
reportHash = eventHash              // sha256 of canonical event payload

// Call:
oracle.publishReportHash(entityId, reportHash, lensId)
```

**What's bound by the commit:** the canonical event-hash payload,
stored off-chain, MUST contain `event_type`, `entity_slug`,
`event_timestamp`, `score_before`, `score_after`, `direction`, and
`state_root_at_event`. That entire JSON is what `eventHash` hashes.
The on-chain commit binds the hash and its block.timestamp. A
reviewer re-derives the event by pulling the payload, hashing it,
and checking against the on-chain `reportHashes[entityId]`.

**Trade-offs vs the "ideal" `publishTrackRecord(eventHash,
stateRootAtEvent, eventType, eventTimestamp)`:**

- The logical `eventTimestamp` is stored ONLY inside the canonical
  payload (bound by `eventHash`), not as a separate on-chain field.
  Acceptable: the hash binds it.
- `stateRootAtEvent` is stored ONLY inside the canonical payload.
  Reviewers can cross-check against `latestStateRoot()` at the commit
  block by querying historical state — minor friction, not a loss.
- `eventType` is bound inside the payload AND encoded into `entityId`
  so queries like "all DIVG events" can filter by `entityId` prefix
  after reconstituting — slightly worse ergonomics, same integrity.

### Dispute commitments

```
lensId    = transitionKind         // bytes4: "SUBM" | "CTRE" | "RSLV"
entityId  = keccak256(abi.encodePacked(
                "basis:dispute:v1",
                disputeId,         // bytes32 — keccak256("dispute:{db_id}")
                transitionKind
            ))
reportHash = transitionHash        // sha256 of canonical transition payload

// Call:
oracle.publishReportHash(entityId, reportHash, lensId)
```

**Feature preserved:** the existing claim in
`docs/methodology_disputes.md` that commits are "write-once per
`(disputeId, transitionKind)`" is now **keeper-enforced**, not
contract-enforced. See Section 4 below.

### Methodology-hash commitments

```
lensId    = bytes4("MTHD")         // 0x4D544844
entityId  = keccak256(abi.encodePacked(
                "basis:methodology:v1",
                bytes(methodologyId_string)   // e.g. "track_record_outcomes_v1"
            ))
reportHash = ruleHash              // sha256 of canonical methodology doc

// Call:
oracle.publishReportHash(entityId, reportHash, lensId)
```

The methodology string itself lives in the committed document (which
is the thing being hashed). On-chain we only store the domain-prefixed
hash of the ID.

### State-root commits

Continue to use the existing `publishStateRoot(bytes32)` unchanged.
No Option C adaptation needed — the function is already generic.

## 3. Collision analysis

Collision surface: two of our new `entityId`s, or one of ours and one
real SII report entity, producing the same 32-byte value.

- Real SII entityIds are `keccak256(symbol)` where `symbol` is a stablecoin
  ticker (e.g. `"USDC"`, ~4–6 bytes). Domain prefixes `"basis:track_record:v1"`
  etc. are always longer. Preimage length differs → distinct preimage
  space → collision only via a 2^-256 keccak collision.
- Across our three new types: the domain-string literal differs
  (`"basis:track_record:v1"` vs `"basis:dispute:v1"` vs
  `"basis:methodology:v1"`). Preimages cannot collide.

Safe.

## 4. Limitations vs. the "ideal" design

| Property | Ideal (new functions) | Option C | Impact |
|---|---|---|---|
| On-chain write-once per key | yes | **no** | **off-chain keeper must check `reportHashes[entityId]` before each call** |
| Separate logical timestamp | yes | no | logical time bound in payload; block.timestamp only |
| Separate eventType field | yes | folded into entityId | filter by reconstructing entityId |
| Separate events per domain | yes | single `ReportPublished` event filtered by lensId | indexers must filter |
| Methodology ID as readable string | yes | hashed into entityId | document itself is the canonical source of the string |

**The only loss with security impact is write-once.** Mitigation:
the keeper MUST call `getReportHash(entityId)` first and refuse to
re-publish if a commitment already exists. Keeper logic in Section 5.

## 5. Keeper integration (pseudo-code)

```typescript
// keeper/publisher.ts — add to the existing publisher

const BASIS_TR_DOMAIN  = "basis:track_record:v1";
const BASIS_DSP_DOMAIN = "basis:dispute:v1";
const BASIS_MTH_DOMAIN = "basis:methodology:v1";

const LENS_TRCK = "0x5452434b";   // bytes4("TRCK")
const LENS_MTHD = "0x4d544844";   // bytes4("MTHD")

function trackRecordEntityId(eventHash: string, eventType: string): string {
  return ethers.keccak256(
    ethers.solidityPacked(
      ["string", "bytes32", "bytes4"],
      [BASIS_TR_DOMAIN, eventHash, eventType]
    )
  );
}

async function publishTrackRecordCompanion(
  oracle: Contract,
  eventHash: string,
  eventType: string  // 'DIVG' | 'RPID' | 'COHD' | 'SCRC', bytes4
): Promise<string> {
  const entityId = trackRecordEntityId(eventHash, eventType);

  // off-chain write-once guard
  const [existingHash, , existingTs] = await oracle.getReportHash(entityId);
  if (existingHash !== ethers.ZeroHash) {
    throw new Error(
      `track_record ${eventHash} already committed at ts=${existingTs}`
    );
  }

  const tx = await oracle.publishReportHash(entityId, eventHash, LENS_TRCK);
  return tx.hash;
}

// Dispute and methodology commits follow the same pattern.
```

On the DB side, `track_record_commitments.on_chain_tx_hash` gets the
returned tx hash. The existing schema does not need a new column for
`on_chain_entity_id` — but it's recommended so reviewers can look up
commits directly via `getReportHash(entityId)`.

## 6. Off-chain indexer changes

Existing `ReportPublished` indexer must split events by `lensId`:

```
ReportPublished(entityId, reportHash, lensId, timestamp)
  if lensId == "TRCK"                 → track_record_commitments
  if lensId in ("SUBM","CTRE","RSLV") → dispute_commitments
  if lensId == "MTHD"                 → methodology_hashes
  else                                → sii/psi report hashes (existing)
```

`app/ops/tools/oracle_monitor.py` must learn about the new lens tags.

## 7. Docs that need updating when this lands

- `docs/methodology_disputes.md`: change "write-once per `(disputeId,
  transitionKind)` on-chain" to "write-once enforced by keeper;
  on-chain anchoring is single-slot-per-entityId where entityId
  encodes `(disputeId, transitionKind)`." Honest.
- `docs/basis_protocol_v9_3_constitution_amendment.md` Articles I/IV:
  replace mentions of `publishTrackRecord` and `publishDisputeHash`
  with the Option C routing description. Ratification gate 1 rewrites
  to "oracle exposes generic commit surface and keeper integrates
  the domain-prefix scheme; no new contract required."
- `docs/methodology_track_record_outcomes.md`: section "Tuning
  resistance" already notes the methodology-hash commitment is
  pending; it can be fulfilled via Option C.

## 8. Verification plan (to be executed in a later session, not now)

Once the keeper is upgraded (Session N+2 per the original plan):

1. Pick one historical qualifying track-record event. Compute its
   canonical hash off-chain.
2. Call `publishReportHash(entityId, eventHash, "TRCK")` on Base
   mainnet. Record tx.
3. Call `getReportHash(entityId)` from a fresh RPC connection. Confirm
   it returns `(eventHash, "TRCK", blocktime)`.
4. Repeat for one dispute submission (lensId `"SUBM"`) and one
   methodology hash (`"MTHD"`).
5. If all three round-trips pass, Option C integration is live.

No contract deploy. No Basescan verification. No new address in any
integration guide. This is a keeper-side change only.

## 9. What Option C does NOT give us (be honest)

- A typed event per commit domain. Consumers that preferred to filter
  by event signature rather than by `lensId` must adapt.
- On-chain enforcement of write-once. A misbehaving or compromised
  keeper could overwrite prior commits on the same `entityId`. If the
  threat model assumes the keeper key is compromised, Option C alone
  is insufficient — but then neither is Option A (any keeper can
  write any bytes32). Real mitigation is keeper-key hygiene + an
  audit log diffing on-chain vs off-chain state, which is the same
  protection Option A would rely on.
- Readable on-chain `methodologyId` strings. The string lives in the
  committed document only. Acceptable — the document IS the
  canonical source of its own ID.

## 10. Net assessment

Option C is a strict win over Option A (new contract, new address,
integration-guide churn) under the current deployed contract's
design:

- Zero contract deploy cost.
- Zero consumer break (nothing changed about the oracle interface).
- Zero new address for integrators to track.
- Keeper-side change only; reversible by rolling back the keeper.
- The one meaningful loss (on-chain write-once) is recoverable via
  keeper-side enforcement + audit.

Proceed.
