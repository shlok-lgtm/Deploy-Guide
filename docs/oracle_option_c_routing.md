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

---

## 11. Design-review Q&A (added after initial draft)

The five questions below were raised as a design review of Sections 1–10
above. They are answered here before any keeper work begins. Where an
answer conflicts with earlier text, this section is authoritative and
the earlier text is superseded.

### Q1 — Event signature and scale

**Question:** What exactly does `ReportPublished` look like on chain,
and what are the consequences of squeezing every new commit type
through it?

**Answer.** From `src/interfaces/IBasisSIIOracle.sol:110`:

```solidity
event ReportPublished(
    bytes32 indexed entityId,
    bytes32         reportHash,
    bytes4          lensId,
    uint48          timestamp
);
```

Only `entityId` is indexed (topic1). `reportHash`, `lensId`, and
`timestamp` live in the non-indexed `data` field. Consequences:

1. You cannot filter by `lensId` using `eth_getLogs` topic filters.
   A subscriber that wants "all track-record commits" must fetch
   every `ReportPublished` log in the range and decode `data` in
   userland, then discard the ones whose `lensId` ≠ the lens of
   interest. O(N) over the history of the log, not O(matches).
2. At current volume (tens of report commits per day across SII +
   PSI) this is fine. At 10k+ cumulative events it becomes costly
   from a public-RPC standpoint. The project already mirrors on-chain
   events to Postgres via the off-chain indexer, so the authoritative
   query path is Postgres, not RPC — see Q5.
3. No `TrackRecordPublished` / `DisputeCommitmentPublished` event
   fires. Those typed events exist in the interface source
   (`IBasisSIIOracle.sol:113-128`) but the deployed bytecode does
   NOT emit them, because the deployed contract predates those
   function additions. Consumers that want "track record vs report vs
   dispute" segregation must do it in the indexer.

### Q2 — Overwrite vs append, and the entityId scheme

**Question:** `publishReportHash` overwrites any prior value at the
same key. What does the final entityId scheme look like, and is it
collision-free?

**Answer.** Storage shape is confirmed overwrite, not append
(`src/BasisSIIOracle.sol:243-261`):

```solidity
mapping(bytes32 => bytes32) public reportHashes;
mapping(bytes32 => uint48)  public reportTimestamps;
mapping(bytes32 => bytes4)  public reportLenses;

function publishReportHash(bytes32 entityId, bytes32 reportHash, bytes4 lensId)
    external onlyKeeper whenNotPaused {
    reportHashes[entityId] = reportHash;          // unconditional write
    reportTimestamps[entityId] = uint48(block.timestamp);
    reportLenses[entityId] = lensId;
    emit ReportPublished(entityId, reportHash, lensId, uint48(block.timestamp));
}
```

No `require(reportHashes[entityId] == 0)` guard. Re-publishing the same
entityId silently replaces prior content. Consequences for the scheme:

1. `entityId` MUST encode enough of the commit's natural key that two
   legitimate commits never collide. For track-record events, that
   means the key material must uniquely identify the event. For
   disputes, it must uniquely identify the `(disputeId, transition)`
   pair. For methodology, it must uniquely identify the versioned
   methodology ID.
2. Authoritative entityId schemes (this supersedes Section 2):

   ```
   // Track-record event
   entityId = keccak256(abi.encodePacked(
       "basis:track_record:v1",
       eventType,              // bytes4
       entity_slug_bytes,      // variable-length ASCII bytes
       uint64(eventTimestamp)  // Unix seconds
   ))
   reportHash = sha256(canonical_event_payload)

   // Dispute transition
   entityId = keccak256(abi.encodePacked(
       "basis:dispute:v1",
       disputeId,              // bytes32 = keccak256("dispute:{db_id}")
       transitionKind          // bytes4: "SUBM" | "CTRE" | "RSLV"
   ))
   reportHash = sha256(canonical_transition_payload)

   // Methodology document
   entityId = keccak256(abi.encodePacked(
       "basis:methodology:v1",
       methodologyId_bytes     // e.g. "track_record_outcomes_v1"
   ))
   reportHash = sha256(canonical_methodology_doc)
   ```

3. Collision analysis. The three domain strings are literal-distinct
   and shorter than any real SII symbol preimage is longer than
   (real SII `entityId = keccak256(symbol)`, with `symbol` ≤ ~8 bytes
   of ASCII). Domain-string length alone guarantees preimage
   separation across types. Within a type, the unique-fields tuple
   is the natural key: if two legitimate commits share one, they are
   by definition the same commit.
4. Write-once is therefore enforced by two layers:
   - **Natural-key uniqueness** in `entityId` (collisions mean the
     same event, so overwriting with the same `reportHash` is a
     no-op from the reviewer's perspective).
   - **Keeper guard** that calls `getReportHash(entityId)` and
     refuses to re-publish with a different `reportHash`. Code in
     Section 5.
   On-chain enforcement is still absent; see Section 9.

### Q3 — Lens byte scheme and the registry

**Question:** Are the lens tags stable? Who picks the bytes? How is
the mapping from lensId → meaning discoverable?

**Answer.** The project has no canonical on-chain lens registry today.
`lensId` is a freeform 4-byte discriminator; current live usage is
only `0x00000000` (keeper/index.ts:472). The RPI "lens" concept
(`migrations/048_lens_configs.sql`, `app/lenses/*.json`) is a
separate off-chain string-keyed system and does NOT share bytes with
`ReportPublished.lensId`. Adopt this numeric range scheme (supersedes
the ASCII tags in Section 2):

| Range | Meaning |
|---|---|
| `0x00000000` | default / unspecified (current SII/PSI report commits) |
| `0x00000001 – 0x000000FF` | reserved for future core report types |
| `0x00000100` | track-record event commit (Bucket A1) |
| `0x00000101 – 0x000001FF` | reserved for future track-record subtypes |
| `0x00000200` | dispute transition commit (Bucket A4) |
| `0x00000201 – 0x000002FF` | reserved for future dispute subtypes |
| `0x00000300` | methodology document commit (Bucket A — misc) |
| `0x00000301 – 0x000003FF` | reserved for future methodology subtypes |
| `0x00000400 – 0x0000FFFF` | reserved for Bucket B |

The numeric scheme is deliberate: it sorts, it is unambiguously
machine-readable, it avoids the trap of ASCII tags that look like
one meaningful value but are actually a different byte order, and it
leaves headroom within each bucket for subtyping without a new
top-level range.

**`transitionKind` for disputes no longer travels as `lensId`.** It
was ambiguous to overload `lensId` for two purposes. `lensId` is
strictly the type discriminator; the transition kind is encoded only
inside `entityId` (per Q2) and inside the canonical payload. Dispute
`lensId` is uniformly `0x00000200`.

**Registry commit is the first act.** Immediately after keeper
integration ships, before any other new-domain commit is published,
the lens registry itself SHALL be committed as a methodology hash:

```
lensId       = 0x00000300  (methodology)
entityId     = keccak256(abi.encodePacked(
                   "basis:methodology:v1",
                   bytes("lens_registry_v1")
               ))
reportHash   = sha256(docs/oracle_option_c_routing.md § 11 Q3 table)
```

That way the registry is itself anchored and reviewers can verify
any later lens interpretation against a hash that was first in the
chain.

### Q4 — Existing consumer impact

**Question:** What breaks when the keeper starts writing
`0x00000100 / 0x00000200 / 0x00000300` to `lensId`?

**Answer.** Survey across the codebase:

- **Keeper (`keeper/index.ts:472`, `keeper/publisher.ts:283`):** the
  only consumer today actively setting `lensId`. It emits
  `0x00000000` for general report hashes. The new values don't
  conflict. Change scope: publisher.ts gains three new entry
  helpers, each hardcoding its lens; index.ts routes three new
  pending-work queues through them.
- **Python backend (`app/**/*.py`):** no references to the string
  literal `ReportPublished`, `publishReportHash`, or `lensId` in
  Python. (`grep` confirmed zero matches.) Nothing on the API side
  filters by lens.
- **Off-chain indexer:** `app/ops/tools/oracle_monitor.py` polls
  the oracle and mirrors state. It does not currently branch on
  `lensId`. Change scope: add a dispatch on the 4-byte tag mapping
  to `track_record_commitments`, `dispute_commitments`,
  `methodology_hashes` tables, with the `0x00000000` default
  continuing to route to the existing SII/PSI report tables.
- **Public integration guide (`basis_protocol_integration_guide.md`):**
  mentions `getReportHash(entityId)` at lines 300, 301, 528, 556 as
  a per-entityId lookup. No external documentation promises lens
  filtering. External integrators are therefore unaffected.
- **Solidity (`src/BasisSIIOracle.sol:251`):** the source comment
  says `lensId Regulatory lens used (e.g., "SCO6")`. That comment
  is out of date (SCO6 was aspirational; no deployed caller uses
  it). Not a consumer — just stale inline text.
- **Test suite (`test/BasisSIIOracle.t.sol`):** tests `lensId`
  round-trip but does not pin it to specific values for SII/PSI.
  Unaffected.

No breakage. The three new lens values occupy a range that has never
been written.

### Q5 — Read path and enumeration

**Question:** When a reviewer asks "show me every track-record
commit for entity X in the last 30 days," where do they go?

**Answer.** There are two tiers:

1. **Per-key integrity check (on-chain, cheap, always-valid):** the
   reviewer has or computes an `entityId` and calls
   `getReportHash(entityId)`. One RPC call, O(1), returns
   `(reportHash, lensId, timestamp)`. This is the ONLY on-chain
   read path we promise for Bucket A. It's sufficient for the
   core honest-anchor property.
2. **Enumeration (off-chain, Postgres-authoritative):** "give me
   every track-record commit in the last 30 days" is answered
   by `track_record_commitments` in Postgres, joined to
   `track_record_events`. The row's `on_chain_tx_hash` and
   `on_chain_entity_id` columns let any reviewer convert a Postgres
   row back into a one-call on-chain integrity check via path (1).

The read path is NOT `eth_getLogs` over the whole ReportPublished
stream. That works today at current volume but is not promised to
scale. Any integrator who wants enumeration uses the Basis Hub API
or Postgres replicas, then verifies selected rows on chain.

Recommended schema addition to `track_record_commitments` and
`dispute_commitments`:

```sql
ALTER TABLE track_record_commitments
  ADD COLUMN on_chain_entity_id bytea;
ALTER TABLE dispute_commitments
  ADD COLUMN on_chain_entity_id bytea;
```

So reviewers can move from DB row → on-chain check without
recomputing the keccak.

---

## 12. Supersession log

- Section 2 tag conventions (`"TRCK"`, `"MTHD"`, `transitionKind` as
  lens) are **SUPERSEDED** by Section 11 Q3 (numeric ranges).
- Section 2 track-record entityId scheme is **SUPERSEDED** by
  Section 11 Q2 (adds explicit `eventType`, `entity_slug`, and
  `eventTimestamp` into the preimage; drops `eventHash`).
- Section 5 keeper pseudo-code is **SUPERSEDED** by the Task 3
  runbook (to be written next) which uses the numeric-range lens
  scheme and the updated entityId preimages.
