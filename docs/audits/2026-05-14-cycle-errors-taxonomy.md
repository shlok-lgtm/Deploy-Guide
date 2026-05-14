# cycle_errors taxonomy — 2026-05-14

Follow-up to the v9.12 stabilization campaign. The 3 schema-drift bugs being addressed by the parallel A2 work (`dex_pools` / `entity_discovery` / `mint_burn_collector`) account for ~155 of the last 7d rows; this taxonomy enumerates everything else that is still firing so we have a single reference for ongoing ops triage.

Substrate window: last 7d (2026-05-07 → 2026-05-14 01:50 UTC).
Total cycle_errors rows in window: **9060**.

Substrate query (verbatim, run against project `small-scene-57890564`):

```sql
SELECT
  cycle_phase,
  LEFT(error_message, 100) AS error_pattern,
  COUNT(*) AS occurrences,
  MIN(occurred_at) AS first_seen,
  MAX(occurred_at) AS last_seen
FROM cycle_errors
WHERE occurred_at > NOW() - INTERVAL '7 days'
GROUP BY cycle_phase, LEFT(error_message, 100)
ORDER BY occurrences DESC
LIMIT 50;
```

## Top-line summary

| Category | 7d occurrences | Notes |
|---|---|---|
| schema-drift (legacy tables, already remediated) | ~3360 | `relation does not exist` patterns; all `last_seen` <= 2026-05-11 — fixed by recent migrations, no further action |
| schema-drift (active) | ~770 | `scored_at`, `collected_at`, `vendor_mentions`, `mentioned_vendors`, plus the 3 A2 bugs (~155) |
| data-type | ~1516 | numeric overflow (`parameter_history`), `NoneType` floats (`vault_collector`), unparseable timestamps (`cda_scores`) |
| dependency | ~1140 | Alchemy 429, llama.fi read timeouts, blockscout ReadTimeout, dwellir reverts, explorer 500/429 |
| business-logic | ~558 | JSON decode failures, etherscan-supported-chain mis-parse, "eurs: []" no-data |
| budget / soft-timeout | ~383 | `enrichment_*` task-budget exceedances; operational, not bugs |
| other | ~245 | wallet_scanner blank `error_message` (logging defect, dependency root cause) |

Total accounts for the dominant patterns; the long tail of <20-occurrence patterns is documented per phase.

## parameter_history

Dominant phase by volume (4517 errors / 7d, 786 in last 24h). Mostly fixable.

### Pattern: `relation "protocol_parameters" does not exist`
- **Category**: schema-drift (already remediated)
- **Occurrences (7d)**: 2145
- **First seen**: 2026-05-08T20:15:02Z
- **Last seen**: 2026-05-11T08:34:51Z
- **Suggested action**: None — `protocol_parameters` table now exists (confirmed via `information_schema.columns`); errors stopped on 2026-05-11.
- **References**: presumably resolved by migration 108 or 110

### Pattern: `numeric field overflow ... precision 30, scale 8 ... 10^22`
- **Category**: data-type
- **Occurrences (7d)**: 1212 (452 in last 24h, ongoing)
- **First seen**: 2026-05-11T11:21:53Z
- **Last seen**: 2026-05-14T01:18:19Z
- **Suggested action**: Real bug — surface for fix. `protocol_parameters.current_value` is `numeric(30,8)` (max ~10^22) but `app/collectors/parameter_history.py:598` writes `raw_int / normalization_factor`; for some specs the normalized value still exceeds the column bound. Options: (a) widen column to `numeric(50,8)`, (b) clamp/skip values > 10^22 and record raw in `current_value_raw`, (c) audit `PROTOCOL_PARAMETER_REGISTRY` for missing/wrong `normalization_factor` entries. Recommend (c) before (a) — current_value_raw is already TEXT so all data is preserved either way. Not a 50-line fix — needs domain review of the parameter registry.
- **References**: see investigation queue

### Pattern: `column "scored_at" does not exist ... ORDER BY scored_at DESC LIMIT 1`
- **Category**: schema-drift (active, not in A2 scope)
- **Occurrences (7d)**: 365 (136 in last 24h, ongoing)
- **First seen**: 2026-05-11T11:26:49Z
- **Last seen**: 2026-05-14T01:18:11Z
- **Suggested action**: Trivial fix — `app/collectors/parameter_history.py:326` selects from `scores` (which has `computed_at`/`updated_at`, NOT `scored_at`). The neighbouring queries on `psi_scores` are fine because `psi_scores.scored_at` exists. **Fix PR 1** below.
- **References**: candidate fix PR `fix/cycle-errors-parameter-history-scored-at`

### Pattern: `numeric field overflow ... precision 10, scale 4 ... 10^6`
- **Category**: data-type
- **Occurrences (7d)**: 304 (110 in last 24h, ongoing)
- **First seen**: 2026-05-11T11:26:53Z
- **Last seen**: 2026-05-14T01:18:12Z
- **Suggested action**: Same class as the 10^22 overflow — different write site, narrower column. Investigate alongside the 10^22 fix. Likely `score_event_value` or similar `numeric(10,4)` column receiving a normalized value. Not in this PR scope.
- **References**: see investigation queue

### Pattern: `both providers failed: alchemy=HTTP 429 ... dashboard.alchemy.com`
- **Category**: dependency (rate-limit)
- **Occurrences (7d)**: 335 (68 in last 24h)
- **First seen**: 2026-05-09T00:55:10Z
- **Last seen**: 2026-05-14T01:18:32Z
- **Suggested action**: Alchemy monthly capacity exhausted. Accept as operational noise; Dwellir fallback already in place but is also failing (next pattern). Not a code fix — billing / provider rotation.

### Pattern: `Expecting value: line 1 column 1 (char 0)`
- **Category**: dependency / business-logic edge
- **Occurrences (7d)**: 338
- **First seen**: 2026-05-08T20:15:00Z
- **Last seen**: 2026-05-08T22:59:41Z
- **Suggested action**: Resolved (no occurrences since 2026-05-08). Was likely empty body from RPC provider being JSON-decoded. No action.

### Pattern: `both providers failed: alchemy=HTTP 429 ... dwellir=rpc error 3: execution reverted`
- **Category**: dependency
- **Occurrences (7d)**: 193 (20 in last 24h)
- **First seen**: 2026-05-09T00:55:13Z
- **Last seen**: 2026-05-14T01:18:34Z
- **Suggested action**: Compound dependency failure — Alchemy quota + Dwellir revert. Operational; longer term consider catching `rpc error 3: execution reverted` separately (it indicates the contract call itself reverted, distinct from provider failure).

### Pattern: `invalid literal for int() with base 16: '//api.etherscan.io/v2/chainlist...`
- **Category**: business-logic
- **Occurrences (7d)**: 220
- **First seen**: 2026-05-08T20:15:01Z
- **Last seen**: 2026-05-11T08:34:45Z
- **Suggested action**: Resolved (no occurrences since 2026-05-11). Error message indicates Etherscan returned an HTML/text error body where hex was expected, and `int(..., 16)` failed. No action.

### Pattern: `relation "protocol_parameter_snapshots" does not exist`
- **Category**: schema-drift (already remediated)
- **Occurrences (7d)**: 78
- **Last seen**: 2026-05-11T08:34:53Z
- **Suggested action**: Resolved. No action.

### Low-volume patterns (parameter_history)
None below 20.

## tti_collector

### Pattern: `relation "tti_disclosure_extractions" does not exist`
- **Category**: schema-drift (already remediated)
- **Occurrences (7d)**: 410
- **Last seen**: 2026-05-11T08:33:00Z
- **Suggested action**: Resolved.

### Pattern: `HTTPSConnectionPool(host='api.llama.fi', port=443): Read timed out. (read timeout=15)`
- **Category**: dependency
- **Occurrences (7d)**: 51 (13 in last 24h)
- **First seen**: 2026-05-08T22:24:38Z
- **Last seen**: 2026-05-13T22:26:25Z
- **Suggested action**: Operational; consider raising timeout to 30s to match `rpi_incident_detector` (which uses 30s for the same host). Trivial change but outside the 5-PR budget; queued.

## rpi_forum_scraper

### Pattern: `column "mentioned_vendors" is of type jsonb but expression is of type text[]`
- **Category**: schema-drift (already remediated)
- **Occurrences (7d)**: 572
- **Last seen**: 2026-05-10T11:55:14Z
- **Suggested action**: Resolved (last_seen 4 days ago). The writer in `app/rpi/forum_scraper.py` was fixed to cast text[] -> jsonb.

## rpi_scorer

### Pattern: `column "vendor_mentions" does not exist`
- **Category**: schema-drift (active, not in A2 scope)
- **Occurrences (7d)**: 39
- **First seen**: 2026-05-10T12:03:15Z
- **Last seen**: 2026-05-12T16:17:00Z
- **Suggested action**: Trivial fix — `app/rpi/scorer.py:537,541,542` reference `vendor_mentions`, but `governance_forum_posts` exposes `mentioned_vendors` (jsonb). Also `collected_at` on line 540 must become `posted_at` (or `created_at`). Below the 20-in-24h threshold, but the bug is real and the fix is small. **Fix PR 2** below.

## enforcement_history

### Pattern: `relation "enforcement_records" does not exist`
- **Category**: schema-drift (already remediated)
- **Occurrences (7d)**: 365
- **Last seen**: 2026-05-11T08:33:07Z
- **Suggested action**: Resolved.

## regulatory_scraper

### Pattern: `relation "regulatory_registry_checks" does not exist`
- **Category**: schema-drift (already remediated)
- **Occurrences (7d)**: 272
- **Last seen**: 2026-05-11T02:16:17Z
- **Suggested action**: Resolved.

## wallet_scanner

### Pattern: (empty error_message; traceback is `httpx.ReadTimeout` in `app/indexer/scanner.py:168`)
- **Category**: dependency
- **Occurrences (7d)**: 245 (46 in last 24h)
- **First seen**: 2026-05-08T20:18:14Z
- **Last seen**: 2026-05-14T01:14:02Z
- **Suggested action**: The error is blockscout v2 timing out; severity `caught` — fallback to Etherscan V2 likely succeeds. The empty `error_message` is a logging defect (handler stores str(exc) of `httpx.ReadTimeout()` which has no args). Low-priority logging cleanup, not a functional bug.

## vault_collector

### Pattern: `float() argument must be a string or a real number, not 'NoneType'`
- **Category**: business-logic / data-type
- **Occurrences (7d)**: 146 (24 in last 24h)
- **First seen**: 2026-05-08T20:19:39Z
- **Last seen**: 2026-05-14T01:16:15Z
- **Suggested action**: Real bug — `app/collectors/vault_collector.py` calls `float(x)` on a `None` somewhere. Without a stack trace in the message, locating the exact line needs a one-shot reproduction. Queued for investigation, not trivial enough to PR blind.

## flows_collection

### Pattern: `eurs: []`
- **Category**: business-logic
- **Occurrences (7d)**: 133 (16 in last 24h)
- **First seen**: 2026-05-07T03:50:43Z
- **Last seen**: 2026-05-14T01:40:17Z
- **Suggested action**: Looks like an asset (`eurs`) being marked as having no flows. May be a "soft" error logged when collector finds nothing, rather than a true failure. Verify by reading flows_collection error-record call sites and decide whether to demote to logger.info.

## entity_discovery

### Pattern: `column "entity_id" does not exist ... FROM generic_index_scores`
- **Category**: schema-drift — **covered by A2**, skip

## mint_burn_collector

### Pattern: `column "entity_id" of relation "discovery_signals" does not exist`
- **Category**: schema-drift — **covered by A2**, skip

## peg_monitor

### Pattern: `column "entity_id" of relation "discovery_signals" does not exist`
- **Category**: schema-drift — same root cause as the mint_burn_collector entry that A2 is fixing (same relation, same missing column); fix should subsume this site. **Surface to A2** if not already in scope.
- **Occurrences (7d)**: 22 (11 in last 24h, ongoing)
- **First seen**: 2026-05-12T01:44:17Z
- **Last seen**: 2026-05-14T01:50:52Z

## dex_pools

### Pattern: `column "stablecoin_id" does not exist ... protocol_collateral_exposure`
- **Category**: schema-drift — **covered by A2**, skip (12 distinct protocol-keyed variants)

## edge_builder:ethereum

### Pattern: `Explorer returned 500 for 0x28c6c062…`
- **Category**: dependency
- **Occurrences (7d)**: 54 (12 in last 24h)
- **Suggested action**: Operational. Single-wallet Etherscan flakiness.

### Pattern: `Explorer rate limit (HTTP 429)`
- **Category**: dependency (rate-limit)
- **Occurrences (last 24h)**: 5
- **Suggested action**: Expected noise.

## dao_collector

### Pattern: `column "collected_at" does not exist`
- **Category**: schema-drift (active, not in A2 scope)
- **Occurrences (7d)**: 35 (7 in last 24h)
- **First seen**: 2026-05-09T14:59:05Z
- **Last seen**: 2026-05-13T20:08:29Z
- **Suggested action**: `app/collectors/dao_collector.py:594` filters `governance_forum_posts.collected_at` (doesn't exist) and line 597 references `raw_text` (also doesn't exist; actual column is `body_excerpt`). Below the 20-in-24h threshold but fix is trivial. Bundle with the rpi_scorer fix below since both files touch the same table and same column-name corrections. **Fix PR 2** below.

## cda_scores

### Pattern: `Unknown string format: 2025-02-28 21:00:00 ET (2025-03-01 09:00:00 HKT)`
- **Category**: business-logic
- **Occurrences (7d)**: 35
- **Last seen**: 2026-05-11T16:40:36Z
- **Suggested action**: Resolved (no occurrences in last 24h). Date parser couldn't handle dual-timezone string. If returns, add a regex preprocessor to strip parenthetical alt-tz. No immediate action.

## enrichment_* (task budgets)

| Phase | 7d | Budget exceeded |
|---|---|---|
| treasury_flows | 71 (300s + 600s) | yes — budget likely tight; consider 900s or pagination |
| materialized_compositions | 42 | 120s |
| actor_classification | 41 | 600s |
| data_catalog_update | 42 | 60s |
| sanctions_screening | 45 | (relation missing; already remediated) |
| parent_financials | 45 | (relation missing; already remediated) |
| validator_performance | 45 | (relation missing; already remediated) |
| wallet_expansion | 36 | 2400s |
| divergence_detection | 36 | 300s |
| web_research | 6 | 600s |
| entity_discovery | 20 | 300s |
| wallet_reindex | 20 | 900s |
| onchain_governance_reads | 30 | 300s |

- **Category**: dependency / soft-timeout (operational)
- **Suggested action**: These are budget-management signals, not bugs. Cross-reference with `docs/audits/2026-05-11-enrichment-task-budget-audit.md` and adjust budgets if a phase is structurally over-budget.

## Investigation queue

Ordered by 7d occurrence count. Each item needs deeper work than a one-line schema fix.

1. **parameter_history numeric overflow (precision 30,8 → 10^22)** — 1212 / 7d, 452 / 24h. Next step: log the offending `(protocol_slug, parameter_key, raw_int, normalization_factor, normalized)` tuple to identify which spec(s) are mis-normalized, then either correct `normalization_factor` in `PROTOCOL_PARAMETER_REGISTRY` or widen the column.
2. **parameter_history numeric overflow (precision 10,4 → 10^6)** — 304 / 7d, 110 / 24h. Locate write site (probably distinct from `current_value` — likely score_event or parameter_change_log). `grep -rn 'precision 10, scale 4\|numeric(10,4)\|numeric(10, 4)' migrations/` to find candidate columns.
3. **vault_collector NoneType float()** — 146 / 7d, 24 / 24h. Next step: enable a single-shot trace in `app/collectors/vault_collector.py` (or read tracebacks from a fresh row of `cycle_errors WHERE cycle_phase='vault_collector' ORDER BY occurred_at DESC LIMIT 1`) to find the line.
4. **flows_collection `eurs: []`** — 133 / 7d. Next step: read the `flows_collection` error-record callsite; if this is just "no data" it should be `logger.info` not `_record_cycle_error`.
5. **peg_monitor discovery_signals.entity_id missing** — 22 / 7d. Sibling to mint_burn_collector bug A2 is fixing; confirm A2's column-add migration covers this writer too, otherwise add to A2's PR.

## Trivial fix PRs (proposed)

Two PRs that meet the criteria (single file each, <50 lines, not in A2 scope):

- **PR 1** `fix/cycle-errors-parameter-history-scored-at` — one-line change in `app/collectors/parameter_history.py:326` (`scored_at` → `computed_at`). Pre-deploy 24h baseline: 136 errors. Post-deploy 2h halt criterion: <10 (1 cycle of stragglers permitted).
- **PR 2** `fix/cycle-errors-governance-forum-posts-column-names` — bundles `app/rpi/scorer.py` (`vendor_mentions` → `mentioned_vendors`, `collected_at` → `posted_at`, jsonb-aware existence check) and `app/collectors/dao_collector.py` (`collected_at` → `posted_at`, `raw_text` → `body_excerpt`). Two files but they share a single defect class (governance_forum_posts column drift) and total <30 lines. Pre-deploy 24h baseline: 7 (dao) + ~5 estimated (rpi_scorer, last seen 2026-05-12). Post-deploy 2h halt criterion: <5.

## Halt-rule check

- Distinct fixable bug classes outside A2: **5** (scored_at, governance_forum_posts columns, numeric(30,8) overflow, numeric(10,4) overflow, vault NoneType). Below the 10-class limit; no further split required.
- Active-outage check: no pattern is failing every cycle. The dominant active ones (`numeric overflow`, `scored_at`, `Alchemy 429`) are partial failures inside collectors that catch and continue.
- A2 overlap: peg_monitor's `discovery_signals.entity_id missing` is a sibling site to A2's mint_burn_collector fix — flagged for A2 to absorb.
