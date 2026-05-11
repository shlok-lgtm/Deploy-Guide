# Enrichment task budget audit — 2026-05-11

**Purpose:** Calibrate per-task budgets in `app/enrichment_worker.py` to actual
runtime, surface structural bottlenecks, and flag operator-decision items
(external-dependency flakiness vs paid-tier upgrade). Per orchestrator
Phase 1 / Item D.

## Methodology

Source: `cycle_errors` over 7 days, grouped by `cycle_phase` for
`error_type LIKE '%timeout%'` or `error_message ILIKE '%exceeded%'`.
Cross-referenced with `EnrichmentTask(name=..., timeout_seconds=...)`
definitions in `app/enrichment_worker.py`. The substrate is the
authoritative timeout-count source; the code has the authoritative
budget literal.

Note: `collector_cycle_stats` records SII fast-cycle collector p50 /
max latencies (per-coin), not enrichment tasks. Enrichment-task p50
runtime is therefore approximated by "did the task complete in <
budget?" — a binary signal, not a distribution. A future improvement
is to land an enrichment-side analog (out of scope for this PR;
queued in §Follow-ups).

## Findings — enrichment tasks with ≥ 3 timeouts in 7 days

| task | timeouts (7d) | budget | last seen | class | recommendation |
|---|---|---|---|---|---|
| treasury_flows | 43 | 300s | 2026-05-11 13:40Z | (a) too tight | bump to 600s |
| data_catalog_update | 42 | **60s** | 2026-05-11 13:37Z | (a) too tight | bump to 300s |
| materialized_compositions | 42 | **120s** | 2026-05-11 13:37Z | (a) too tight | bump to 300s |
| divergence_detection | 36 | 300s | 2026-05-11 13:40Z | (a) too tight | bump to 600s |
| onchain_governance_reads | 30 | 300s | 2026-05-11 13:40Z | (a)/(b) | bump to 600s, monitor |
| actor_classification | 27 | 600s | 2026-05-11 13:45Z | (b) structural | split per-chain or per-actor-class |
| entity_discovery | 20 | 300s | 2026-05-10 16:12Z | (a)/(b) | bump to 600s, monitor |
| wallet_reindex | 13 | 900s | 2026-05-11 13:50Z | (b) | PR #160 (Wave 4) restored concurrency; watch over next 7d |
| wallet_expansion | 11 | 2400s | 2026-05-11 14:15Z | (b)/(c) | structural; Etherscan rate-limit dominated — surface |
| governance_activity | 8 | 600s | 2026-05-11 13:46Z | (a)/(b) | bump to 900s; if still timing out, structural |
| dex_pool_ohlcv | 5 | 900s | 2026-05-11 11:39Z | watch | no change yet |
| mint_burn_events | 3 | 600s | 2026-05-10 11:24Z | (c) | Alchemy quota-exhausted (op-followup #1); no budget bump fixes this |

## Findings — non-enrichment but high-volume timeouts

These are not `EnrichmentTask` budgets; they are per-request budgets
inside collector code. Documented here for completeness but out of
scope for this audit; each has its own follow-up.

| cycle_phase | error_type | count (7d) | class | recommendation |
|---|---|---|---|---|
| edge_builder:ethereum | explorer_timeout | 246 | (c) external | Blockscout/Etherscan rate-limit; PR #160 fixed structural in the *wallet-scanner* but the edge-builder is a separate path |
| parameter_history | collectors__eth_call_sync_failure | 168 | (c) external | Alchemy plan exhausted (op-followup #1); accepted degraded |
| dao_collector | collectors__automate_dao_audit_cadence_failure | 6 | (b) | separate diagnostic; not budget-shaped |

## Classification key

- **(a) Budget too tight** — work is fundamentally bounded, but the
  budget literal doesn't reflect the work's actual size. Bump the
  literal; substrate should show the timeout rate drop ≥50% in 24h.
- **(b) Work is structurally too large** — work scales with a growing
  table or fan-out and won't fit in any reasonable single-task budget.
  Split or parallelize the task. Example: PR #160 split the wallet
  scanner's serial loop into concurrent gather. Materialized-view
  refresh is another candidate (per-view rather than all-at-once).
- **(c) External dependency is flaky / quota-exhausted** — operator
  decision: accept-degraded, pay for upgraded tier, or migrate
  provider. Code can't fix this and budget bumps don't help.

## Recommended actions

### Immediate (this audit's follow-up PRs)

| # | Task | From | To | Class | PR |
|---|---|---|---|---|---|
| 1 | data_catalog_update | 60s | 300s | (a) | follow-up |
| 2 | materialized_compositions | 120s | 300s | (a) | follow-up |
| 3 | treasury_flows | 300s | 600s | (a) | follow-up |
| 4 | divergence_detection | 300s | 600s | (a) | follow-up |
| 5 | onchain_governance_reads | 300s | 600s | (a)/(b) | follow-up |
| 6 | entity_discovery | 300s | 600s | (a)/(b) | follow-up |
| 7 | governance_activity | 600s | 900s | (a)/(b) | follow-up |

These are mechanical literal-bumps; can ship as a single PR since they
all touch `app/enrichment_worker.py` `EnrichmentTask(timeout_seconds=N)`
lines. Substrate gate (24h post-deploy): each task's timeout count in
`cycle_errors` should drop ≥ 50%; if not, escalate to (b).

### Queued (Wave-N CC prompts, one per task)

| Task | Why structural | Notes |
|---|---|---|
| actor_classification | 27 timeouts at 600s; classification pass over all wallets fans out unbounded as the wallet graph grows (currently 168k wallets). | Split per-chain or batch by last-classified-at age. |
| wallet_expansion | 2400s and still timing out (11 / 7d). Etherscan rate-limit dominated, similar to PR #160. | May need the same `asyncio.gather` shape; check `app/indexer/expander.py` for any serial-loop pattern. |
| materialized_compositions (if budget bump doesn't fix) | If post-deploy still ≥ 20 timeouts/7d, the work itself doesn't fit; refresh per-view rather than all-at-once. | Touch `app/data_layer/materialized_compositions.py`. |

### Operator decisions (surface only)

- **Alchemy plan upgrade.** parameter_history's 168 / 7d failures and
  mint_burn_events' 3 / 7d are both Alchemy 429s (op-followup #1).
  Current degraded state is "accepted." If decision changes, the
  budgets are sized correctly — no code action needed.
- **Edge-builder Blockscout/Etherscan tier.** edge_builder:ethereum
  is 246 / 7d timeouts. PR #160 fixed the scanner side. The
  edge-builder is a separate call path; same upstream constraint
  applies. Pursue (a) free-tier acceptance + retry, or (b) paid
  tier, or (c) defer the domain like bridge_flows.

## Substrate verification

```sql
SELECT cycle_phase, error_type, COUNT(*) AS n, MAX(occurred_at) AS last_seen
FROM cycle_errors
WHERE occurred_at > NOW() - INTERVAL '7 days'
  AND (error_type ILIKE '%timeout%' OR error_message ILIKE '%exceeded%')
GROUP BY 1, 2
ORDER BY n DESC
LIMIT 30;
```

Result quoted above. Run at 2026-05-11 16:30Z.

## Follow-ups

- **F1.** Land the literal-bump PR (items 1-7). Substrate verifier: after
  24h on the new budgets, each task's 7d timeout count drops ≥ 50%.
- **F2.** Land a per-task runtime histogram (analogous to
  `collector_cycle_stats` but for `EnrichmentTask`) so this audit can
  use real p50/p95 next time. Worth ~20 lines in
  `app/enrichment_worker.py::_execute_task` writing to a new
  `enrichment_task_stats` table.
- **F3.** Wave-N prompts for actor_classification / wallet_expansion
  structural splits.
- **F4.** Re-run this audit on 2026-05-18 (one week post-bump) and
  re-classify any task still timing out at the new budget as (b)
  structural.

## Cross-references

- `docs/basis_punchlist_2026_05_11.md` lessons 7, 8, 9, 10
- `app/enrichment_worker.py` (EnrichmentTask definitions)
- PR #160 (wallet scanner concurrency — exemplar of (b) split)
- v9.12 amendment (module-canonical) — Phase-2 work in this session
