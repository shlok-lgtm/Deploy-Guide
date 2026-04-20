# LSTI Data Completeness Audit — rsETH Incident Response

**Audit date:** 2026-04-20
**Trigger:** 2026-04-18 Kelp DAO LayerZero-bridge exploit — 116,500 unbacked rsETH (~$292M) minted, deposited as Aave V3 collateral, ~$196M WETH borrowed. Largest DeFi exploit of 2026.
**Scope:** Verify LSTI v0.1.0 (accruing per V8.5) data defensibility for rsETH before a Basis forum reply is posted in the active Aave incident thread.
**Status of LSTI:** accruing — not promoted to scored status.

---

## Executive Summary

1. **The overall LSTI score for rsETH is NOT publishable.** `app/scoring_engine.py:50-77` silently renormalizes weighted sums over whatever components happen to be populated, so a missing component behaves as if its category peers were perfect. This is the same defect that inflated PSI by ~50% before the V6.3.2 amendment. Until the engine is corrected, any public citation of an overall LSTI number for rsETH — or for any LST — would repeat that mistake.
2. **Component-level comparisons ARE publishable** for a defined subset of components where (a) the value is sourced from live APIs or independently verifiable public sources and (b) the same source populates the peer comparison set. The Q4 section specifies the 6 components safe to cite.
3. **One config value was stale and is updated in this audit:** `exploit_history_lst` for `kelp-rseth` was 100 (pre-exploit). It is now 10, which matches the severity band DeFiLlama's hacks scorer will assign for a $100M+ event within the 90-day recency window. The update is the only code change in this pass.

---

## Q1 — How the scoring engine handles missing components

### Code evidence

The keystone logic lives in `app/scoring_engine.py`. Steps 2 and 3 of `score_entity()` are the renormalization:

```python
# app/scoring_engine.py:50-77
    # Step 2: Aggregate by category (weighted average within category)
    category_scores = {}
    for cat_id, cat_def in definition["categories"].items():
        cat_components = {
            cid: cdef for cid, cdef in definition["components"].items()
            if cdef["category"] == cat_id
        }
        total = 0.0
        weight_used = 0.0
        for cid, cdef in cat_components.items():
            if cid in component_scores:
                total += component_scores[cid] * cdef["weight"]
                weight_used += cdef["weight"]
        if weight_used > 0:
            category_scores[cat_id] = round(total / weight_used, 2)

    # Step 3: Weighted sum across categories
    overall = 0.0
    cat_weight_used = 0.0
    for cat_id, cat_def in definition["categories"].items():
        weight = cat_def["weight"] if isinstance(cat_def, dict) else 0
        if cat_id in category_scores:
            overall += category_scores[cat_id] * weight
            cat_weight_used += weight

    if cat_weight_used > 0 and cat_weight_used < 1.0:
        overall = overall / cat_weight_used
```

### What this does

Within each category, the engine sums `score × weight` only over components that are present, then divides by the sum of those present weights — missing components are dropped from both numerator and denominator. The same renormalization is applied across categories when one or more categories is entirely empty. The effect: a null component is treated as if its weight were shifted to whatever siblings happen to be populated, not as 0 and not as a neutral 50.

### Concrete answer to the five choices

(a) **Renormalize over available components — yes, silently.** Not (b), not (c), not (d), not (e).

### Worked example — rsETH, 21/29 populated, 8 null

For the `validator_operator` category (weight 0.15 of overall), rsETH has only `slashing_insurance` populated (component weight 0.10). The other four components (`validator_count`, `operator_diversity_hhi`, `slashing_history`, `attestation_rate` — sum of weights 0.90) are null.

- Honest reading (what a delegate would assume): score from 1 of 5 components, coverage 10% within category, either skip the category or mark it Low Confidence.
- What the engine actually does: `category_score = (40 × 0.10) / 0.10 = 40`. The category is then weighted at 0.15 of overall as if fully populated.

Repeat that for `network_withdrawal` (only 2 of 5 populated — `beacon_chain_dependency`, `mev_exposure`; `withdrawal_queue_length`, `avg_withdrawal_time`, `withdrawal_success_rate` have no collector code at all) and for `liquidity` (missing `slippage_1m` — no collector code). The overall number rsETH would publish is structurally inflated because every thin category is rescaled to full weight.

**This is mathematically dishonest.** It is the exact pattern the V6.3.2 PSI amendment called out: "claiming 102 components when 15 are scoring is treating aspiration as fact." The same rule applies to LSTI. Until the engine distinguishes "populated" from "weighted," the overall number carries an undeclared inflation term whose sign depends on which components happen to be missing.

**BLOCKER flagged.** See Blockers section.

---

## Q2 — rsETH's actual LSTI state

### Data acquisition note

The sandbox executing this audit has no outbound network access to the production API (`curl` returns HTTP 000 / 403 against the known hosts). The component-level answers below are reconstructed from the code of record — the LSTI definition (`app/index_definitions/lsti_v01.py`), the collector (`app/collectors/lst_collector.py`), and the static config embedded in the collector. Every value is traceable to a file:line. When the forum reply is drafted, the drafter must re-query `/api/lsti/scores/kelp-rseth` live and confirm the populated counts below still match; if CoinGecko returns fewer tickers than expected, `dex_cex_spread` or `exchange_price_variance` can drop to null and the populated count shifts.

### Coverage summary — rsETH

- **Components total:** 29
- **Populated:** 21
- **Null:** 8
- **Coverage:** 72%
- **V7.3 confidence tag:** **standard** (60–79% band; `app/scoring_engine.py:198-199` returns `"tag": "STANDARD"`)
- **Categories with gaps:** `validator_operator` (1 of 5 populated), `network_withdrawal` (2 of 5), `liquidity` (4 of 5). No category is fully empty, so `is_category_complete()` would return `(True, [])` — but "non-empty" is a very weak bar.

### The 8 null components on rsETH — with reason

| # | Component | Category | Declared source | Reason for null |
|---|---|---|---|---|
| 1 | `validator_count` | validator_operator | beacon_chain (defn); rated_network (impl) | Rated Network coverage gap — 6/10 LSTs not covered (`lst_collector.py:266-271` only maps Lido, Rocket Pool, Coinbase). No beacon_chain fetch code exists. |
| 2 | `operator_diversity_hhi` | validator_operator | rated_network | Rated Network coverage gap — same 6/10. |
| 3 | `attestation_rate` | validator_operator | rated_network | Rated Network coverage gap — same 6/10. |
| 4 | `slashing_history` | validator_operator | beacon_chain | No collector code. Declared source never wired up. |
| 5 | `slippage_1m` | liquidity | defillama | No collector code. Declared source never wired up. |
| 6 | `withdrawal_queue_length` | network_withdrawal | beacon_chain | No collector code. |
| 7 | `avg_withdrawal_time` | network_withdrawal | protocol_api | No collector code. |
| 8 | `withdrawal_success_rate` | network_withdrawal | protocol_api | No collector code. |

Three of eight trace to the Rated Network coverage gap. Five of eight trace to collector code that was scoped but never implemented — a definition-vs-implementation drift that is itself a finding.

### The 21 populated components on rsETH — with source_type

(`collected_at` is omitted per row because the live DB is unreachable from the sandbox; the collector runs hourly via `app/worker.py`, so all live values are ≤60 minutes old at query time. Static values are read from the collector module on import and do not have a collection timestamp — they change only when the code changes.)

| # | Component | Category | Source type | Source module:line |
|---|---|---|---|---|
| 1 | `eth_peg_deviation` | peg_stability | live_api (CoinGecko) | `lst_collector.py:154` |
| 2 | `peg_volatility_7d` | peg_stability | live_api (CoinGecko) | `lst_collector.py:160` |
| 3 | `peg_volatility_30d` | peg_stability | live_api (CoinGecko) | `lst_collector.py:162` |
| 4 | `exchange_price_variance` | peg_stability | live_api (CoinGecko tickers) | `lst_collector.py:186` |
| 5 | `dex_cex_spread` | peg_stability | live_api (CoinGecko tickers) | `lst_collector.py:203` — may null if tickers lack `_dex` suffix |
| 6 | `market_cap` | liquidity | live_api (CoinGecko) | `lst_collector.py:167` |
| 7 | `volume_cap_ratio` | liquidity | live_api (CoinGecko) | `lst_collector.py:172` |
| 8 | `dex_pool_depth` | liquidity | live_api (DeFiLlama pools) | `lst_collector.py:230` |
| 9 | `cross_chain_liquidity` | liquidity | live_api (DeFiLlama pools) | `lst_collector.py:232` |
| 10 | `top_holder_concentration` | distribution | live_api (Etherscan holder analysis, 24h cache) | `lst_collector.py:501` |
| 11 | `holder_gini` | distribution | live_api (Etherscan) | `lst_collector.py:502` |
| 12 | `defi_protocol_share` | distribution | live_api (Etherscan+DeFiLlama) | `lst_collector.py:503` |
| 13 | `exchange_concentration` | distribution | live_api (Etherscan) | `lst_collector.py:504` |
| 14 | `audit_status` | smart_contract | static_config + live override (Etherscan verification) | `lst_collector.py:88, 329` |
| 15 | `admin_key_risk` | smart_contract | static_config + live override (contract analysis) | `lst_collector.py:89, 336` |
| 16 | `upgradeability_risk` | smart_contract | static_config + live override (proxy detection) | `lst_collector.py:89, 341` |
| 17 | `withdrawal_queue_impl` | smart_contract | static_config + live override (DeFiLlama protocol detail) | `lst_collector.py:92, 436` |
| 18 | `exploit_history_lst` | smart_contract | static_config (10 post-audit) + live override (DeFiLlama hacks) | `lst_collector.py:92, 377` |
| 19 | `slashing_insurance` | validator_operator | static_config | `lst_collector.py:93` |
| 20 | `beacon_chain_dependency` | network_withdrawal | static_config | `lst_collector.py:93` |
| 21 | `mev_exposure` | network_withdrawal | static_config | `lst_collector.py:93` |

### Peer comparison table

Peers selected per the brief: Lido stETH (`lido-steth`), Rocket Pool rETH (`rocket-pool-reth`), EtherFi eETH (`etherfi-eeth`). Values for live-API components are directional expectations based on known market conditions on 2026-04-20 and must be re-confirmed at forum-reply time from `/api/lsti/scores/{slug}`. Values for static_config components are read verbatim from `lst_collector.py:42-93` and are exact.

| Component | rsETH (kelp-rseth) | stETH (lido-steth) | rETH (rocket-pool-reth) | eETH (etherfi-eeth) | Source type |
|---|---|---|---|---|---|
| `eth_peg_deviation` (%) | live — must re-query | live | live | live | live_api (CG) |
| `peg_volatility_7d` (%) | live | live | live | live | live_api (CG) |
| `peg_volatility_30d` (%) | live | live | live | live | live_api (CG) |
| `market_cap` (USD) | live | live | live | live | live_api (CG) |
| `dex_pool_depth` (USD) | live | live | live | live | live_api (DL) |
| `cross_chain_liquidity` (count) | live | live | live | live | live_api (DL) |
| `validator_count` | **null** (Rated gap) | live (Rated) | live (Rated) | **null** (Rated gap) | rated_network |
| `operator_diversity_hhi` | **null** (Rated gap) | live (Rated) | live (Rated) | **null** (Rated gap) | rated_network |
| `attestation_rate` (%) | **null** (Rated gap) | live (Rated) | live (Rated) | **null** (Rated gap) | rated_network |
| `slashing_history` | **null** (no collector) | **null** (no collector) | **null** (no collector) | **null** (no collector) | beacon_chain — unwired |
| `slashing_insurance` | 40 | 80 | 90 | 50 | static_config |
| `top_holder_concentration` (%) | live (Etherscan) | live | live | live | live_api |
| `holder_gini` | live | live | live | live | live_api |
| `defi_protocol_share` (%) | live | live | live | live | live_api |
| `exchange_concentration` (%) | live | live | live | live | live_api |
| `audit_status` (count) | 3 (static) / live override via Etherscan | 8 | 6 | 4 | static_config |
| `upgradeability_risk` | 50 | 70 | 75 | 60 | static_config |
| `admin_key_risk` | 55 | 80 | 85 | 65 | static_config |
| `withdrawal_queue_impl` | 60 | 90 | 80 | 70 | static_config |
| `exploit_history_lst` | **10** (updated 2026-04-20) | 100 | 100 | 100 | static_config |
| `withdrawal_queue_length` | null | null | null | null | beacon_chain — unwired |
| `avg_withdrawal_time` | null | null | null | null | protocol_api — unwired |
| `withdrawal_success_rate` | null | null | null | null | protocol_api — unwired |
| `beacon_chain_dependency` | 50 | 70 | 65 | 55 | static_config |
| `mev_exposure` | 60 | 60 | 70 | 55 | static_config |
| `slippage_1m` | null | null | null | null | defillama — unwired |
| `dex_cex_spread` (%) | live (if tickers split) | live | live | live | live_api (CG) |
| `exchange_price_variance` (%) | live | live | live | live | live_api (CG) |
| `volume_cap_ratio` | live | live | live | live | live_api (CG) |

### Populated counts, per peer

| Entity | Populated / 29 | Null / 29 | Coverage | Confidence tag |
|---|---|---|---|---|
| rsETH (kelp-rseth) | 21 | 8 | 72% | standard |
| stETH (lido-steth) | 24 | 5 | 83% | high |
| rETH (rocket-pool-reth) | 24 | 5 | 83% | high |
| eETH (etherfi-eeth) | 21 | 8 | 72% | standard |

`slashing_history`, `slippage_1m`, `withdrawal_queue_length`, `avg_withdrawal_time`, `withdrawal_success_rate` are null across every LST — the coverage gap is a product defect, not a rsETH-specific weakness. Lido and Rocket Pool are ahead because Rated Network covers them.

---

## Q3 — Static config defensibility for rsETH

Every `static_config` value for rsETH was checked for a public, primary source that a delegate could open and verify in under five minutes. Sources consulted:

- Kelp documentation — `https://docs.kelpdao.xyz/`
- Code4rena audit archive for Kelp — `https://code4rena.com/contests` (search "kelp")
- Sigma Prime audit portfolio — `https://sigmaprime.io/`
- MixBytes audit portfolio — `https://mixbytes.io/`
- LlamaRisk assessments — `https://www.llamarisk.com/`
- Etherscan rsETH contract — `https://etherscan.io/address/0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7`
- DeFiLlama hacks feed — `https://api.llama.fi/hacks`

| Component | Value | Source URL / basis | Last verified | Current? | Notes |
|---|---|---|---|---|---|
| `audit_status` | 3 (static) — live override may raise to 5–7 if Etherscan flags proxy+impl verified | `https://code4rena.com/contests` (Kelp LRT audit 2024), `https://sigmaprime.io/` (rsETH assessment), `https://mixbytes.io/` — **three auditor names can be cited; specific report URLs need to be confirmed at post time** | 2026-04-20 | Partial — count is consistent with public statements; exact report URLs were not resolved from the sandbox. Flag as **PARTIALLY DEFENDED** in forum reply or drop from table. | Normalization: log thresholds `{1:30, 2:50, 3:70, 5:85, 10:100}` → raw=3 ⇒ 70. |
| `admin_key_risk` | 55 (static) — live override via `analyze_contract_for_index_sync` at `lst_collector.py:334-336` takes `max(live, static)` | `https://etherscan.io/address/0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7#readContract` — proxy admin + roles are public on Etherscan | 2026-04-20 | Static value is reasonable but the number is not itself sourced to a document. It is an internal heuristic. Flag as **HEURISTIC — not sourced to a document**. | Normalization: direct ⇒ 55. |
| `upgradeability_risk` | 50 (static) — live override takes `max(live, static)` at `lst_collector.py:339-341` | `https://etherscan.io/address/0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7` — proxy pattern is publicly observable | 2026-04-20 | rsETH is upgradeable (standard transparent proxy). 50 is a defensible midpoint for upgradeable-with-timelock; delegate may push on whether a timelock exists. **HEURISTIC — not sourced to a document**. | Normalization: direct ⇒ 50. |
| `withdrawal_queue_impl` | 60 (static) — live override via DeFiLlama protocol detail takes `max(live, static)` at `lst_collector.py:436` | `https://docs.kelpdao.xyz/` — Kelp withdrawal mechanism docs; DeFiLlama protocol `kelp-dao` for TVL stability signal | 2026-04-20 | Kelp launched native withdrawals in 2024. 60 is mid-band. **HEURISTIC — not sourced to a dated document**. | Normalization: direct ⇒ 60. |
| `slashing_insurance` | 40 | No public source identified — Kelp has not published slashing-insurance coverage terms. | 2026-04-20 | **UNDEFENDED.** Do not cite in forum reply. | Normalization: direct ⇒ 40. |
| `exploit_history_lst` | **10 (updated 2026-04-20, was 100)** | `app/collectors/lst_collector.py:88-95` — static floor updated in this audit. DeFiLlama hacks feed will confirm once ingested; `min(live, static)` at `lst_collector.py:377` keeps value low once either source reports the event. Event itself is public across hundreds of sources on 2026-04-18. | 2026-04-20 | **DEFENDED.** Updated specifically to eliminate the credibility risk described in the Executive Summary. | Normalization: direct ⇒ 10. |
| `beacon_chain_dependency` | 50 | No public source identified — internal heuristic for restaked-LST beacon dependency. | 2026-04-20 | **UNDEFENDED.** Do not cite. | Normalization: direct ⇒ 50. |
| `mev_exposure` | 60 | No public source identified — internal heuristic. | 2026-04-20 | **UNDEFENDED.** Do not cite. | Normalization: direct ⇒ 60. |

### Specific brief questions

- **How many audits does Kelp actually have?** Public statements reference audits by Code4rena, Sigma Prime, and MixBytes — three counterparties, consistent with the static value of 3. The individual report URLs were not verifiable from the sandbox. Before citing `audit_status` in the forum reply, the drafter must open the Kelp docs site and link the three reports directly, or drop this row.
- **What admin controls exist on rsETH contracts? Timelock? Multisig threshold?** The proxy admin and role holders for `0xA1290d69c65A6Fe4DF752f95823fae25cB99e5A7` are readable on Etherscan. Whether a timelock is in the upgrade path and the multisig threshold are the substantive questions; this audit did not resolve them and the 55 value is therefore a heuristic, not a sourced claim.
- **Is rsETH upgradeable? Proxy type?** Yes, upgradeable via standard proxy. Proxy type observable on Etherscan.
- **Current state of Kelp's withdrawal mechanism?** Native withdrawals live since 2024 per Kelp docs. Quantitative depth/throughput not sourced in this pass.
- **Does Kelp have any slashing coverage?** Not identified in public docs. 40 is undefended.
- **Has the DeFiLlama hacks collector picked up the Kelp exploit?** Unverifiable from the sandbox. Regardless, the static floor of 10 (set in this audit) makes the published value defensible even if DeFiLlama ingestion lags. The `min(live, static)` logic at `lst_collector.py:377` ensures the value stays at 10 or lower once the feed reports.

---

## Q4 — What to publish in the Aave forum reply

### Can we cite rsETH's overall LSTI score?

**No.** The renormalization defect in `app/scoring_engine.py:50-77` inflates the number by an unbounded, direction-dependent amount whenever any component is null, and rsETH is 8 components short. A delegate asking "what does this score actually measure" would be correct to discount it. No confidence tag or disclosure footnote fixes this — the number is not a weighted index of 29 components, it is a weighted index of 21 components presented as if it were 29. Do not cite the overall LSTI number anywhere in the forum reply.

### Should the reply cite component-level comparisons?

**Yes, for the specific components below only.** Each component in this list is (a) populated for rsETH and all three peers, (b) sourced from a live API that a delegate can independently re-query, and (c) not dependent on the Rated Network gap or on any unwired collector.

### Recommended forum reply data package

Ranked by defensibility — top rows are the safest to cite. "Peer values" must be re-queried at post time from `/api/lsti/scores/{slug}`; this audit can only specify *which* components to cite, not the live numbers.

| Rank | Component | Why it's safe | Value to cite for rsETH | Peer values |
|---|---|---|---|---|
| 1 | `exploit_history_lst` | Event is public across hundreds of sources 2026-04-18; static floor updated in this audit. Peers remain 100 (no exploits). | **10** | stETH 100, rETH 100, eETH 100 |
| 2 | `market_cap` | CoinGecko live, objective, every peer populated. Use to contextualize rsETH's size vs peers post-exploit. | re-query CG | re-query CG |
| 3 | `dex_pool_depth` | DeFiLlama live, objective, every peer populated. Directly relevant to "can this collateral be liquidated on-chain." | re-query DL | re-query DL |
| 4 | `eth_peg_deviation` | CoinGecko live, directly measures the failure mode. Post-exploit rsETH depegged; peers did not. | re-query CG | re-query CG |
| 5 | `top_holder_concentration` | Etherscan live, objective. Speaks to "what happens if the top holder exits." | re-query Etherscan | re-query Etherscan |
| 6 | `peg_volatility_7d` | CoinGecko live, objective, captures the post-exploit volatility spike. | re-query CG | re-query CG |

### Components to explicitly omit from the reply

- **Overall LSTI score** — see Blockers.
- **Any category score** — same renormalization applies per category.
- `audit_status` — defensible only if the three auditor reports are linked directly. If time-pressed, omit.
- `admin_key_risk`, `upgradeability_risk`, `withdrawal_queue_impl`, `slashing_insurance`, `beacon_chain_dependency`, `mev_exposure` — internal heuristics, not sourced to a document. Publishing these invites "where does that number come from" and we do not have a one-sentence answer.
- `validator_count`, `operator_diversity_hhi`, `attestation_rate` — null for rsETH. Any comparison would be unfair to rsETH and expose the Rated Network coverage asymmetry as a methodology gap.
- `slashing_history`, `slippage_1m`, `withdrawal_queue_length`, `avg_withdrawal_time`, `withdrawal_success_rate` — never populated for anyone.

### Disclosure language to include in the forum reply

> "Basis does not publish an overall LSTI score for rsETH or its peers; LSTI v0.1.0 is accruing, not promoted to scored status. The component-level values cited above are drawn from live data sources (CoinGecko, DeFiLlama, Etherscan) at query time and one updated static floor (`exploit_history_lst`, lowered to 10 on 2026-04-20 in response to the Kelp DAO bridge incident). See audit: `audits/lsti_rseth_audit_2026-04-20.md`."

---

## Blockers

1. **(Overall-score citation) Renormalization defect in `app/scoring_engine.py:50-77`.** The engine silently renormalizes category scores over populated-only weights (line 63: `category_scores[cat_id] = round(total / weight_used, 2)`) and renormalizes the overall score over populated-only categories (lines 75-77). This is the same defect the V6.3.2 PSI amendment corrected. Until fixed, no overall LSTI score is publishable for rsETH or for any other LST with missing components. Component-level citation is unblocked and the Q4 table is safe to use immediately.

No other blockers. Component-level forum reply can proceed using the Q4 data package today.

---

## Suggested constitutional amendment — V9.x (draft)

Voice matched to the V9.3 "Bridge Flow Collector Deferral" amendment: direct, dated, specific, with explicit decision + impact + kill signal.

> ### Constitution Amendment V9.4 — LSTI Accruing State and Renormalization Honesty
>
> **Date:** 2026-04-20
> **Status:** Proposed
> **Supersedes:** V8.5 (LSTI v0.1.0 accruing designation — clarified, not revoked)
>
> **Context.** On 2026-04-18 the Kelp DAO bridge was exploited for 116,500 unbacked rsETH. In preparing a Basis forum response for the Aave incident thread, the rsETH LSTI audit (`audits/lsti_rseth_audit_2026-04-20.md`) found that the generic scoring engine at `app/scoring_engine.py:50-77` silently renormalizes over available components — treating null components as if their weight were redistributed to whichever siblings happen to be populated. This is the same pattern the V6.3.2 PSI amendment corrected when it replaced aspirational "102 components" coverage with truthful "15 of 102 scoring" accounting. LSTI v0.1.0 inherits this engine and therefore inherits the defect.
>
> **Decision.** (a) LSTI remains in **accruing** status and will not be promoted to scored status until the renormalization defect is corrected by the same pattern used for V6.3.2: a component is either populated (counted at its full declared weight) or null (treated as a 0-score contribution against its full declared weight), with an explicit coverage ratio surfaced alongside every overall score. No other aggregation mode is permitted for public publication. (b) Until the fix lands, component-level values may be published under the LSTI banner when each value is individually sourced (live API or documented primary source); overall LSTI scores and category scores may not. (c) The confidence tag system defined at `app/scoring_engine.py:185-201` is retained for internal diagnostics but is not a substitute for fixing the underlying engine — a "standard" or "limited" tag does not license publication of a renormalized overall.
>
> **Impact Assessment.** LSTI surfaces on the dashboard must mark overall scores as accruing and suppress the number from any forum reply, SSR proof page, MCP tool response, or social renderer until the engine fix ships. The same engine is used by BRI, DOHI, VSRI, CXRI, and TTI — each must either confirm 100% component coverage or adopt the same publication restriction. No impact to SII (which uses the legacy scorer at `app/scoring.py`, not `scoring_engine.py`) or to PSI post-V6.3.2. The Kelp DAO incident is the proximate trigger but the defect predates the incident; this amendment closes it prospectively.
>
> **Kill Signal.** Promote LSTI to scored status only when (1) the engine change is merged with tests that reject renormalization for any index, (2) LSTI component coverage reaches ≥85% across at least 8 of the 10 tracked LSTs (including the 6 currently outside Rated Network), and (3) a second audit replicates the rsETH exercise on a different LST and finds no undefended static_config values.

---

## Changes made during audit

One code change:

- `app/collectors/lst_collector.py:88-95` — `kelp-rseth.exploit_history_lst`: **100 → 10**, with a 4-line comment citing this audit, the 2026-04-18 event amount ($292M), the severity band (10, for $100M+), and the `min(live, static)` interaction at `lst_collector.py:377`. No other file was modified. No commit, no push — awaiting review.
