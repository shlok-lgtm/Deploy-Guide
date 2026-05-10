# Entity Count Drift: Canon vs Reality (2026-05-09)

**Comparison baseline:** V9.4 constitution amendment (the most recent canon)

## PSI — Protocol Safety Index

Canon (V9.4): 13 protocols
Live (codebase): 13 hardcoded + auto-promotion from backlog
Dashboard: 15

| Protocol | Status | Notes |
|----------|--------|-------|
| aave | IN_CANON | V8 original |
| lido | IN_CANON | V8 original |
| eigenlayer | IN_CANON | V8 original |
| sky | IN_CANON | V8 original (was MakerDAO) |
| compound-finance | IN_CANON | V8 original |
| uniswap | IN_CANON | V8 original |
| curve-finance | IN_CANON | V8 original |
| morpho | IN_CANON | V8 original |
| spark | IN_CANON | V8 original |
| convex-finance | IN_CANON | V8 original |
| drift | IN_CANON | V9.4 expansion |
| jupiter-perpetual-exchange | IN_CANON | V9.4 expansion |
| raydium | IN_CANON | V9.4 expansion |
| *(2 unknown)* | ADDED_POST_V9.4 | Auto-promoted from backlog; need DB query to identify slugs |

**Action:** Run `SELECT DISTINCT protocol_slug FROM psi_scores ORDER BY protocol_slug` to identify the 2 promoted protocols. Amend V9.13 to canonize them if legitimate.

## VSRI — Vault/Strategy Risk Index

Canon (V9.4): 7 entities
Live (codebase): 10 hardcoded in `app/index_definitions/vsri_v01.py:248`
Dashboard: 10

| Entity | Status | Notes |
|--------|--------|-------|
| yearn-usdc | IN_CANON | V9.2 original |
| yearn-dai | IN_CANON | V9.2 original |
| yearn-eth | IN_CANON | V9.2 original |
| morpho-usdc-aave | IN_CANON | V9.2 original |
| morpho-eth-aave | IN_CANON | V9.2 original |
| beefy-usdc-eth | IN_CANON | V9.2 original |
| beefy-usdt-usdc | IN_CANON | V9.2 original |
| pendle-steth-dec25 | ADDED_POST_V9.4 | Pendle stETH vault |
| pendle-eeth-dec25 | ADDED_POST_V9.4 | Pendle eETH vault |
| sommelier-turbo-steth | ADDED_POST_V9.4 | Sommelier Turbo stETH |

**Action:** Amend V9.13 to canonize pendle-steth-dec25, pendle-eeth-dec25, sommelier-turbo-steth.

## TTI — Tokenized Treasury Index

Canon (V9.4): 5 entities
Live (codebase): 10 hardcoded in `app/index_definitions/tti_v01.py:590`
Dashboard: 10

| Entity | Status | Notes |
|--------|--------|-------|
| ondo-ousg | IN_CANON | V9.2 original |
| ondo-usdy | IN_CANON | V9.2 original |
| blackrock-buidl | IN_CANON | V9.2 original |
| franklin-benji | IN_CANON | V9.2 original |
| mountain-usdm | IN_CANON | V9.2 original |
| backed-bib01 | ADDED_POST_V9.4 | Backed Finance bIB01 |
| maple-cash | ADDED_POST_V9.4 | Maple Cash Management |
| centrifuge-pools | ADDED_POST_V9.4 | Centrifuge Pools |
| superstate-ustb | ADDED_POST_V9.4 | Superstate USTB |
| openeden-tbill | ADDED_POST_V9.4 | OpenEden TBILL |

**Action:** Amend V9.13 to canonize all 5 additions.

## Summary

| Index | Canon | Live | Drift | Additions needing amendment |
|-------|-------|------|-------|-----------------------------|
| PSI | 13 | 15 | +2 | 2 (need DB query to identify) |
| VSRI | 7 | 10 | +3 | pendle-steth-dec25, pendle-eeth-dec25, sommelier-turbo-steth |
| TTI | 5 | 10 | +5 | backed-bib01, maple-cash, centrifuge-pools, superstate-ustb, openeden-tbill |

**Total entities needing V9.13 amendment:** 10 (2 PSI + 3 VSRI + 5 TTI)
**No stale/removed entities found** — all canon entities are still live in codebase.
