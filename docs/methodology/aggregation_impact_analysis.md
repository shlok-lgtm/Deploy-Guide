# Aggregation Formula Impact Analysis

**Generated:** 2026-04-21T17:37:36.504673+00:00
**Source:** `scripts/analyze_aggregation_impact.py`
**Formulas evaluated:** coverage_weighted, coverage_withheld, legacy_renormalize, legacy_sii_v1, strict_neutral, strict_zero
**Withheld thresholds:** [0.6, 0.65, 0.7, 0.75, 0.8, 0.85]

This report compares every current score under every registered aggregation
formula and candidate threshold. It is the decision artifact for each index's
migration PR. No index should migrate without citing a specific row in this
report.

---

## Section A — Per-index coverage distribution

| Index | n | min | 25th | median | 75th | max |
|---|---|---|---|---|---|---|
| sii | 36 | 0.39 | 0.48 | 0.50 | 0.61 | 0.62 |
| psi | 13 | 0.67 | 0.78 | 0.81 | 0.85 | 0.85 |
| rpi | 13 | 0.40 | 0.40 | 0.40 | 0.60 | 0.80 |
| lsti | 10 | 0.48 | 0.62 | 0.62 | 0.62 | 0.62 |
| bri | 9 | 0.83 | 0.83 | 0.83 | 0.83 | 0.83 |
| dohi | 12 | 0.04 | 0.27 | 0.36 | 0.88 | 0.88 |
| vsri | 9 | 0.18 | 0.75 | 0.82 | 0.86 | 0.89 |
| cxri | 8 | 0.69 | 0.76 | 0.76 | 0.76 | 0.76 |
| tti | 10 | 0.04 | 0.05 | 0.41 | 0.72 | 0.78 |

## Section B — Per-index delta tables

### sii
| entity | legacy | cw@0.0 | cwh@0.6 | cwh@0.65 | cwh@0.7 | cwh@0.75 | cwh@0.8 | cwh@0.85 |
|---|---|---|---|---|---|---|---|---|
| USDTB | 63.14 | 69.05 | withheld | withheld | withheld | withheld | withheld | withheld |
| USDT | 89.44 | 90.44 | 90.44 | withheld | withheld | withheld | withheld | withheld |
| USDS | 81.83 | 86.00 | withheld | withheld | withheld | withheld | withheld | withheld |
| USDP | 70.44 | 79.01 | withheld | withheld | withheld | withheld | withheld | withheld |
| USDe | 73.22 | 73.20 | 73.20 | withheld | withheld | withheld | withheld | withheld |
| USDD (BTTC bridge) | 35.20 | 33.68 | withheld | withheld | withheld | withheld | withheld | withheld |
| USDD | 71.52 | 75.45 | 75.45 | withheld | withheld | withheld | withheld | withheld |
| USDC | 93.36 | 94.60 | 94.60 | withheld | withheld | withheld | withheld | withheld |
| USD1 | 83.68 | 86.80 | 86.80 | withheld | withheld | withheld | withheld | withheld |
| TUSD | 72.85 | 75.89 | 75.89 | withheld | withheld | withheld | withheld | withheld |
| SUSDS | 54.78 | 53.66 | withheld | withheld | withheld | withheld | withheld | withheld |
| SUSDE | 52.93 | 49.53 | withheld | withheld | withheld | withheld | withheld | withheld |
| sUSD | 38.09 | 36.49 | withheld | withheld | withheld | withheld | withheld | withheld |
| STKGHO | 45.99 | 48.88 | withheld | withheld | withheld | withheld | withheld | withheld |
| STEAKUSDC | 42.52 | 40.76 | withheld | withheld | withheld | withheld | withheld | withheld |
| SDOLA | 37.71 | 38.23 | withheld | withheld | withheld | withheld | withheld | withheld |
| RLUSD | 75.39 | 82.47 | withheld | withheld | withheld | withheld | withheld | withheld |
| RAI | 35.58 | 32.87 | withheld | withheld | withheld | withheld | withheld | withheld |
| PYUSD | 79.37 | 81.92 | 81.92 | withheld | withheld | withheld | withheld | withheld |
| OUSD | 56.99 | 64.01 | withheld | withheld | withheld | withheld | withheld | withheld |
| MUSD | 64.77 | 74.87 | withheld | withheld | withheld | withheld | withheld | withheld |
| MIM | 60.33 | 68.36 | withheld | withheld | withheld | withheld | withheld | withheld |
| LUSD | 56.28 | 60.09 | withheld | withheld | withheld | withheld | withheld | withheld |
| GUSD | 58.34 | 67.11 | withheld | withheld | withheld | withheld | withheld | withheld |
| GHO | 74.60 | 79.69 | withheld | withheld | withheld | withheld | withheld | withheld |
| FRAX | 37.02 | 34.05 | withheld | withheld | withheld | withheld | withheld | withheld |
| FRAX | 65.84 | 67.12 | 67.12 | withheld | withheld | withheld | withheld | withheld |
| FDUSD | 74.01 | 76.25 | 76.25 | withheld | withheld | withheld | withheld | withheld |
| EURS | 33.09 | 24.92 | withheld | withheld | withheld | withheld | withheld | withheld |
| EURI | 46.98 | 48.23 | withheld | withheld | withheld | withheld | withheld | withheld |
| EURE | 42.33 | 43.54 | withheld | withheld | withheld | withheld | withheld | withheld |
| EURC | 55.84 | 54.63 | withheld | withheld | withheld | withheld | withheld | withheld |
| DOLA | 63.11 | 69.06 | withheld | withheld | withheld | withheld | withheld | withheld |
| DAI | 84.52 | 85.77 | 85.77 | withheld | withheld | withheld | withheld | withheld |
| crvUSD | 75.03 | 77.02 | withheld | withheld | withheld | withheld | withheld | withheld |
| BUSD0 | 42.11 | 41.63 | withheld | withheld | withheld | withheld | withheld | withheld |

### psi
| entity | legacy | cw@0.0 | cwh@0.6 | cwh@0.65 | cwh@0.7 | cwh@0.75 | cwh@0.8 | cwh@0.85 |
|---|---|---|---|---|---|---|---|---|
| aave | 84.32 | 83.58 | 83.58 | 83.58 | 83.58 | 83.58 | 83.58 | 83.58 |
| compound-finance | 62.42 | 63.06 | 63.06 | 63.06 | 63.06 | 63.06 | 63.06 | 63.06 |
| convex-finance | 63.38 | 60.82 | 60.82 | 60.82 | 60.82 | 60.82 | 60.82 | 60.82 |
| curve-finance | 53.34 | 55.65 | 55.65 | 55.65 | 55.65 | 55.65 | 55.65 | 55.65 |
| drift | 48.88 | 49.28 | 49.28 | 49.28 | 49.28 | 49.28 | withheld | withheld |
| eigenlayer | 60.36 | 58.64 | 58.64 | 58.64 | 58.64 | 58.64 | withheld | withheld |
| jupiter-perpetual-exchange | 60.02 | 62.42 | 62.42 | 62.42 | withheld | withheld | withheld | withheld |
| lido | 80.34 | 78.34 | 78.34 | 78.34 | 78.34 | 78.34 | 78.34 | withheld |
| morpho | 75.24 | 73.48 | 73.48 | 73.48 | 73.48 | 73.48 | 73.48 | 73.48 |
| raydium | 56.73 | 56.64 | 56.64 | 56.64 | 56.64 | 56.64 | withheld | withheld |
| sky | 73.00 | 71.55 | 71.55 | 71.55 | 71.55 | 71.55 | 71.55 | withheld |
| spark | 67.64 | 67.36 | 67.36 | 67.36 | 67.36 | withheld | withheld | withheld |
| uniswap | 75.72 | 77.22 | 77.22 | 77.22 | 77.22 | 77.22 | 77.22 | withheld |

### rpi
| entity | legacy | cw@0.0 | cwh@0.6 | cwh@0.65 | cwh@0.7 | cwh@0.75 | cwh@0.8 | cwh@0.85 |
|---|---|---|---|---|---|---|---|---|
| aave | 50.00 | 61.54 | 61.54 | withheld | withheld | withheld | withheld | withheld |
| compound-finance | 33.33 | 44.44 | withheld | withheld | withheld | withheld | withheld | withheld |
| convex-finance | 50.00 | 61.54 | 61.54 | withheld | withheld | withheld | withheld | withheld |
| curve-finance | 33.33 | 44.44 | withheld | withheld | withheld | withheld | withheld | withheld |
| drift | 33.33 | 44.44 | withheld | withheld | withheld | withheld | withheld | withheld |
| eigenlayer | 33.33 | 44.44 | withheld | withheld | withheld | withheld | withheld | withheld |
| jupiter-perpetual-exchange | 33.33 | 44.44 | withheld | withheld | withheld | withheld | withheld | withheld |
| lido | 33.33 | 44.44 | withheld | withheld | withheld | withheld | withheld | withheld |
| morpho | 40.20 | 47.30 | 47.30 | 47.30 | 47.30 | 47.30 | 47.30 | withheld |
| raydium | 33.33 | 44.44 | withheld | withheld | withheld | withheld | withheld | withheld |
| sky | 33.33 | 44.44 | withheld | withheld | withheld | withheld | withheld | withheld |
| spark | 33.33 | 44.44 | withheld | withheld | withheld | withheld | withheld | withheld |
| uniswap | 50.00 | 61.54 | 61.54 | withheld | withheld | withheld | withheld | withheld |

### lsti
| entity | legacy | cw@0.0 | cwh@0.6 | cwh@0.65 | cwh@0.7 | cwh@0.75 | cwh@0.8 | cwh@0.85 |
|---|---|---|---|---|---|---|---|---|
| coinbase-cbeth | 43.12 | 40.72 | withheld | withheld | withheld | withheld | withheld | withheld |
| etherfi-eeth | 54.71 | 60.22 | 60.22 | withheld | withheld | withheld | withheld | withheld |
| etherfi-weeth | 52.11 | 50.74 | 50.74 | withheld | withheld | withheld | withheld | withheld |
| frax-sfrxeth | 35.42 | 34.51 | 34.51 | withheld | withheld | withheld | withheld | withheld |
| kelp-rseth | 35.75 | 33.23 | 33.23 | withheld | withheld | withheld | withheld | withheld |
| lido-steth | 64.85 | 62.85 | 62.85 | withheld | withheld | withheld | withheld | withheld |
| lido-wsteth | 55.78 | 50.58 | 50.58 | withheld | withheld | withheld | withheld | withheld |
| mantle-meth | 40.95 | 42.66 | 42.66 | withheld | withheld | withheld | withheld | withheld |
| rocket-pool-reth | 54.03 | 49.47 | 49.47 | withheld | withheld | withheld | withheld | withheld |
| swell-sweth | 32.87 | 34.83 | 34.83 | withheld | withheld | withheld | withheld | withheld |

### bri
| entity | legacy | cw@0.0 | cwh@0.6 | cwh@0.65 | cwh@0.7 | cwh@0.75 | cwh@0.8 | cwh@0.85 |
|---|---|---|---|---|---|---|---|---|
| across | 72.05 | 68.87 | 68.87 | 68.87 | 68.87 | 68.87 | 68.87 | withheld |
| axelar | 79.67 | 78.79 | 78.79 | 78.79 | 78.79 | 78.79 | 78.79 | withheld |
| celer-cbridge | 68.58 | 65.37 | 65.37 | 65.37 | 65.37 | 65.37 | 65.37 | withheld |
| circle-cctp | 54.04 | 58.56 | 58.56 | 58.56 | 58.56 | 58.56 | 58.56 | withheld |
| debridge | 71.57 | 68.42 | 68.42 | 68.42 | 68.42 | 68.42 | 68.42 | withheld |
| layerzero | 80.73 | 75.57 | 75.57 | 75.57 | 75.57 | 75.57 | 75.57 | withheld |
| stargate | 69.98 | 66.53 | 66.53 | 66.53 | 66.53 | 66.53 | 66.53 | withheld |
| synapse | 65.14 | 60.82 | 60.82 | 60.82 | 60.82 | 60.82 | 60.82 | withheld |
| wormhole | 76.38 | 71.32 | 71.32 | 71.32 | 71.32 | 71.32 | 71.32 | withheld |

### dohi
| entity | legacy | cw@0.0 | cwh@0.6 | cwh@0.65 | cwh@0.7 | cwh@0.75 | cwh@0.8 | cwh@0.85 |
|---|---|---|---|---|---|---|---|---|
| aave-dao | 75.20 | 77.32 | 77.32 | 77.32 | 77.32 | 77.32 | 77.32 | 77.32 |
| arbitrum-dao | 75.23 | 73.15 | 73.15 | 73.15 | 73.15 | 73.15 | 73.15 | withheld |
| compound-dao | 57.26 | 58.83 | 58.83 | 58.83 | 58.83 | 58.83 | 58.83 | 58.83 |
| convex-dao | 65.53 | 61.67 | withheld | withheld | withheld | withheld | withheld | withheld |
| curve-dao | 31.06 | 50.17 | withheld | withheld | withheld | withheld | withheld | withheld |
| ens-dao | 47.77 | 54.41 | withheld | withheld | withheld | withheld | withheld | withheld |
| gitcoin-dao | 76.35 | 76.69 | withheld | withheld | withheld | withheld | withheld | withheld |
| lido-dao | 69.28 | 71.10 | 71.10 | 71.10 | 71.10 | 71.10 | 71.10 | 71.10 |
| maker-dao | 74.97 | 70.95 | withheld | withheld | withheld | withheld | withheld | withheld |
| optimism-dao | 0.00 | 0.00 | withheld | withheld | withheld | withheld | withheld | withheld |
| safe-dao | 48.29 | 56.85 | withheld | withheld | withheld | withheld | withheld | withheld |
| uniswap-dao | 60.54 | 61.83 | 61.83 | 61.83 | 61.83 | 61.83 | 61.83 | 61.83 |

### vsri
| entity | legacy | cw@0.0 | cwh@0.6 | cwh@0.65 | cwh@0.7 | cwh@0.75 | cwh@0.8 | cwh@0.85 |
|---|---|---|---|---|---|---|---|---|
| beefy-usdc-eth | 69.31 | 73.47 | 73.47 | 73.47 | 73.47 | 73.47 | withheld | withheld |
| beefy-usdt-usdc | 79.95 | 77.04 | 77.04 | 77.04 | 77.04 | 77.04 | withheld | withheld |
| morpho-eth-aave | 65.26 | 68.47 | 68.47 | 68.47 | 68.47 | 68.47 | 68.47 | 68.47 |
| morpho-usdc-aave | 71.19 | 72.85 | 72.85 | 72.85 | 72.85 | 72.85 | 72.85 | 72.85 |
| pendle-eeth-dec25 | 57.22 | 46.41 | withheld | withheld | withheld | withheld | withheld | withheld |
| pendle-steth-dec25 | 57.22 | 46.41 | withheld | withheld | withheld | withheld | withheld | withheld |
| yearn-dai | 67.91 | 70.50 | 70.50 | 70.50 | 70.50 | 70.50 | 70.50 | 70.50 |
| yearn-eth | 64.18 | 68.40 | 68.40 | 68.40 | 68.40 | 68.40 | 68.40 | withheld |
| yearn-usdc | 69.54 | 71.94 | 71.94 | 71.94 | 71.94 | 71.94 | 71.94 | 71.94 |

### cxri
| entity | legacy | cw@0.0 | cwh@0.6 | cwh@0.65 | cwh@0.7 | cwh@0.75 | cwh@0.8 | cwh@0.85 |
|---|---|---|---|---|---|---|---|---|
| binance | 73.23 | 67.95 | 67.95 | 67.95 | 67.95 | 67.95 | withheld | withheld |
| bitget | 63.86 | 57.61 | 57.61 | 57.61 | 57.61 | 57.61 | withheld | withheld |
| bybit | 60.55 | 60.55 | 60.55 | 60.55 | withheld | withheld | withheld | withheld |
| coinbase | 86.71 | 87.29 | 87.29 | 87.29 | 87.29 | 87.29 | withheld | withheld |
| gate-io | 63.04 | 56.54 | 56.54 | 56.54 | 56.54 | 56.54 | withheld | withheld |
| kraken | 79.85 | 80.19 | 80.19 | 80.19 | 80.19 | 80.19 | withheld | withheld |
| kucoin | 62.95 | 58.51 | 58.51 | 58.51 | 58.51 | 58.51 | withheld | withheld |
| okx | 71.54 | 71.54 | 71.54 | 71.54 | withheld | withheld | withheld | withheld |

### tti
| entity | legacy | cw@0.0 | cwh@0.6 | cwh@0.65 | cwh@0.7 | cwh@0.75 | cwh@0.8 | cwh@0.85 |
|---|---|---|---|---|---|---|---|---|
| backed-bib01 | 77.78 | 74.04 | withheld | withheld | withheld | withheld | withheld | withheld |
| blackrock-buidl | 82.99 | 82.89 | 82.89 | 82.89 | 82.89 | withheld | withheld | withheld |
| centrifuge-pools | 72.78 | 60.94 | withheld | withheld | withheld | withheld | withheld | withheld |
| franklin-benji | 76.90 | 77.09 | 77.09 | 77.09 | 77.09 | withheld | withheld | withheld |
| maple-cash | 42.15 | 35.03 | withheld | withheld | withheld | withheld | withheld | withheld |
| mountain-usdm | 56.21 | 54.88 | 54.88 | 54.88 | 54.88 | 54.88 | withheld | withheld |
| ondo-ousg | 72.38 | 71.49 | 71.49 | 71.49 | 71.49 | 71.49 | withheld | withheld |
| ondo-usdy | 70.19 | 69.32 | 69.32 | 69.32 | 69.32 | withheld | withheld | withheld |
| openeden-tbill | 76.67 | 73.16 | withheld | withheld | withheld | withheld | withheld | withheld |
| superstate-ustb | 85.77 | 82.65 | withheld | withheld | withheld | withheld | withheld | withheld |


## Section C — CQI matrix shift

| asset | protocol | legacy CQI | shift under PSI migrations |
|---|---|---|---|
| USDTB | aave | 76.19 | psi=legacy_renormalize:76.19(+0.00); psi=coverage_weighted:75.85(-0.34); psi=coverage_withheld@0.6:75.85(-0.34) |
| USDTB | compound-finance | 65.55 | psi=legacy_renormalize:65.55(+0.00); psi=coverage_weighted:65.89(+0.34); psi=coverage_withheld@0.6:65.89(+0.34) |
| USDTB | convex-finance | 66.05 | psi=legacy_renormalize:66.05(+0.00); psi=coverage_weighted:64.71(-1.34); psi=coverage_withheld@0.6:64.71(-1.34) |
| USDTB | curve-finance | 60.60 | psi=legacy_renormalize:60.60(+0.00); psi=coverage_weighted:61.89(+1.29); psi=coverage_withheld@0.6:61.89(+1.29) |
| USDTB | drift | 58.01 | psi=legacy_renormalize:58.01(+0.00); psi=coverage_weighted:58.24(+0.23); psi=coverage_withheld@0.6:58.24(+0.23) |
| USDTB | eigenlayer | 64.46 | psi=legacy_renormalize:64.46(+0.00); psi=coverage_weighted:63.54(-0.92); psi=coverage_withheld@0.6:63.54(-0.92) |
| USDTB | jupiter-perpetual-exchange | 64.28 | psi=legacy_renormalize:64.28(+0.00); psi=coverage_weighted:65.55(+1.27); psi=coverage_withheld@0.6:65.55(+1.27) |
| USDTB | lido | 74.37 | psi=legacy_renormalize:74.37(+0.00); psi=coverage_weighted:73.44(-0.93); psi=coverage_withheld@0.6:73.44(-0.93) |
| USDTB | morpho | 71.97 | psi=legacy_renormalize:71.97(+0.00); psi=coverage_weighted:71.12(-0.85); psi=coverage_withheld@0.6:71.12(-0.85) |
| USDTB | raydium | 62.49 | psi=legacy_renormalize:62.49(+0.00); psi=coverage_weighted:62.44(-0.05); psi=coverage_withheld@0.6:62.44(-0.05) |
| USDTB | sky | 70.89 | psi=legacy_renormalize:70.89(+0.00); psi=coverage_weighted:70.18(-0.71); psi=coverage_withheld@0.6:70.18(-0.71) |
| USDTB | spark | 68.24 | psi=legacy_renormalize:68.24(+0.00); psi=coverage_weighted:68.10(-0.14); psi=coverage_withheld@0.6:68.10(-0.14) |
| USDTB | uniswap | 72.20 | psi=legacy_renormalize:72.20(+0.00); psi=coverage_weighted:72.91(+0.71); psi=coverage_withheld@0.6:72.91(+0.71) |
| USDT | aave | 85.11 | psi=legacy_renormalize:85.11(+0.00); psi=coverage_weighted:84.74(-0.37); psi=coverage_withheld@0.6:84.74(-0.37) |
| USDT | compound-finance | 73.23 | psi=legacy_renormalize:73.23(+0.00); psi=coverage_weighted:73.60(+0.37); psi=coverage_withheld@0.6:73.60(+0.37) |
| USDT | convex-finance | 73.79 | psi=legacy_renormalize:73.79(+0.00); psi=coverage_weighted:72.28(-1.51); psi=coverage_withheld@0.6:72.28(-1.51) |
| USDT | curve-finance | 67.69 | psi=legacy_renormalize:67.69(+0.00); psi=coverage_weighted:69.14(+1.45); psi=coverage_withheld@0.6:69.14(+1.45) |
| USDT | drift | 64.80 | psi=legacy_renormalize:64.80(+0.00); psi=coverage_weighted:65.07(+0.27); psi=coverage_withheld@0.6:65.07(+0.27) |
| USDT | eigenlayer | 72.01 | psi=legacy_renormalize:72.01(+0.00); psi=coverage_weighted:70.98(-1.03); psi=coverage_withheld@0.6:70.98(-1.03) |
| USDT | jupiter-perpetual-exchange | 71.81 | psi=legacy_renormalize:71.81(+0.00); psi=coverage_weighted:73.23(+1.42); psi=coverage_withheld@0.6:73.23(+1.42) |
| USDT | lido | 83.08 | psi=legacy_renormalize:83.08(+0.00); psi=coverage_weighted:82.04(-1.04); psi=coverage_withheld@0.6:82.04(-1.04) |
| USDT | morpho | 80.40 | psi=legacy_renormalize:80.40(+0.00); psi=coverage_weighted:79.45(-0.95); psi=coverage_withheld@0.6:79.45(-0.95) |
| USDT | raydium | 69.81 | psi=legacy_renormalize:69.81(+0.00); psi=coverage_weighted:69.76(-0.05); psi=coverage_withheld@0.6:69.76(-0.05) |
| USDT | sky | 79.19 | psi=legacy_renormalize:79.19(+0.00); psi=coverage_weighted:78.40(-0.79); psi=coverage_withheld@0.6:78.40(-0.79) |
| USDT | spark | 76.23 | psi=legacy_renormalize:76.23(+0.00); psi=coverage_weighted:76.07(-0.16); psi=coverage_withheld@0.6:76.07(-0.16) |
| USDT | uniswap | 80.65 | psi=legacy_renormalize:80.65(+0.00); psi=coverage_weighted:81.45(+0.80); psi=coverage_withheld@0.6:81.45(+0.80) |
| USDS | aave | 81.69 | psi=legacy_renormalize:81.69(+0.00); psi=coverage_weighted:81.33(-0.36); psi=coverage_withheld@0.6:81.33(-0.36) |
| USDS | compound-finance | 70.29 | psi=legacy_renormalize:70.29(+0.00); psi=coverage_weighted:70.65(+0.36); psi=coverage_withheld@0.6:70.65(+0.36) |
| USDS | convex-finance | 70.83 | psi=legacy_renormalize:70.83(+0.00); psi=coverage_weighted:69.38(-1.45); psi=coverage_withheld@0.6:69.38(-1.45) |
| USDS | curve-finance | 64.98 | psi=legacy_renormalize:64.98(+0.00); psi=coverage_weighted:66.37(+1.39); psi=coverage_withheld@0.6:66.37(+1.39) |
| USDS | drift | 62.20 | psi=legacy_renormalize:62.20(+0.00); psi=coverage_weighted:62.45(+0.25); psi=coverage_withheld@0.6:62.45(+0.25) |
| USDS | eigenlayer | 69.12 | psi=legacy_renormalize:69.12(+0.00); psi=coverage_weighted:68.13(-0.99); psi=coverage_withheld@0.6:68.13(-0.99) |
| USDS | jupiter-perpetual-exchange | 68.92 | psi=legacy_renormalize:68.92(+0.00); psi=coverage_weighted:70.29(+1.37); psi=coverage_withheld@0.6:70.29(+1.37) |
| USDS | lido | 79.74 | psi=legacy_renormalize:79.74(+0.00); psi=coverage_weighted:78.74(-1.00); psi=coverage_withheld@0.6:78.74(-1.00) |
| USDS | morpho | 77.17 | psi=legacy_renormalize:77.17(+0.00); psi=coverage_weighted:76.26(-0.91); psi=coverage_withheld@0.6:76.26(-0.91) |
| USDS | raydium | 67.01 | psi=legacy_renormalize:67.01(+0.00); psi=coverage_weighted:66.96(-0.05); psi=coverage_withheld@0.6:66.96(-0.05) |
| USDS | sky | 76.01 | psi=legacy_renormalize:76.01(+0.00); psi=coverage_weighted:75.25(-0.76); psi=coverage_withheld@0.6:75.25(-0.76) |
| USDS | spark | 73.17 | psi=legacy_renormalize:73.17(+0.00); psi=coverage_weighted:73.02(-0.15); psi=coverage_withheld@0.6:73.02(-0.15) |
| USDS | uniswap | 77.42 | psi=legacy_renormalize:77.42(+0.00); psi=coverage_weighted:78.18(+0.76); psi=coverage_withheld@0.6:78.18(+0.76) |
| USDP | aave | 80.35 | psi=legacy_renormalize:80.35(+0.00); psi=coverage_weighted:80.00(-0.35); psi=coverage_withheld@0.6:80.00(-0.35) |
| USDP | compound-finance | 69.13 | psi=legacy_renormalize:69.13(+0.00); psi=coverage_weighted:69.49(+0.36); psi=coverage_withheld@0.6:69.49(+0.36) |
| USDP | convex-finance | 69.66 | psi=legacy_renormalize:69.66(+0.00); psi=coverage_weighted:68.24(-1.42); psi=coverage_withheld@0.6:68.24(-1.42) |
| USDP | curve-finance | 63.91 | psi=legacy_renormalize:63.91(+0.00); psi=coverage_weighted:65.28(+1.37); psi=coverage_withheld@0.6:65.28(+1.37) |
| USDP | drift | 61.18 | psi=legacy_renormalize:61.18(+0.00); psi=coverage_weighted:61.43(+0.25); psi=coverage_withheld@0.6:61.43(+0.25) |
| USDP | eigenlayer | 67.98 | psi=legacy_renormalize:67.98(+0.00); psi=coverage_weighted:67.01(-0.97); psi=coverage_withheld@0.6:67.01(-0.97) |
| USDP | jupiter-perpetual-exchange | 67.79 | psi=legacy_renormalize:67.79(+0.00); psi=coverage_weighted:69.13(+1.34); psi=coverage_withheld@0.6:69.13(+1.34) |
| USDP | lido | 78.43 | psi=legacy_renormalize:78.43(+0.00); psi=coverage_weighted:77.45(-0.98); psi=coverage_withheld@0.6:77.45(-0.98) |
| USDP | morpho | 75.90 | psi=legacy_renormalize:75.90(+0.00); psi=coverage_weighted:75.01(-0.89); psi=coverage_withheld@0.6:75.01(-0.89) |
| USDP | raydium | 65.91 | psi=legacy_renormalize:65.91(+0.00); psi=coverage_weighted:65.86(-0.05); psi=coverage_withheld@0.6:65.86(-0.05) |
| USDP | sky | 74.76 | psi=legacy_renormalize:74.76(+0.00); psi=coverage_weighted:74.02(-0.74); psi=coverage_withheld@0.6:74.02(-0.74) |
| USDP | spark | 71.97 | psi=legacy_renormalize:71.97(+0.00); psi=coverage_weighted:71.82(-0.15); psi=coverage_withheld@0.6:71.82(-0.15) |
| USDP | uniswap | 76.14 | psi=legacy_renormalize:76.14(+0.00); psi=coverage_weighted:76.89(+0.75); psi=coverage_withheld@0.6:76.89(+0.75) |
| USDe | aave | 81.24 | psi=legacy_renormalize:81.24(+0.00); psi=coverage_weighted:80.89(-0.35); psi=coverage_withheld@0.6:80.89(-0.35) |
| USDe | compound-finance | 69.90 | psi=legacy_renormalize:69.90(+0.00); psi=coverage_weighted:70.26(+0.36); psi=coverage_withheld@0.6:70.26(+0.36) |
| USDe | convex-finance | 70.44 | psi=legacy_renormalize:70.44(+0.00); psi=coverage_weighted:69.00(-1.44); psi=coverage_withheld@0.6:69.00(-1.44) |
| USDe | curve-finance | 64.62 | psi=legacy_renormalize:64.62(+0.00); psi=coverage_weighted:66.00(+1.38); psi=coverage_withheld@0.6:66.00(+1.38) |
| USDe | drift | 61.86 | psi=legacy_renormalize:61.86(+0.00); psi=coverage_weighted:62.11(+0.25); psi=coverage_withheld@0.6:62.11(+0.25) |
| USDe | eigenlayer | 68.74 | psi=legacy_renormalize:68.74(+0.00); psi=coverage_weighted:67.75(-0.99); psi=coverage_withheld@0.6:67.75(-0.99) |
| USDe | jupiter-perpetual-exchange | 68.54 | psi=legacy_renormalize:68.54(+0.00); psi=coverage_weighted:69.90(+1.36); psi=coverage_withheld@0.6:69.90(+1.36) |
| USDe | lido | 79.30 | psi=legacy_renormalize:79.30(+0.00); psi=coverage_weighted:78.31(-0.99); psi=coverage_withheld@0.6:78.31(-0.99) |
| USDe | morpho | 76.74 | psi=legacy_renormalize:76.74(+0.00); psi=coverage_weighted:75.84(-0.90); psi=coverage_withheld@0.6:75.84(-0.90) |
| USDe | raydium | 66.64 | psi=legacy_renormalize:66.64(+0.00); psi=coverage_weighted:66.59(-0.05); psi=coverage_withheld@0.6:66.59(-0.05) |
| USDe | sky | 75.59 | psi=legacy_renormalize:75.59(+0.00); psi=coverage_weighted:74.84(-0.75); psi=coverage_withheld@0.6:74.84(-0.75) |
| USDe | spark | 72.77 | psi=legacy_renormalize:72.77(+0.00); psi=coverage_weighted:72.62(-0.15); psi=coverage_withheld@0.6:72.62(-0.15) |
| USDe | uniswap | 76.99 | psi=legacy_renormalize:76.99(+0.00); psi=coverage_weighted:77.75(+0.76); psi=coverage_withheld@0.6:77.75(+0.76) |
| USDD (BTTC bridge) | aave | 59.27 | psi=legacy_renormalize:59.27(+0.00); psi=coverage_weighted:59.01(-0.26); psi=coverage_withheld@0.6:59.01(-0.26) |
| USDD (BTTC bridge) | compound-finance | 50.99 | psi=legacy_renormalize:50.99(+0.00); psi=coverage_weighted:51.26(+0.27); psi=coverage_withheld@0.6:51.26(+0.27) |
| USDD (BTTC bridge) | convex-finance | 51.38 | psi=legacy_renormalize:51.38(+0.00); psi=coverage_weighted:50.34(-1.04); psi=coverage_withheld@0.6:50.34(-1.04) |
| USDD (BTTC bridge) | curve-finance | 47.14 | psi=legacy_renormalize:47.14(+0.00); psi=coverage_weighted:48.15(+1.01); psi=coverage_withheld@0.6:48.15(+1.01) |
| USDD (BTTC bridge) | drift | 45.13 | psi=legacy_renormalize:45.13(+0.00); psi=coverage_weighted:45.31(+0.18); psi=coverage_withheld@0.6:45.31(+0.18) |
| USDD (BTTC bridge) | eigenlayer | 50.15 | psi=legacy_renormalize:50.15(+0.00); psi=coverage_weighted:49.43(-0.72); psi=coverage_withheld@0.6:49.43(-0.72) |
| USDD (BTTC bridge) | jupiter-perpetual-exchange | 50.00 | psi=legacy_renormalize:50.00(+0.00); psi=coverage_weighted:50.99(+0.99); psi=coverage_withheld@0.6:50.99(+0.99) |
| USDD (BTTC bridge) | lido | 57.85 | psi=legacy_renormalize:57.85(+0.00); psi=coverage_weighted:57.13(-0.72); psi=coverage_withheld@0.6:57.13(-0.72) |
| USDD (BTTC bridge) | morpho | 55.99 | psi=legacy_renormalize:55.99(+0.00); psi=coverage_weighted:55.33(-0.66); psi=coverage_withheld@0.6:55.33(-0.66) |
| USDD (BTTC bridge) | raydium | 48.61 | psi=legacy_renormalize:48.61(+0.00); psi=coverage_weighted:48.58(-0.03); psi=coverage_withheld@0.6:48.58(-0.03) |
| USDD (BTTC bridge) | sky | 55.15 | psi=legacy_renormalize:55.15(+0.00); psi=coverage_weighted:54.60(-0.55); psi=coverage_withheld@0.6:54.60(-0.55) |
| USDD (BTTC bridge) | spark | 53.08 | psi=legacy_renormalize:53.08(+0.00); psi=coverage_weighted:52.97(-0.11); psi=coverage_withheld@0.6:52.97(-0.11) |
| USDD (BTTC bridge) | uniswap | 56.16 | psi=legacy_renormalize:56.16(+0.00); psi=coverage_weighted:56.72(+0.56); psi=coverage_withheld@0.6:56.72(+0.56) |
| USDD | aave | 77.80 | psi=legacy_renormalize:77.80(+0.00); psi=coverage_weighted:77.46(-0.34); psi=coverage_withheld@0.6:77.46(-0.34) |
| USDD | compound-finance | 66.94 | psi=legacy_renormalize:66.94(+0.00); psi=coverage_weighted:67.28(+0.34); psi=coverage_withheld@0.6:67.28(+0.34) |
| USDD | convex-finance | 67.45 | psi=legacy_renormalize:67.45(+0.00); psi=coverage_weighted:66.07(-1.38); psi=coverage_withheld@0.6:66.07(-1.38) |
| USDD | curve-finance | 61.88 | psi=legacy_renormalize:61.88(+0.00); psi=coverage_weighted:63.20(+1.32); psi=coverage_withheld@0.6:63.20(+1.32) |
| USDD | drift | 59.23 | psi=legacy_renormalize:59.23(+0.00); psi=coverage_weighted:59.48(+0.25); psi=coverage_withheld@0.6:59.48(+0.25) |
| USDD | eigenlayer | 65.82 | psi=legacy_renormalize:65.82(+0.00); psi=coverage_weighted:64.88(-0.94); psi=coverage_withheld@0.6:64.88(-0.94) |
| USDD | jupiter-perpetual-exchange | 65.64 | psi=legacy_renormalize:65.64(+0.00); psi=coverage_weighted:66.94(+1.30); psi=coverage_withheld@0.6:66.94(+1.30) |
| USDD | lido | 75.94 | psi=legacy_renormalize:75.94(+0.00); psi=coverage_weighted:74.99(-0.95); psi=coverage_withheld@0.6:74.99(-0.95) |
| USDD | morpho | 73.49 | psi=legacy_renormalize:73.49(+0.00); psi=coverage_weighted:72.63(-0.86); psi=coverage_withheld@0.6:72.63(-0.86) |
| USDD | raydium | 63.81 | psi=legacy_renormalize:63.81(+0.00); psi=coverage_weighted:63.76(-0.05); psi=coverage_withheld@0.6:63.76(-0.05) |
| USDD | sky | 72.39 | psi=legacy_renormalize:72.39(+0.00); psi=coverage_weighted:71.66(-0.73); psi=coverage_withheld@0.6:71.66(-0.73) |
| USDD | spark | 69.68 | psi=legacy_renormalize:69.68(+0.00); psi=coverage_weighted:69.53(-0.15); psi=coverage_withheld@0.6:69.53(-0.15) |
| USDD | uniswap | 73.72 | psi=legacy_renormalize:73.72(+0.00); psi=coverage_weighted:74.45(+0.73); psi=coverage_withheld@0.6:74.45(+0.73) |
| USDC | aave | 86.14 | psi=legacy_renormalize:86.14(+0.00); psi=coverage_weighted:85.76(-0.38); psi=coverage_withheld@0.6:85.76(-0.38) |
| USDC | compound-finance | 74.11 | psi=legacy_renormalize:74.11(+0.00); psi=coverage_weighted:74.49(+0.38); psi=coverage_withheld@0.6:74.49(+0.38) |
| USDC | convex-finance | 74.68 | psi=legacy_renormalize:74.68(+0.00); psi=coverage_weighted:73.15(-1.53); psi=coverage_withheld@0.6:73.15(-1.53) |
| USDC | curve-finance | 68.51 | psi=legacy_renormalize:68.51(+0.00); psi=coverage_weighted:69.98(+1.47); psi=coverage_withheld@0.6:69.98(+1.47) |
| USDC | drift | 65.58 | psi=legacy_renormalize:65.58(+0.00); psi=coverage_weighted:65.85(+0.27); psi=coverage_withheld@0.6:65.85(+0.27) |
| USDC | eigenlayer | 72.88 | psi=legacy_renormalize:72.88(+0.00); psi=coverage_weighted:71.83(-1.05); psi=coverage_withheld@0.6:71.83(-1.05) |
| USDC | jupiter-perpetual-exchange | 72.67 | psi=legacy_renormalize:72.67(+0.00); psi=coverage_weighted:74.11(+1.44); psi=coverage_withheld@0.6:74.11(+1.44) |
| USDC | lido | 84.08 | psi=legacy_renormalize:84.08(+0.00); psi=coverage_weighted:83.02(-1.06); psi=coverage_withheld@0.6:83.02(-1.06) |
| USDC | morpho | 81.37 | psi=legacy_renormalize:81.37(+0.00); psi=coverage_weighted:80.41(-0.96); psi=coverage_withheld@0.6:80.41(-0.96) |

## Section D — RQS portfolio impact

| protocol | scored_coverage | baseline_rqs | t=0.5 | t=0.7 | t=0.85 |
|---|---|---|---|---|---|
| compound-finance | 1.00 | 87.91 | 87.91 | 87.91 | 87.91 |
| aave | 0.98 | 77.17 | 77.17 | 77.17 | 77.17 |
| sky | 1.00 | 87.99 | 87.99 | 87.99 | 87.99 |
| curve-finance | 1.00 | 80.33 | 80.33 | 80.33 | 80.33 |
| morpho | 1.00 | 85.91 | 85.91 | 85.91 | 85.91 |
| lido | 1.00 | 85.10 | 85.10 | 85.10 | 85.10 |
| spark | 1.00 | 81.59 | 81.59 | 81.59 | 81.59 |
| uniswap | 1.00 | 86.16 | 86.16 | 86.16 | 86.16 |
| convex-finance | 1.00 | 81.05 | 81.05 | 81.05 | 81.05 |

## Section E — Per-index migration recommendation

Recommendations are derived automatically from the coverage distributions
in Section A and the score movements in Sections B–D. Each paragraph
proposes target formula, threshold (if applicable), expected movement,
and any entities likely to withhold.

_To be populated: one paragraph per index, written from the generated
data above. Authoring guidance inside the analyzer; each paragraph must
cite specific entities and specific numbers._

## Hand-worked case studies

### USDC under SII
_(The minimal-movement reference case.)_

### rsETH under LSTI
_(The audit's reference case. Expected to withhold at threshold ≥ 0.75.)_

### Aave V3 under PSI
_(CQI-adjacent. Kelp context.)_

_To be populated: category-by-category walkthrough from generated rescoring data._
