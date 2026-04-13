# Blockscout Builder vs Etherscan: Endpoint Parity & Migration Assessment


## 1. Complete Endpoint Parity Table

Every Etherscan API call in the codebase mapped to its Blockscout equivalent.

```
#   What we call                          Etherscan endpoint              Blockscout equivalent?     Files using it
--  ------------------------------------  ------------------------------  -------------------------  -----------------------------------------------
1   Token balances (all tokens/address)   account/addresstokenbalance     FULL PARITY                scanner.py
                                                                          Etherscan-compat wrapper
                                                                          + native v2 /api/v2/
                                                                          addresses/{addr}/
                                                                          token-balances

2   Single token balance                  account/tokenbalance            FULL PARITY                etherscan.py, scanner.py, holder_analysis.py
                                                                          account/tokenbalance
                                                                          via Etherscan-compat API

3   Token transfers (tokentx)             account/tokentx                 FULL PARITY                flows.py, mint_burn_collector.py,
                                                                          account/tokentx            wallet_expansion.py, edges.py,
                                                                          Already wrapped in         scanner.py, treasury_flows.py
                                                                          blockscout_client.py

4   Transaction list                      account/txlist                  FULL PARITY                parameter_collector.py, vault_collector.py,
                                                                          account/txlist             bridge_collector.py, keeper_monitor.py,
                                                                          Not yet wrapped but        oracle_monitor.py
                                                                          endpoint exists

5   Top holders                           token/tokenholderlist           FULL PARITY                pool_wallet_collector.py, scanner.py,
                                                                          Already using both.        expander.py
                                                                          Wrapped in
                                                                          blockscout_client.py

6   Holder count                          token/tokenholdercount          FULL PARITY                etherscan.py, holder_analysis.py
                                                                          token/tokenholdercount
                                                                          Wrapped + shadow-compared

7   Contract ABI                          contract/getabi                 FULL PARITY                smart_contract.py, tti_collector.py,
                                                                          contract/getabi            data_source_comparator.py
                                                                          Wrapped + shadow-compared

8   Contract source code                  contract/getsourcecode          FULL PARITY                smart_contract.py, contract_surveillance.py
                                                                          contract/getsourcecode
                                                                          Wrapped in
                                                                          blockscout_client.py

9   eth_call (contract reads)             proxy/eth_call                  FULL PARITY                dao_collector.py (multisig + timelock reads)
                                                                          proxy/eth_call
                                                                          Not yet wrapped but
                                                                          endpoint exists

10  Event logs (getLogs)                  N/A - NOT an Etherscan call     N/A                        oracle_monitor.py
                                                                          Uses direct JSON-RPC
                                                                          to Alchemy/node.
                                                                          Unaffected by migration.

11  Internal transactions                 account/txlistinternal          Not used in codebase       —
```

Verdict: Blockscout covers 100% of Etherscan endpoints we use.
9 out of 9 actual Etherscan calls have a direct Blockscout equivalent.
The getLogs call is JSON-RPC to a node, not Etherscan.
txlistinternal is not used anywhere.

blockscout_client.py already wraps 6 of 9 endpoints.
The 3 missing wrappers (tokenbalance, txlist, eth_call) are trivial —
same module/action interface, just new functions in the client.


## 2. Current State

```
Provider                Configured rate    Daily capacity    Rate limit    Cost
----------------------  -----------------  ----------------  ------------  ------------------
Etherscan (Std/Pro)     8.0 req/s          200K calls/day    10/s          Paid Pro (unknown)
Blockscout (Free)       4.0 req/s          100K credits/day  5/s           $0
Combined                ~12 effective/s    ~300K/day         —             Pro cost
```

Blockscout is already the default primary provider
(BLOCK_EXPLORER_PROVIDER defaults to "blockscout").
Etherscan is the automatic fallback if Blockscout returns
zero results for all wallets.


## 3. After Switch: Blockscout Builder

```
Metric              Current (Free + Pro)        After (Builder + Free)
------------------  --------------------------  --------------------------
Primary provider    Blockscout Free             Blockscout Builder
Fallback provider   Etherscan Pro               Etherscan Free
Daily capacity      300K combined               3,300,000+
Rate limit          ~12/s effective             15/s (Builder) + 5/s per native instance
Monthly cost        $49+ (Etherscan Pro)        $49 (Blockscout Builder)
```

That's an 11x increase in daily capacity.


## 4. Capacity Math

Blockscout Builder: $49/month, 100M credits/month, 15 req/sec.

```
Use case                                            Calls/day      % of 3.3M budget
--------------------------------------------------  -------------  ----------------
Full token balance scan (500K wallets x 3 chains)   1,500,000      45%
Deep holder pagination (receipt tokens + stables)   200,000        6%
Transaction history (10K new wallets/day)            100,000        3%
Mint/burn tracking (all stablecoins, all chains)     5,000          0.2%
Contract surveillance (weekly ABI + source)          1,000          0.03%
Holder counts + DAO contract reads                   2,000          0.06%
--------------------------------------------------  -------------  ----------------
TOTAL                                                ~1,808,000     55%
```

45% headroom remaining.
The wallet census goes from "months to reach 500K" to "weeks."

Additionally: the native v2 token-balances endpoint
(eth.blockscout.com/api/v2/addresses/{addr}/token-balances)
is free and does NOT consume Builder credits. That's the biggest
consumer (1.5M calls for balance scanning). If we keep using native
v2 for balance scanning and Builder credits for everything else,
effective daily credit usage drops to ~308K — under 10% of budget.


## 5. Per-Chain Budget

Blockscout Builder gives one API key that works across all chains
via api.blockscout.com/v2/api?chain_id=X.

The 100M credits are SHARED across all chains, not per-chain.

However, the native v2 endpoints are free per instance with
independent rate limits:

    eth.blockscout.com       — free, ~5 req/s
    base.blockscout.com      — free, ~5 req/s
    arbitrum.blockscout.com  — free, ~5 req/s

Strategy: Use native v2 (free) for high-volume balance scanning.
Use Builder credits (3.3M/day) for tokentx, txlist, getabi,
getsourcecode, eth_call, tokenholdercount, tokenholderlist.


## 6. TLSNotary / Provenance Impact

The provenance registry (app/data_layer/provenance_scaling.py) currently has:

```
source_domain          Provider      Endpoint                                  Data types
---------------------  -----------   ----------------------------------------  --------------------------------
etherscan_holders      etherscan     /tokenholdercount                         wallet_holdings
etherscan_tokentx      etherscan     /tokentx                                  mint_burn_events
etherscan_sourcecode   etherscan     /getsourcecode                            contract_surveillance
blockscout_balances    blockscout    /v2/addresses/{address}/token-balances    wallet_holdings, wallet_behavior_tags
```

No domain change needed. The PRO/Builder API endpoint is
api.blockscout.com regardless of tier. The native v2 endpoints
(eth.blockscout.com, etc.) are unchanged. TLSNotary proofs use
source_domain as the registry key — we add new blockscout_*
entries for the migrated calls and keep etherscan_* entries as
fallback sources. No reconfiguration of the external prover
service required.


## 7. Recommended Provider Strategy

Make Blockscout Builder the sole primary.
Downgrade Etherscan to free-tier emergency fallback.

### Implementation steps

1. Upgrade Blockscout to Builder ($49/mo) — 100M credits/mo, 15 req/s

2. Update rate limits:
   - shared_rate_limiter.py:  "blockscout": (4.0, 12)  -->  (14.0, 30)
   - indexer/config.py:       EXPLORER_RATE_LIMIT_DELAY  0.22  -->  0.07  (~14 req/s)

3. Update blockscout_client.py doc:
   "Free tier: 100K credits/day"  -->  "Builder tier: 100M credits/month (~3.3M/day)"

4. Add 3 missing wrappers to blockscout_client.py:
   - get_token_balance()       account/tokenbalance
   - get_transaction_list()    account/txlist
   - eth_call()                proxy/eth_call

5. Migrate 5 Etherscan-only callers to Blockscout primary + Etherscan free fallback:
   - dao_collector.py          proxy/eth_call    (multisig + timelock reads)
   - parameter_collector.py    account/txlist    (governance tx tracking)
   - vault_collector.py        account/txlist    (vault monitoring)
   - bridge_collector.py       account/txlist    (bridge flows)
   - keeper_monitor.py         account/txlist    (keeper tx history)

6. Update provenance registry — add blockscout_tokentx,
   blockscout_sourcecode, blockscout_holders source entries

7. Cancel/downgrade Etherscan Pro — keep free tier
   (5 calls/sec, ~100K/day) as emergency fallback only

8. No change needed for BLOCK_EXPLORER_PROVIDER default —
   already "blockscout"


### Cost comparison

```
                Current             After               Delta
--------------  ------------------  ------------------  ------------------
Blockscout      $0 (free)           $49/mo (Builder)    +$49
Etherscan       $?/mo (Pro)         $0 (free tier)      -$? savings
Net             $?/mo               $49/mo              likely net savings
```


### Files that need changes

```
File                                     Change
---------------------------------------  ------------------------------------------------
app/utils/blockscout_client.py           Add get_token_balance(), get_transaction_list(),
                                         eth_call() wrappers. Update docstring + rate
                                         limit comment.

app/shared_rate_limiter.py               Bump blockscout from (4.0, 12) to (14.0, 30)

app/indexer/config.py                    Change EXPLORER_RATE_LIMIT_DELAY from 0.22 to
                                         0.07 for blockscout branch

app/collectors/dao_collector.py          Route proxy/eth_call through blockscout_client.py
                                         with Etherscan free fallback

app/rpi/parameter_collector.py           Route account/txlist through blockscout_client.py
                                         with Etherscan free fallback

app/collectors/vault_collector.py        Route account/txlist through blockscout_client.py
                                         with Etherscan free fallback

app/collectors/bridge_collector.py       Route account/txlist through blockscout_client.py
                                         with Etherscan free fallback

app/ops/tools/keeper_monitor.py          Route account/txlist through blockscout_client.py
                                         with Etherscan free fallback

app/data_layer/provenance_scaling.py     Add blockscout_holders, blockscout_tokentx,
                                         blockscout_sourcecode source entries
```


### Files that need NO changes

```
File                                     Why
---------------------------------------  ------------------------------------------------
app/indexer/scanner.py                   Already uses Blockscout primary + Etherscan fallback
app/indexer/edges.py                     Already uses Blockscout tokentx
app/utils/data_source_comparator.py      Shadow comparison — retire or keep for validation
app/ops/tools/oracle_monitor.py          Uses direct JSON-RPC, not Etherscan
app/collectors/flows.py                  Uses EXPLORER_BASE which resolves to Blockscout
app/collectors/etherscan.py              Uses EXPLORER_BASE, routes through Blockscout
```
