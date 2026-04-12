# basis-provenance: Registry-Driven Refactor Spec

This document describes the changes needed in the `basis-provenance` service
to read from the hub's `data_source_registry` table instead of a hardcoded
source list.

## Current State (hardcoded)

The provenance service has a hardcoded list of ~4 source endpoints:
- CoinGecko price (`pro-api.coingecko.com`)
- DeFiLlama stablecoins (`stablecoins.llama.fi`)
- DeFiLlama TVL (`api.llama.fi`)
- Etherscan token balance (`api.etherscan.io`)

Each cycle, it iterates this list, runs TLSNotary MPC-TLS for each, uploads
proofs to R2, and registers metadata with the hub via `POST /api/provenance/register`.

## Target State (registry-driven)

### 1. Replace hardcoded list with API call

At the start of each proving cycle, fetch the registry:

```
GET {HUB_API_BASE}/api/provenance/sources?prove=true
```

Response:
```json
{
  "sources": [
    {
      "source_domain": "pro-api.coingecko.com",
      "source_endpoint": "/api/v3/coins/usd-coin",
      "method": "GET",
      "description": "Current coin data for SII scoring",
      "collector": "sii_collector",
      "params_template": {"localization": "false", "tickers": "true"},
      "prove_frequency": "hourly",
      "notes": null
    },
    ...
  ],
  "count": 12
}
```

### 2. API key mapping config

Create a config that maps domains to API key environment variables. The registry
never stores keys; the provenance service injects them at prove time.

```python
DOMAIN_API_KEYS = {
    "pro-api.coingecko.com": {
        "header": "x-cg-pro-api-key",
        "env": "COINGECKO_API_KEY",
    },
    "api.etherscan.io": {
        "param": "apikey",
        "env": "ETHERSCAN_API_KEY",
    },
    "mainnet.helius-rpc.com": {
        "param": "api-key",
        "env": "HELIUS_API_KEY",
    },
    "api.helius.xyz": {
        "param": "api-key",
        "env": "HELIUS_API_KEY",
    },
    # Domains without entries (api.llama.fi, stablecoins.llama.fi,
    # yields.llama.fi, api.curve.finance) need no API key.
}
```

When a new domain appears that isn't in this map and doesn't require auth,
it just works. When it requires auth, log a warning and skip:

```
WARN: New source hub.snapshot.org requires API key not configured — skipping
```

### 3. Handle prove_frequency

Not every source needs hourly proving. The registry's `prove_frequency` field
tells the service how often to prove each source:

- `hourly`: prove every cycle (default)
- `daily`: prove once per 24h (check last proof timestamp before running)
- `weekly`: prove once per 7d

Implementation: before starting a TLSNotary session for a source, check
`provenance_proofs` for the most recent proof for that domain. If the last
proof is more recent than the frequency allows, skip it.

### 4. Handle method restrictions

Sources with `method: "POST"` are flagged with `prove: false` in the registry
because TLSNotary POST support is unverified. These include:

- `hub.snapshot.org/graphql` (POST/GraphQL)
- `api.tally.xyz/query` (POST/GraphQL)
- `mainnet.helius-rpc.com` (POST/JSON-RPC)

If TLSNotary adds POST support in the future, these can be re-enabled by
setting `prove = true` in the registry.

### 5. Handle large responses

Some sources return responses too large for efficient TLSNotary proving.
These are flagged in the `notes` field of the registry. The provenance service
should respect `prove = false` and log the reason from `notes`.

### 6. Proving loop pseudocode

```python
async def proving_cycle():
    # 1. Fetch registry
    sources = await fetch_sources_from_hub()

    for source in sources:
        # 2. Check frequency
        if not should_prove_now(source):
            continue

        # 3. Build request
        url = f"https://{source['source_domain']}{source['source_endpoint']}"
        headers, params = inject_api_key(source['source_domain'],
                                          source.get('params_template', {}))

        # 4. Run TLSNotary MPC-TLS session
        proof = await run_tlsnotary_session(url, headers, params)

        # 5. Upload proof to R2
        proof_url = await upload_to_r2(proof)

        # 6. Register with hub
        await register_proof_with_hub(
            source_domain=source['source_domain'],
            source_endpoint=source['source_endpoint'],
            response_hash=proof.response_hash,
            attestation_hash=proof.attestation_hash,
            proof_url=proof_url,
        )
```

### 7. Zero-regression requirement

The original 4 sources MUST continue to be proved after the refactor.
They will appear in the registry because the existing collectors
(coingecko.py, defillama.py, etherscan.py) now call `register_data_source()`
on every fetch.

### 8. Gap monitoring

The hub now has `GET /api/provenance/gaps` which returns sources with
`prove=true` that have no proof in the last 24 hours. The provenance service
can use this as a self-check after each cycle.

## What NOT to change

- R2 storage format and proof verification logic stay the same
- Attestor key management stays the same
- `POST /api/provenance/register` API stays the same
- Proof metadata schema stays the same
