# Service Runtime Matrix

Mapping of the 13 Railway services in the `valiant-celebration` project
(`projectId: 736af3ec-ed9d-41d5-a8b6-c778393dd12b`, production environment
`e50bf3e2-ee86-445d-b934-ad44098d040a`) to Dockerfiles and start commands.

Data captured 2026-05-12 via Railway MCP `railway-agent` against the live
service configs. The repo contains only three Dockerfiles
(`Dockerfile.api`, `Dockerfile.worker`, `Dockerfile.keeper`). Two services
(`basis-state`, `basis-provenance`) deploy from sibling repos
(`basis-protocol/basis-state`, `basis-protocol/basis-provenance`) and use
Dockerfiles that live in those repos, not in `basis-hub`.

---

## Summary Table

| Service | Service ID | Source repo | Dockerfile | Effective start command | Notable env | Notes |
|---|---|---|---|---|---|---|
| api-server | 39defdfc-cfa8-4929-9e17-6dfe86d7d791 | basis-protocol/basis-hub @ main | `/Dockerfile.api` | (Dockerfile CMD) `python main.py` | `PORT`, `WORKER_ENABLED`, `KEEPER_ENABLED`, `WEB_CONCURRENCY`, `WEB_WORKERS`, `COLLECTION_INTERVAL`, `DATABASE_URL`, `ADMIN_KEY`, many vendor keys | Healthcheck `/healthz` (300s). Dockerfile hardcodes `WORKER_ENABLED=false`, `KEEPER_ENABLED=false`, `PORT=8000`; runtime env overrides these. |
| Scoring-Worker | 8da95cc8-e389-460c-9efa-0f77a19de6fc | basis-protocol/basis-hub @ main | `/Dockerfile.worker` | (Dockerfile CMD) `python -m app.worker --loop` | `WORKER_ENABLED`, `COLLECTION_INTERVAL`, `DATABASE_URL`, `ALCHEMY_API_KEY`, `HELIUS_API_KEY`, `DWELLIR_*`, `PYTHONASYNCIODEBUG` | Long-running scoring loop. No `--loop` override needed because CMD already supplies it. |
| Keeper (note trailing space in name) | f3b9fe6e-3e15-46b1-bdde-a919574d4732 | basis-protocol/basis-hub @ main, rootDir `/` | `/Dockerfile.keeper` | (Railway override, identical to CMD) `npx tsx keeper/index.ts` | `KEEPER_PRIVATE_KEY`, `BASE_RPC_URL`, `ARBITRUM_RPC_URL`, `BASE_ORACLE_ADDRESS`, `ARBITRUM_ORACLE_ADDRESS`, `BASE_SBT_ADDRESS`, `MAX_GAS_PRICE_GWEI`, `POLL_INTERVAL_SECONDS`, `BASIS_API_URL` | No `KEEPER_ENABLED` flag here; this container is always-on. The flag only gates the in-process keeper subprocess on api-server. |
| basis-state | 61d5cb8c-7441-4db8-bfda-ad466f39497a | basis-protocol/basis-state @ main | `/Dockerfile` (in sibling repo) | (Dockerfile CMD; not visible from hub repo) | `PORT`, `BASIS_API_URL` | Dockerfile lives in the `basis-state` repo. TBD; investigated separately by subagent 4. |
| basis-provenance | 7eb5b894-2107-439c-9c53-00e99fc37b72 | basis-protocol/basis-provenance @ main | `/Dockerfile.prover` (in sibling repo) | (Dockerfile CMD; not visible from hub repo) | `POLL_INTERVAL_SECONDS`, `NOTARY_HOST`, `NOTARY_PORT`, `HUB_API_URL`, `HUB_API_KEY`, `ATTESTOR_PRIVATE_KEY`, `R2_*`, `COINGECKO_API_KEY`, `ETHERSCAN_API_KEY` | Dockerfile lives in the `basis-provenance` repo. TBD; investigated separately by subagent 4. |
| basis-backfill-tti | cd68915b-0618-454b-8019-c2a66fa9b9af | (no `source.repo` set on service) | inherited image (see "Backfill family resolution") | `python -m scripts.backfill.backfill_tti` (Railway override) | `DATABASE_URL` (refs psi service) | `restartPolicyType: NEVER` (one-shot). |
| basis-backfill-cxri | ed93b492-6aab-4b38-b3da-856455fd9e70 | basis-protocol/basis-hub @ main | inherits hub build | `python -m scripts.backfill.backfill_cxri` | `DATABASE_URL` | One-shot. |
| basis-backfill-vsri | 016d7a54-044b-4b8f-9e38-091cf58b578a | basis-protocol/basis-hub @ main | inherits hub build | `python -m scripts.backfill.backfill_vsri` | `DATABASE_URL`, `COINGECKO_API_KEY` | One-shot. |
| basis-backfill-dohi | 8ef9bdf4-5694-4493-ab5b-c702fcc833e0 | (no `source.repo` set on service) | inherited image | `python -m scripts.backfill.backfill_dohi` | `DATABASE_URL` | One-shot. |
| basis-backfill-bri | 5681954e-4267-412f-8672-7a06f283b86f | (no `source.repo` set on service) | inherited image | `python -m scripts.backfill.backfill_bri` | `DATABASE_URL` | One-shot. |
| basis-backfill-lsti | 43b17a89-59d3-457e-9546-552a12d02844 | basis-protocol/basis-hub @ main | inherits hub build | `python -m scripts.backfill.backfill_lsti` | `DATABASE_URL`, `COINGECKO_API_KEY` | One-shot. |
| basis-backfill-rpi | fd19df20-21b9-4640-b2a3-9c3c40c36b52 | (no `source.repo` set on service) | inherited image | `python -m scripts.backfill.backfill_rpi` | `DATABASE_URL` | One-shot. |
| basis-backfill-psi | f84eea5d-c2c4-4db5-b16a-b8bf70000a23 | basis-protocol/basis-hub @ main | inherits hub build | `python -m scripts.backfill.backfill_psi` | `DATABASE_URL` (shared) | One-shot. The psi service's `DATABASE_URL` is referenced by all other backfill services via `${{f84eea5d-...DATABASE_URL}}`. |

---

## api-server

- **Service ID:** `39defdfc-cfa8-4929-9e17-6dfe86d7d791`
- **Source:** `basis-protocol/basis-hub`, branch `main`
- **Build:** `dockerfilePath: /Dockerfile.api`
- **Start command:** unset on Railway → falls back to the Dockerfile CMD `python main.py`.
- **Healthcheck:** `GET /healthz`, 300s timeout.
- **Port:** `PORT` env var (Dockerfile sets `PORT=8000`; Railway-provided value overrides at runtime).
- **Runtime env quirks:**
  - Dockerfile hardcodes `WORKER_ENABLED=false` and `KEEPER_ENABLED=false`, but **both keys are present in the Railway env**, so the runtime values win. Confirm flag intent before redeploy — running the worker inside api-server is wasteful when `Scoring-Worker` exists.
  - Has `WEB_CONCURRENCY` + `WEB_WORKERS` (uvicorn tuning).
  - Has a stray env var `" SLACK_ENGINE_WEBHOOK_URL"` with a leading space (likely typo, harmless unless code expects the unstripped key).
- **Replicas:** 1 in `us-west2`.

## Scoring-Worker

- **Service ID:** `8da95cc8-e389-460c-9efa-0f77a19de6fc`
- **Source:** `basis-protocol/basis-hub`, branch `main`
- **Build:** `dockerfilePath: /Dockerfile.worker`
- **Start command:** unset → Dockerfile CMD `python -m app.worker --loop`.
- **Env:** `WORKER_ENABLED=true` (required for the loop to do work — `app.worker` reads this), `COLLECTION_INTERVAL` (minutes between cycles), `DATABASE_URL`, plus vendor keys (`COINGECKO_API_KEY`, `HELIUS_API_KEY`, `ETHERSCAN_API_KEY`, `ALCHEMY_API_KEY`, `BLOCKSCOUT_API_KEY`, `DWELLIR_API_KEY`, `DWELLIR_ETH_URL`, `FIRECRAWL_API_KEY`, `PARALLEL_API_KEY`, `REDUCTO_API_KEY`, `RESEND_API_KEY`).
- **Port:** none (background service).
- **Notes:** This is the real background scorer. It does NOT run via the in-process worker thread in `main.py` (that thread is gated by `WORKER_ENABLED` on api-server, which should be `false` in production to avoid double scoring).

## Keeper

- **Service ID:** `f3b9fe6e-3e15-46b1-bdde-a919574d4732` (display name has a trailing space: `"Keeper "`)
- **Source:** `basis-protocol/basis-hub`, branch `main`, `rootDirectory: /`
- **Build:** `dockerfilePath: /Dockerfile.keeper` (node:22-slim, installs `package.json`, copies `keeper/`).
- **Start command:** Railway sets `npx tsx keeper/index.ts` explicitly — this matches Dockerfile CMD verbatim, so the override is redundant but harmless.
- **`preDeployCommand`:** empty array (explicitly set).
- **Env:** `KEEPER_PRIVATE_KEY` (signer), `BASE_RPC_URL`, `ARBITRUM_RPC_URL`, `BASE_ORACLE_ADDRESS`, `ARBITRUM_ORACLE_ADDRESS`, `BASE_SBT_ADDRESS`, `MAX_GAS_PRICE_GWEI`, `POLL_INTERVAL_SECONDS`, `BASIS_API_URL`.
- **Notes:** This is the on-chain oracle publisher described in `CLAUDE.md`. It reads scores from the hub API (`BASIS_API_URL`) and submits transactions to Base + Arbitrum oracle contracts.

## basis-state

- **Service ID:** `61d5cb8c-7441-4db8-bfda-ad466f39497a`
- **Source:** `basis-protocol/basis-state`, branch `main` (sibling repo, NOT this hub).
- **Build:** `dockerfilePath: /Dockerfile` (default name; lives in the sibling repo, not visible from `basis-hub`).
- **Start command:** unknown from hub side — depends on the sibling repo's Dockerfile CMD.
- **Env:** `PORT`, `BASIS_API_URL` (so it consumes the hub's REST API).
- **Status:** **TBD; investigated separately by subagent 4.** Likely a state-attestation publishing service, but cannot confirm from this repo.

## basis-provenance

- **Service ID:** `7eb5b894-2107-439c-9c53-00e99fc37b72`
- **Source:** `basis-protocol/basis-provenance`, branch `main` (sibling repo, NOT this hub).
- **Build:** `dockerfilePath: /Dockerfile.prover` (lives in the sibling repo).
- **Start command:** unknown from hub side.
- **Env:** `NOTARY_HOST`, `NOTARY_PORT`, `POLL_INTERVAL_SECONDS`, `HUB_API_URL`, `HUB_API_KEY`, `ATTESTOR_PRIVATE_KEY`, `R2_ENDPOINT`/`R2_PUBLIC_URL`/`R2_BUCKET`/`R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`, `COINGECKO_API_KEY`, `ETHERSCAN_API_KEY`.
- **Status:** **TBD; investigated separately by subagent 4.** Appears to be a provenance/notary service that polls the hub API and persists evidence to Cloudflare R2 with an attestor signature.

---

## Backfill family resolution

All eight `basis-backfill-*` services are **one-shot Python jobs** (`restartPolicyType: NEVER`) that share the same runtime contract:

```
START: python -m scripts.backfill.backfill_<index>
ENV:   DATABASE_URL (+ COINGECKO_API_KEY for vsri & lsti)
BUILD: inherits the hub repo's build (no explicit dockerfilePath set on the service)
```

Concretely:

| Service | Module invoked | Index target | Source repo on service config |
|---|---|---|---|
| basis-backfill-tti | `scripts.backfill.backfill_tti` | TTI (`app/index_definitions/tti_v01.py`) | not set on service |
| basis-backfill-cxri | `scripts.backfill.backfill_cxri` | CXRI (`app/index_definitions/cxri_v01.py`) | basis-protocol/basis-hub @ main |
| basis-backfill-vsri | `scripts.backfill.backfill_vsri` | VSRI (`app/index_definitions/vsri_v01.py`) | basis-protocol/basis-hub @ main |
| basis-backfill-dohi | `scripts.backfill.backfill_dohi` | DOHI (`app/index_definitions/dohi_v01.py`) | not set on service |
| basis-backfill-bri | `scripts.backfill.backfill_bri` | BRI (`app/index_definitions/bri_v01.py`) | not set on service |
| basis-backfill-lsti | `scripts.backfill.backfill_lsti` | LSTI (`app/index_definitions/lsti_v01.py`) | basis-protocol/basis-hub @ main |
| basis-backfill-rpi | `scripts.backfill.backfill_rpi` | RPI (`app/index_definitions/rpi_v2.py`) | not set on service |
| basis-backfill-psi | `scripts.backfill.backfill_psi` | PSI (`app/index_definitions/psi_v01.py`) | basis-protocol/basis-hub @ main |

**The hub repo confirms these modules exist** under `/home/user/basis-hub/scripts/backfill/`:
`backfill_tti.py`, `backfill_cxri.py`, `backfill_vsri.py`, `backfill_dohi.py`,
`backfill_bri.py`, `backfill_lsti.py`, `backfill_rpi.py`, `backfill_psi.py`,
plus shared `base.py` and `__init__.py`.

**Important quirk — inconsistent `source.repo` on backfill services:**
Four services (cxri, vsri, lsti, psi) have `source.repo` set to `basis-protocol/basis-hub@main`, but the other four (tti, dohi, bri, rpi) have no `source` block at all. None of the eight has an explicit `build.dockerfilePath`. In practice Railway will rebuild from the hub repo on push for the four configured services and rely on the last-cached image / nixpacks default for the other four. This is fragile — the four without `source.repo` will not auto-redeploy when `scripts/backfill/*` changes. Worth normalizing.

**No `BACKFILL_INDEX` / `INDEX_ID` / `BACKFILL_TARGET` env vars exist** — the index family is selected purely by which module the start command names. So the eight services are clones differing only by their `deploy.startCommand`.

**Shared DATABASE_URL:** Seven of the eight backfill services reference
`${{f84eea5d-c2c4-4db5-b16a-b8bf70000a23.DATABASE_URL}}` (the `basis-backfill-psi`
service's `DATABASE_URL`), and `basis-backfill-psi` itself references
`${{shared.DATABASE_URL}}`. So `basis-backfill-psi`'s var is a fan-out point for
the family — changing it propagates to all seven siblings.

---

## Gaps

1. **basis-state Dockerfile/CMD:** Service deploys from `basis-protocol/basis-state` using `/Dockerfile`. The actual CMD, exposed port, and runtime entrypoint are defined in that sibling repo and cannot be confirmed from `basis-hub`. **TBD; investigated separately by subagent 4.**
2. **basis-provenance Dockerfile/CMD:** Service deploys from `basis-protocol/basis-provenance` using `/Dockerfile.prover`. Same situation as basis-state. **TBD; investigated separately by subagent 4.**
3. **Backfill services without `source.repo`** (tti, dohi, bri, rpi): no source block visible on the service config. Possibilities: (a) historically deployed by image push and never re-attached to a repo; (b) the agent's view omitted the field. The start command and `DATABASE_URL` are confirmed, but it's not certain which commit/build of `scripts/backfill/*.py` they currently run.
4. **Port mapping / public domains:** the `networking` blocks returned by the agent are empty or omitted. Only `api-server` has a healthcheck (`/healthz`); no service surfaces a public domain in the captured config. If the api-server's Railway-managed domain is required, it must be looked up via the dashboard or `gh`-style introspection.
5. **`WORKER_ENABLED` on api-server:** the env var is set but its current value was hidden. If it is `true`, api-server is double-running the worker alongside `Scoring-Worker`. Should be verified out-of-band.
6. **`KEEPER_ENABLED` on api-server:** likewise hidden. If `true`, api-server spawns a keeper subprocess in addition to the dedicated `Keeper` service.
7. **Display-name whitespace:** the keeper service is literally named `"Keeper "` (trailing space). Cosmetic, but breaks naive name matching.
8. **Stray env-var key on api-server:** `" SLACK_ENGINE_WEBHOOK_URL"` has a leading space. Likely unused; should be cleaned up.
9. **`Dockerfile.worker` cache-bust** (dated 2026-04-14) suggests past trouble re-pulling layers. Not a current problem but worth noting if redeploys behave oddly.
