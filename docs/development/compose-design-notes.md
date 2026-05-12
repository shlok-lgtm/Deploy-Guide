# docker-compose.dev.yml — Design Notes

Phase A2 of the local-dev-environment plan. Explains the decisions baked
into `docker-compose.dev.yml` so future operators can understand why the
file looks the way it does, and what its known limitations are.

## Service scope: which Railway services are mirrored

The Railway `valiant-celebration` project has 13 services. This compose
file mirrors 11 of them:

- `api-server`, `worker` (Scoring-Worker on Railway), `keeper`
- 8 × `backfill-{tti,cxri,vsri,dohi,bri,lsti,rpi,psi}`
- 1 × `anvil` sidecar (no Railway equivalent — see below)

**`basis-state` and `basis-provenance` are intentionally omitted.** They
deploy from sibling repos (`basis-protocol/basis-state` and
`basis-protocol/basis-provenance`) using Dockerfiles that live in those
repos. They are downstream consumers of the hub's REST API — neither has
an inbound dependency from the hub. Including them here would require
checking out two more repos and would force every hub dev to maintain
their build context. Per the Phase A2 operator decision: omitted. Devs
who need them clone the sibling repos and run them in their own compose
stacks pointing at `http://host.docker.internal:8000` for the hub API.

## Anvil: why a Base mainnet fork

The production keeper publishes scores to two on-chain oracle contracts:
`BASE_ORACLE_ADDRESS` (chain ID 8453) and `ARBITRUM_ORACLE_ADDRESS`
(chain ID 42161). In dev we substitute a single Foundry Anvil container
forked from Base mainnet so the keeper can:

1. Submit real transactions against a realistic chain state (gas prices,
   nonces, block timing) without burning ETH.
2. Verify contract calls against the actual deployed `BasisSIIOracle.sol`
   bytecode rather than a fresh deployment that may diverge from prod.

Chain ID is pinned to 8453 so the keeper's chain-id assertions pass.
Arbitrum is not forked — single-fork keeps RAM use bounded; operators
testing Arbitrum-specific paths should override `ARBITRUM_RPC_URL`.

**Entrypoint override.** Empirically, `ghcr.io/foundry-rs/foundry:latest`
ships with an sh-based entrypoint rather than `["anvil"]`. Passing a list
`command:` of anvil flags without an explicit `entrypoint:` override sends
those flags through sh and produces `/bin/sh: 0: Illegal option --` at
container start (caught during Codespace boot on 2026-05-12). The compose
file therefore pins `entrypoint: ["anvil"]` on the anvil service. If a
future foundry release breaks the override, fall back to a single
string-form `command:` (shell form) and re-document here.

## Backfills: why a profile

The eight backfill services on Railway have `restartPolicyType: NEVER` —
they are one-shot jobs run manually (or by a sibling CI step) when a new
index needs a historical reconstruction. Wiring them into the default
compose `up` set would:

- Fire 8 long jobs on every `make dev`, burning Neon-side query budget
  and rate-limiting upstream APIs.
- Wedge the dev loop on first boot while the backfills churn.

Putting them under `profiles: [backfill]` makes them opt-in:
`docker compose -f docker-compose.dev.yml --profile backfill run --rm backfill-psi`.

## Healthcheck rationale

| Service | Check | Why |
|---|---|---|
| api-server | `curl /healthz` every 30s, start_period 60s, retries 5 | Mirrors Railway's healthcheck (which is `/healthz` with a 300s timeout). FastAPI cold start is ~30-45s with the 6,684-line server.py and 3 background subsystems. |
| worker | none | Background scoring loop; no HTTP surface. A psql-based attestation-freshness probe is overkill for compose. Rely on `docker compose logs -f worker` to diagnose. |
| keeper | none | Polling loop. RPC failures show up as repeated tx errors in logs, which is more diagnostic than a contrived liveness probe. |
| anvil | `cast block-number --rpc-url http://localhost:8545` every 15s | Proves the fork is up and the RPC is accepting. Used as a `service_healthy` gate for `keeper.depends_on`. |
| backfills | n/a | One-shot jobs; exit code is the signal. |

## Image tagging

All hub-built services use `image: basis-hub-<svc>:${BASIS_IMAGE_TAG:-dev}`
so operators can promote a built image across compose runs by setting
`BASIS_IMAGE_TAG=$(git rev-parse --short HEAD)` and avoid rebuilds when
the source tree hasn't changed.

## env.dev contract

All services load `env_file: env.dev`. That file is gitignored; the
template lives at `env.dev.example` (written by a sibling Phase A2
subagent). Required keys for a working stack: `DATABASE_URL`,
`COINGECKO_API_KEY`, `ETHERSCAN_API_KEY`, and `KEEPER_PRIVATE_KEY` if
the keeper service is part of the run. Anvil generates funded test
accounts on startup — the keeper's private key for dev should be one of
those (printed in `anvil` logs).

## Known limitations

1. **`BASE_FORK_URL` defaults to the public Base RPC** (`https://mainnet.base.org`),
   which aggressively rate-limits. Sustained dev should use a paid endpoint
   (Alchemy, QuickNode, Dwellir) and set `BASE_FORK_URL` accordingly in the
   shell or in `.env` (compose auto-loads `.env` for variable substitution).
2. **`Dockerfile.worker` does not COPY `scripts/`**, but backfills live under
   `scripts/backfill/`. To run them against the worker image without
   modifying the Dockerfile, each backfill service bind-mounts `./scripts`
   read-only at `/app/scripts`. This is a dev-only shim — production
   backfills rebuild the image. If the Dockerfile is ever extended to
   include `scripts/`, the mount becomes redundant but harmless.
3. **No Postgres container.** `DATABASE_URL` in `env.dev` is expected to
   point at a Neon dev branch (or `host.docker.internal:5432` if the
   operator runs Postgres on the host). This matches production, which is
   on owned Neon (pooler endpoint per CLAUDE.md).
4. **`api-server.depends_on: worker` is a soft dep** (`required: false`).
   API can boot without the worker — useful when iterating on routes.
5. **Arbitrum is not forked.** Keeper paths that hit Arbitrum will fail
   unless `ARBITRUM_RPC_URL` in env.dev points at a live RPC (mainnet
   read-only is usually fine since dev keeper has no Arbitrum funds).
6. **Worker has no healthcheck.** If it crashes silently mid-cycle the
   compose stack will report `Up` but not `healthy`. Restart policy
   (`unless-stopped`) covers crash-then-exit; silent hangs require log
   inspection. Acceptable for dev.
