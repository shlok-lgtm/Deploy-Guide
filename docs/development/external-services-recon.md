# External Services Recon — basis-state & basis-provenance

**Date:** 2026-05-12
**Source:** Railway project `valiant-celebration` (`736af3ec-ed9d-41d5-a8b6-c778393dd12b`), environment `production` (`e50bf3e2-ee86-445d-b934-ad44098d040a`), plus basis-hub repo grep.
**My GitHub MCP scope:** `basis-protocol/basis-hub` only. I cannot read source from the two sibling repos described below; everything here comes from the Railway service config and basis-hub side-references.

---

## basis-state — what it is, runtime, source location, integration shape

### Identity
- **Railway service ID:** `61d5cb8c-7441-4db8-bfda-ad466f39497a`
- **Railway service name:** `basis-state`
- **Region / replicas:** `us-west2`, 1 replica
- **Public domain:** none configured (internal-only)

### Source location
- **Sibling GitHub repo:** `basis-protocol/basis-state` (branch `main`)
- NOT in basis-hub. NOT a third-party SaaS.
- The repo is not accessible to this session's GitHub MCP (basis-hub-scoped).

### Runtime
- **Builder:** Dockerfile at repository root: `/Dockerfile`
- **Start/build commands:** none configured (uses image defaults)
- **Env vars set on the service (names only):**
  - `BASIS_API_URL` — points back at the hub API (hub-spoke pattern)
  - `PORT` — HTTP listen port

### Integration shape
- Pure spoke. Talks to the hub via HTTP through `BASIS_API_URL`. No `DATABASE_URL`, so it does not touch Postgres directly.
- Public-facing domain is `basisstate.xyz` per `/home/user/basis-hub/BASISSTATE_ANALYTICS_TODO.md` (which calls out adding Plausible analytics there). That TODO file also notes "the basis-state repo is separate and not available in this workspace."
- Punchlist confirms it required a force-redeploy from its own `main` after the 2026-05-10 Neon incident — verifying it is auto-deployed from `basis-protocol/basis-state`, not from basis-hub.
- Described in CLAUDE.md as "state subgraph" — likely serves a public read-only view over hub data via the API.

---

## basis-provenance — what it is, runtime, source location, integration shape

### Identity
- **Railway service ID:** `7eb5b894-2107-439c-9c53-00e99fc37b72`
- **Railway service name:** `basis-provenance`
- **Region / replicas:** `us-west2`, 1 replica
- **Public domain:** none configured (internal-only)

### Source location
- **Sibling GitHub repo:** `basis-protocol/basis-provenance` (branch `main`)
- NOT in basis-hub. NOT a third-party SaaS.
- Per `docs/canon-update-2026-05-10.md`: "If the canon doc currently calls it 'prover', rename to `basis-provenance`." The service used to be named "prover".

### Runtime
- **Builder:** Dockerfile at `/Dockerfile.prover` in the basis-provenance repo
- **Start/build commands:** none configured
- **Env vars set on the service (names only):**
  - `NOTARY_HOST`, `NOTARY_PORT` — its own bind config
  - `HUB_API_URL`, `HUB_API_KEY` — calls hub API (hub-spoke)
  - `ATTESTOR_PRIVATE_KEY` — signs attestations
  - `R2_ENDPOINT`, `R2_PUBLIC_URL`, `R2_BUCKET`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` — Cloudflare R2 for archived proofs
  - `COINGECKO_API_KEY`, `COINGECKO_BASE_URL`, `ETHERSCAN_API_KEY` — independent data fetches
  - `POLL_INTERVAL_SECONDS` — its own polling cadence

### Integration shape
- Pure spoke. Calls hub via `HUB_API_URL` with `HUB_API_KEY` for auth.
- Independently fetches CoinGecko + Etherscan to corroborate hub-published scores, then signs the result with `ATTESTOR_PRIVATE_KEY` and archives to Cloudflare R2 (the "R2-archived proofs" referenced in CLAUDE.md).
- Hub side recognizes attestor signatures via `ATTESTOR_PUBLIC_KEY` (see env inventory line 19, used in `app/server.py` for attestation verification).
- R2 itself is third-party SaaS (Cloudflare). The notary/prover service is in-org code.

---

## Implications for Phase A docker-compose

| Service | Recommendation | Why |
|---|---|---|
| **basis-state** | **Stub with a fixture** (do not containerize) | Source repo not accessible to this session. Hub does not call basis-state — basis-state calls hub. From basis-hub's perspective it is a downstream read-only consumer that has no impact on local dev of hub features. Phase A should focus on running the hub, worker, keeper locally; basis-state can simply not exist in compose. |
| **basis-provenance** | **Stub with a fixture** (do not containerize) | Same direction of dependency: basis-provenance calls hub, hub never calls basis-provenance. Hub only consumes attestor signatures it has already received and stored. For local dev, leave `ATTESTOR_PUBLIC_KEY` unset (or set to a dev keypair) and the relevant endpoint paths will simply report no attestations. R2 access (third-party SaaS) is not needed for hub dev. |

**Specific Phase A guidance:**
- **Do not** attempt to add `Dockerfile.state` or `Dockerfile.provenance` to basis-hub. Both have their own Dockerfiles in their own repos (`/Dockerfile` and `/Dockerfile.prover` respectively).
- **Do not** add them to docker-compose as `build:` services. If a future Phase wants integration testing, use `image:` pulling pre-built images from a registry, or `git clone` the sibling repos out of band. That is out of scope for Phase A.
- **Do** document in the compose README that these two services are sibling repos and intentionally omitted from local compose.
- **Optional:** if anyone needs to mock the hub-side surface that basis-provenance writes to (e.g. signed attestation payloads), commit a small JSON fixture under `tests/fixtures/attestations/` and a fake `ATTESTOR_PUBLIC_KEY` matching it. This unblocks rendering `/api/state-root/latest` etc. in a dev environment.

---

## Gaps

1. **Exact API surface basis-state exposes** — I know it has a `PORT` and a `BASIS_API_URL`. I do not know which endpoints it serves or what shape `basisstate.xyz` presents. Would require reading `basis-protocol/basis-state` source.
2. **basis-provenance polling logic** — I know it polls (`POLL_INTERVAL_SECONDS`) and fetches CoinGecko + Etherscan. I do not know the exact verification algorithm, which hub endpoints it calls, or what proof artifact format it writes to R2.
3. **Hub endpoints called by basis-provenance** — `HUB_API_KEY` implies authenticated calls but the route(s) are not visible from this side. Likely candidates based on hub code: `/api/scores/...`, `/api/state-root/latest`, `/api/provenance/*`, but unconfirmed.
4. **Whether basis-state shares the hub DB** — `BASISSTATE_ANALYTICS_TODO.md` line 69 raises this as an open question: "If basis-state shares the hub database (same DATABASE_URL), you could log to `api_request_log` instead." So even the local docs are uncertain. From the Railway env-var list, basis-state has NO `DATABASE_URL`, so currently it does not share the DB — but the doc author wasn't sure.
5. **Canon repo state** — `docs/canon-update-2026-05-10.md` lists proposed updates to the canon repo to add these two services; whether the canon was actually updated is unknown to me (canon repo not accessible).
6. **Whether either service has historical Dockerfile changes that we'd want mirrored in a local compose** — not visible.
