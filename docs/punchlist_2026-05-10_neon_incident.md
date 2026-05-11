# Punchlist Entry — 2026-05-10 Neon migration incident

**Incident date:** 2026-05-10 ~22:48 UTC (api-server deploy 54967f6a)
**Root cause identified:** 2026-05-11 ~00:50 UTC
**Production restored:** 2026-05-11 ~01:03 UTC (api-server on PR #130)
**Severity:** P0 (all 13 Railway services unable to connect to DB; api-server returned 500 on every request).
**Detection failure:** Railway marked the breaking deploy as SUCCESS because Uvicorn bound the port despite `init_pool()` raising. Caught by external observation, not internal monitoring.

## Root cause

`app/database.py::init_pool()` passed
`options="-c statement_timeout=120000"` as a libpq startup parameter to
`ThreadedConnectionPool`. The new owned-Neon `DATABASE_URL` points at the
`-pooler` endpoint (pgbouncer, transaction mode), which rejects all libpq
startup `options`:

> ERROR: unsupported startup parameter in options: statement_timeout.
> Please use unpooled connection or remove this parameter from the startup package.

`init_pool()` raised; every downstream `get_conn()` raised the cascading
`RuntimeError: Database pool not initialized. Call init_pool() first.`

The Replit-managed Neon integration had hidden this incompatibility — its
DATABASE_URL pointed at the direct (unpooled) endpoint by default. The
owned-Neon `DATABASE_URL` points at `-pooler`, which is correct for
connection-efficiency but exposed the latent libpq-options bug.

## Tracks

| Track | Owner | Scope | Status | Reference |
|---|---|---|---|---|
| A pt1 | CC | Drop libpq `options=`, apply timeout via `SET LOCAL` in `get_conn()` | ✅ merged | basis-hub#130 (`7b6828a`) |
| A pt2 | CC | `init_pool` retry (3× exp backoff), fail-loud `sys.exit(1)` before port bind, `/healthz`, `/readyz`, Railway `healthcheckPath` | 🟡 open | basis-hub#132 |
| A pt3 | CC | Migration runner safety — no change needed; 001 already idempotent, `migrations` table self-bootstraps, runner skips applied | ✅ verified | (no PR — folded into #132 commit message) |
| B | CC | pgbouncer pattern audit across repo | ✅ report | `docs/audits/2026-05-10-pgbouncer-audit.md` |
| C | CC | Replit-ism purge (5 files deleted, 12 docs updated, latent `_ensure_pg_env` bug fixed) | ✅ merged | basis-hub#131 (`2897ede`) |
| D | CC | Railway env-var inventory across 13 services | ✅ done | All clean. No PG*, no REPL*, no broken refs. |
| E | CC | Neon DB state verification (sequences, extensions, etc.) | ✅ report | `docs/audits/2026-05-10-neon-state.md`. No behind sequences. Write traffic safe. |
| F | CC | Canon doc update | 🟡 drafted | This file + `basis_protocol_v9_9_constitution_amendment.md` (in basis-hub; copy to canon manually) |
| Wave-2 | CC | VACUUM unpooled endpoint, batch backfill/indexer inserts | 🟡 open | basis-hub#133 |

## Resolution status (as of 2026-05-11 02:00 UTC)

- **Production restored:** ✅ all DB-using services on the fix commit and confirmed healthy via logs (`Database pool initialized`, normal worker activity).
- **6 services auto-deployed the fix:** api-server, Scoring-Worker, Keeper, basis-backfill-cxri, basis-backfill-vsri, basis-backfill-lsti.
- **3 services required force-redeploy from main HEAD:** basis-state and basis-provenance (separate repos; rebuilt from their own main), basis-backfill-psi (wired to `basis-protocol/basis-hub` repo so it auto-deploys going forward).
- **4 backfill services intentionally paused:** basis-backfill-tti, dohi, bri, rpi (`restart_policy: NEVER`, completed April 22, not affected by incident).
- **DB state verified clean:** no behind sequences, extensions correct, schemas correct, row counts consistent with pre-migration volumes. Write traffic safe.

## Hardening pending (open PRs)

1. **basis-hub#132** — deploy-safety PR. Once merged, a future `init_pool()` failure will be retried 3× automatically, then crash the process before port bind, then trigger Railway's `restartPolicyType: ON_FAILURE` rollback. The detection-failure mode from this incident cannot reproduce.

2. **basis-hub#133** — Wave-2 PR. Removes the three remaining pgbouncer-unsafe patterns from worker startup + PSI backfill + wallet indexer. Production-functional today but probabilistically unsafe under load spikes.

## Manual follow-ups (out of scope for PRs)

- **Neon console:** bump `history_retention_seconds` from 21600 (6h) to 604800 (7d).
- **Neon console:** set compute `suspend_timeout_seconds` to 0 (disabled) to eliminate routine cold starts.
- **Canon repo:** copy `basis_protocol_v9_9_constitution_amendment.md` + this punchlist entry from basis-hub `docs/` to the canon repo. Tag commit `transition-2026-05-10`.
- **Claude.ai project knowledge:** manual re-upload of updated CLAUDE.md (Shlok; not automatable from CC).

## Lessons

1. **Replit-managed Neon hid assumptions.** Auto-injected env vars, integration variable refs, libpq option handling, session-pooling guarantees — none of those survive the move to owned Neon. The Wave-2 audit (Tracks B, C, D, E) was the right response: sweep the whole surface rather than just fix the symptom.

2. **Deploy success != deploy works.** Railway's port-bind probe was satisfied even with a dead pool. The healthcheck path (#132) closes this.

3. **Pooler-mode constraints belong in the constitution.** Amendment v9.9 documents the contract.

4. **Sequence sanity is the scariest migration risk.** Even though it didn't bite us here, the audit (Track E §6) confirms the migration was clean. Future Postgres migrations should run this check before write traffic resumes.
