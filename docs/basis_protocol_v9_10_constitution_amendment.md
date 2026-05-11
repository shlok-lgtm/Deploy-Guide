# Constitution Amendment v9.10 — Pooled Connection Contract

**Date:** 2026-05-11
**Status:** Proposed (Wave-2 of the 2026-05-10 Neon migration audit)
**Supersedes:** (none — establishes a new architectural constraint surfaced by the 2026-05-10 incident)
**Numbering note:** Originally filed as v9.9; renumbered to v9.10 on 2026-05-11 to disambiguate from the v9.9.5–v9.9.9 amendment subseries in canon. v9.9 (without subdecimal) was never assigned in the active chain.

## Forcing function

On 2026-05-10, Basis migrated production Postgres from the Replit-managed
Neon integration to a directly-owned Neon project. Every Python service
crash-looped at startup with:

> ERROR: unsupported startup parameter in options: statement_timeout.
> Please use unpooled connection or remove this parameter from the startup package.

Root cause: `app/database.py::init_pool()` passed
`options="-c statement_timeout=120000"` as a libpq startup parameter to the
new pooler endpoint. Neon's pooler is pgbouncer in transaction mode, which
rejects any libpq startup `options`. Replit-managed Neon had hidden this
incompatibility because its DATABASE_URL pointed at the direct (unpooled)
endpoint by default.

The fix (PR #130) is mechanical. The architectural lesson is not: the
choice of pooler endpoint imposes constraints on every line of database
code in the repo. We are committing to those constraints in this
amendment so future contributors can ship safely without rediscovering
them on a production deploy.

## Decision

**Every Postgres connection in the Basis hub MUST be assumed to be
multiplexed through pgbouncer in transaction mode**, unless it is
explicitly routed to the direct (unpooled) endpoint via the
`_direct_db_url()` helper in `app/worker.py`.

This means a checkout returned by `app.database.get_conn()`:

1. **Cannot rely on session-level state surviving across transactions.**
   Each `BEGIN ... COMMIT` may be served by a different physical server
   connection. Settings that survive include only those set inside the
   current transaction (`SET LOCAL`) or as part of the connect-time URL.
   Settings that do NOT survive include `SET` without `LOCAL`,
   `SET SESSION`, `autocommit = True` toggled mid-flight,
   session-scoped advisory locks (`pg_advisory_lock` without `_xact`),
   `WITH HOLD` cursors, persistent prepared statements
   (psycopg3's `prepare_threshold` auto-prepare), named cursors held
   across commits, and `LISTEN` / `NOTIFY`.

2. **Cannot run statements that are themselves disallowed in
   transactions.** `VACUUM`, `CLUSTER`, `CREATE DATABASE`,
   `ALTER SYSTEM`, etc. cannot run via the pooler. They must use the
   direct endpoint.

3. **Should hold a transaction for as short a time as possible.** Each
   transaction occupies a slot in the pool (`max_conn=50` per service ×
   13 services = 650 client slots). Per-row `execute()` loops scale as
   O(n) transactions and should be replaced with
   `psycopg2.extras.execute_values()` (or equivalent COPY for large
   batches).

## Pooled connection contract

Code calling `get_conn()` / `get_cursor()` MAY rely on:

- The connection is open, healthy, and has TCP keepalives enabled.
- `statement_timeout` is set to 120000 ms for the duration of the
  current transaction via `SET LOCAL` at checkout.
- The transaction will be committed if the block exits cleanly, rolled
  back if it raises.

Code calling `get_conn()` MUST NOT rely on:

- Session-level GUCs persisting across `commit()` / `rollback()`.
- Server-side state (temp tables, prepared statements, advisory locks)
  outliving the current transaction.
- A specific physical connection — connections are pooled and
  multiplexed.

## When to use the direct (unpooled) endpoint

The direct endpoint is the same host without the `-pooler` suffix. It is
appropriate for:

- `VACUUM` / `ANALYZE` / `CLUSTER` / other transaction-incompatible
  maintenance.
- Migrations that must hold session-level locks or run
  `CREATE INDEX CONCURRENTLY`.
- One-shot administrative scripts (`setup.py`, ad-hoc backfills)
  where the convenience of session-level state outweighs the cost of
  the dedicated connection.
- Long-running diagnostics where session-level state (named cursors,
  WITH HOLD) is genuinely needed.

The direct endpoint MUST NOT be used for the app's connection pool
(`init_pool`), worker scoring cycles, or per-request work. Those stay
on `-pooler`.

The canonical helper is `app/worker.py::_direct_db_url()` — it derives
the direct URL by stripping `-pooler` from `DATABASE_URL` and returns
empty string if derivation fails, so callers can fall back gracefully.

## Enforcement

1. Code review: any new file that opens a `psycopg2.connect(...)` (vs
   going through `get_conn()`) should justify why in a code comment and
   reference this amendment.

2. A future pre-commit lint can grep for the patterns Track B audited
   (`SET ` without `LOCAL`, `pg_advisory_lock` without `_xact`,
   `LISTEN`, `NOTIFY`, `WITH HOLD`, `prepare_threshold`, named cursors)
   and warn. Not in scope for this amendment.

3. The pgbouncer audit doc at
   `docs/audits/2026-05-10-pgbouncer-audit.md` is the standing reference
   for what does and doesn't work.

## Scope

In scope: all Python services that talk to Postgres
(`app/server.py`, `app/worker.py`, `app/indexer/*`, `app/services/*`,
backfill scripts, ops tools, governance crawler).

Out of scope: the Solidity contracts, the keeper TypeScript service
(no DB), the React frontend (no DB), dbt
(reads via `DATABASE_URL` parsing — same pooler constraint applies,
covered by Track C's `app/discovery.py::_ensure_pg_env` fix).

## References

- Track A pt1: `basis-hub` PR #130 (immediate fix, merged 2026-05-11).
- Track A pt2: `basis-hub` PR #132 (deploy safety: retry,
  fail-loud, `/healthz`, `/readyz`).
- Track B audit: `docs/audits/2026-05-10-pgbouncer-audit.md`.
- Track C: `basis-hub` PR #131 (Replit-ism purge, merged
  2026-05-11).
- Track E audit: `docs/audits/2026-05-10-neon-state.md`.
- Wave-2 fixes: `basis-hub` PR #133 (VACUUM unpooled, batch
  inserts).
