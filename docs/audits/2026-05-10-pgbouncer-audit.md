# PostgreSQL Transaction-Mode Pooling Audit
## 2026-05-10: Basis Protocol Neon Migration

**Audit Context**: On 2026-05-10, Basis Protocol migrated production Postgres from Replit-managed Neon to a directly-owned Neon project using the `-pooler` endpoint (pgbouncer in transaction mode). This pooling mode multiplexes connections at the transaction boundary, making many session-level state changes unsafe. This audit identifies code patterns that do not survive transaction-mode pooling and may break unpredictably under load.

**Key Constraint**: pgbouncer transaction mode resets ALL session state between transactions. Session-scoped settings (SET without LOCAL, session-state variables, prepared statements, advisory locks, LISTEN/NOTIFY, named cursors, etc.) are not preserved across physical connection reuses.

---

## Pattern 1: SET without LOCAL

**Finding**: ✅ CLEAR
- `app/database.py:167` correctly uses `SET LOCAL statement_timeout`
- `app/ops/tools/health_checker.py:39` correctly uses `SET LOCAL statement_timeout`

Both instances properly use `SET LOCAL`, which is scoped to the current transaction and survives pgbouncer multiplexing.

**No findings in this category.**

---

## Pattern 2: Advisory Locks

**Finding**: ✅ CLEAR

Searched for `pg_advisory_lock`, `pg_try_advisory_lock`, etc. across all Python and SQL files.

**Result**: No advisory locks found in codebase. Safe.

---

## Pattern 3: LISTEN / NOTIFY

**Finding**: ✅ CLEAR

Searched for LISTEN and NOTIFY PostgreSQL commands.

**Result**: No LISTEN or NOTIFY usage found. Safe for transaction-mode pooling.

---

## Pattern 4: WITH HOLD Cursors

**Finding**: ✅ CLEAR

Searched for `WITH HOLD` and `withhold=True` patterns.

**Result**: No WITH HOLD cursors found. All cursor usage is within a single transaction and released automatically on commit.

---

## Pattern 5: Named / Server-Side Cursors

**Finding**: ✅ CLEAR

Searched for `cursor(name=...)` patterns in `app/` directory.

**Result**: No named cursors found. All cursor usage via standard `conn.cursor()` which creates unnamed (server-side) cursors but releases them within the transaction scope.

---

## Pattern 6: Temporary Tables Outside Transactions

**Finding**: 🟡 SHOULD-FIX (1 finding)

### Finding 6.1: Historical Price Backfill Temp Table
- **File**: `app/services/historical_backfill.py:73–80`
- **Code**:
  ```python
  cur.execute("""
      CREATE TEMP TABLE _hp_stage (
          coingecko_id VARCHAR(50),
          ts TIMESTAMPTZ,
          price DOUBLE PRECISION,
          market_cap DOUBLE PRECISION,
          volume_24h DOUBLE PRECISION
      ) ON COMMIT DROP
  """)
  ```
- **Risk**: The temp table is created and used within `with get_conn() as conn:`, which wraps the entire operation in a transaction with `conn.commit()` at line 100. **Actually safe** — the `ON COMMIT DROP` semantic applies within a single logical transaction. However, pgbouncer transaction mode means if the underlying physical connection is reused for the next caller before the first transaction commits, there could be edge-case race conditions if multiple concurrent requests hit this code path.
- **Assessment**: Low risk because:
  1. The temp table is created and dropped in a single transaction (lines 70–100).
  2. `ON COMMIT DROP` is the correct semantic for transaction-mode safety.
  3. No blocking risk to statement execution.
- **Recommendation**: Current approach is safe. No changes required. Could document the transaction boundary for future maintainers.

---

## Pattern 7: Prepared Statements

**Finding**: ✅ CLEAR

- **Psycopg2 version**: `psycopg2-binary==2.9.10` (from `requirements.txt:14`)
- **Auto-prepare behavior**: psycopg2 (version 2.x) does NOT auto-prepare statements. Only psycopg3 has the automatic `prepare_threshold` behavior that can conflict with pgbouncer transaction mode.
- **Manual PREPARE**: No `PREPARE` or `EXECUTE` statements found in SQL files.
- **Result**: Safe. No prepared-statement incompatibility.

---

## Pattern 8: Autocommit Assumptions

**Finding**: 🔴 MUST-FIX (2 findings)

### Finding 8.1: Setup Script Direct Connection
- **File**: `setup.py:52–53`
- **Code**:
  ```python
  conn = psycopg2.connect(db_url)
  conn.autocommit = True
  ```
- **Risk**: This is a one-time setup script using a direct connection (not the pool), so it runs against the main (non-pooler) endpoint. Safe by design. However, if this script is ever used in production against the pooler endpoint (e.g., for hot-reload schema changes), it will fail because pgbouncer rejects `autocommit=True` at the session level.
- **Assessment**: Low immediate risk (one-time setup), but fragile.
- **Recommendation**: Document that this script must use the direct (non-pooler) endpoint. Add a comment or environment variable check.

### Finding 8.2: Vacuum Worker VACUUM ANALYZE
- **File**: `app/worker.py:2819`
- **Code**:
  ```python
  _vac_conn = _vac_pg.connect(_vac_url)
  _vac_conn.autocommit = True
  ```
- **Risk**: **CRITICAL**. This creates a direct connection and sets `autocommit = True` to run `VACUUM ANALYZE` on large tables (lines 2822–2844). If this connection ever routes through pgbouncer (e.g., if DATABASE_URL is changed to the pooler endpoint), the autocommit setting will fail. `VACUUM` cannot run in an explicit transaction in PostgreSQL, so this will error.
- **Behavior today**: Likely working because the DATABASE_URL on 2026-05-10 points to the pooler endpoint, so this code path is probably using the pooler. The autocommit setting will be rejected by pgbouncer. **This is currently broken or bypassed.**
- **Recommendation**: Either:
  1. Connect to the direct (unpooled) Neon endpoint for VACUUM, or
  2. Remove VACUUM from worker startup and rely on PostgreSQL autovacuum, or
  3. Disable this code path entirely with a feature flag until a proper solution is in place.
  4. **Do not rely on `conn.autocommit = True` for pgbouncer connections.**

---

## Pattern 9: Long-Running Queries / Transactions (>30s expected)

**Finding**: 🟡 SHOULD-FIX (3 findings)

### Finding 9.1: Historical Price Backfill Sync Loop
- **File**: `app/services/historical_backfill.py:107–160+`
- **Code**: `backfill_coin_sync()` fetches 90-day chunks from CoinGecko and calls `_store_chunk()` repeatedly.
- **Risk**: The function processes multiple 90-day chunks in a loop (line 130), making HTTP requests between database operations. Each `_store_chunk()` call (line 37) wraps a full transaction including temp table creation, bulk insert, and merge (lines 70–100). Total latency could exceed 30s if:
  - CoinGecko API is slow (30s timeout at line 146)
  - Multiple coins being backfilled
  - Network delays accumulate
- **Assessment**: Moderate risk. Each transaction is bounded, but the overall backfill job could stall on slow API responses, holding a connection from the pool.
- **Recommendation**: Add per-transaction timeout and implement exponential backoff for CoinGecko rate limiting. Consider breaking into smaller batches if a single coin backfill takes >30s.

### Finding 9.2: PSI Protocol Backfill Per-Entry Inserts
- **File**: `app/services/psi_backfill.py:112–143` (TVL) and `219–243` (token prices)
- **Code**: 
  ```python
  for entry in tvl_history:  # line 112
      execute("""INSERT INTO historical_protocol_data ...""")
      records += 1
  ```
  ```python
  for ts_ms, price in prices:  # line 219
      execute("""INSERT INTO historical_protocol_data ...""")
      records += 1
  ```
- **Risk**: These loops execute one INSERT per historical data point (potentially hundreds per protocol). Each `execute()` call goes through `get_cursor()` → `get_conn()` → transaction. For a protocol with 1000 days of history, this is 1000 separate transactions, each taking a pool slot briefly. Under high concurrency, this could cause pool saturation and statement timeouts (120s limit per transaction).
- **Assessment**: **SHOULD-FIX**. Not an immediate crash, but inefficient and risky under load.
- **Recommendation**: Batch the inserts using `psycopg2.extras.execute_values()` (similar to `historical_backfill.py:82–88`) to reduce from O(n) transactions to O(1).

### Finding 9.3: Wallet Indexer Async Delete + Insert Loop
- **File**: `app/indexer/pipeline.py:81–113` (holdings) and `116–150` (risk scores)
- **Code**:
  ```python
  for h in holdings:
      await execute_async("""DELETE FROM wallet_graph.wallet_holdings ...""")
      await execute_async("""INSERT INTO wallet_graph.wallet_holdings ...""")
  ```
- **Risk**: For each wallet, there's a DELETE and INSERT transaction per holding (one per token). A wallet with 50 holdings = 100 transactions. If the indexer processes 1000 wallets in parallel, that's 100k concurrent transactions. This is not safe under pgbouncer's 30–50 connection limit and 120s statement timeout.
- **Assessment**: **SHOULD-FIX** (probabilistic, but high-frequency code path).
- **Recommendation**: Batch the delete/insert as a single upsert transaction per wallet:
  ```sql
  WITH updates AS (
    INSERT INTO wallet_graph.wallet_holdings (wallet_address, token_address, ...) 
    VALUES (...) ON CONFLICT (wallet_address, token_address, indexed_at::date)
    DO UPDATE SET ... RETURNING *
  ),
  deletes AS (
    DELETE FROM wallet_graph.wallet_holdings 
    WHERE wallet_address = %s 
      AND indexed_at::date = CURRENT_DATE
      AND (wallet_address, token_address) NOT IN (SELECT ... FROM updates)
  )
  SELECT * FROM updates;
  ```

---

## Pattern 10: Connection-Level Role / Search Path Setup

**Finding**: ✅ CLEAR

Searched for `SET ROLE` and `SET search_path` in Python files.

**Result**: No connection-level role or search_path setup found in application code. Schema-qualified table references (e.g., `wallet_graph.wallets`) are hardcoded, making search_path unnecessary.

---

## Open Questions

1. **Database URL Configuration**: Is `DATABASE_URL` pointing to the `-pooler` (transaction-mode) endpoint or the direct endpoint? Track A's statement_timeout fix assumes pooler. If setup.py or worker.py's VACUUM code runs against pooler with `autocommit=True`, it will fail.

2. **VACUUM Necessity**: Is `VACUUM ANALYZE` on startup (worker.py:2813–2849) required for performance, or can autovacuum handle it? Transaction mode pooling cannot support explicit `VACUUM` without a dedicated unpooled connection.

3. **Backfill Concurrency**: Are `backfill_protocol_tvl()` and `backfill_protocol_token()` called concurrently for multiple protocols? The per-insert loop pattern will cause O(n) transaction count.

4. **Wallet Indexer Parallelism**: How many wallets are processed in parallel by `app/indexer/pipeline.py`? If >5, per-holding delete/insert will saturate the connection pool.

---

## Summary Table

| Pattern | Category | File:Line | Issue | Severity |
|---------|----------|-----------|-------|----------|
| 8.1 | Autocommit | setup.py:52 | Direct conn with autocommit=True; fragile for pooler reuse | DEFER |
| 8.2 | Autocommit | app/worker.py:2819 | VACUUM with autocommit=True; broken on pooler endpoint | **MUST-FIX** |
| 6.1 | Temp Table | app/services/historical_backfill.py:73 | ON COMMIT DROP safe within transaction | DEFER |
| 9.1 | Long TX | app/services/historical_backfill.py:107 | Per-chunk HTTP requests; potential stall >30s | SHOULD-FIX |
| 9.2 | Long TX | app/services/psi_backfill.py:112–243 | Per-entry INSERT loop; O(n) transactions, batching needed | SHOULD-FIX |
| 9.3 | Long TX | app/indexer/pipeline.py:81–150 | Per-holding delete+insert; 100k+ tx under load | SHOULD-FIX |

---

## Recommended Fix Priority

1. **MUST-FIX (Blocking)**: 
   - `app/worker.py:2819` — Move VACUUM to dedicated unpooled endpoint or remove.

2. **SHOULD-FIX (Load-bearing)**:
   - `app/services/psi_backfill.py:112–243` — Batch inserts (low effort, high impact).
   - `app/indexer/pipeline.py:81–150` — Upsert per-wallet, not per-holding (medium effort).

3. **DEFER (Fragile but non-blocking)**:
   - `setup.py:52` — Document pooler endpoint restriction or add env var guard.
   - `app/services/historical_backfill.py:107` — Already safe; add comment for future maintainers.

---

## Audit Completion

**Scanned**: 507 Python/SQL files in /home/user/basis-hub  
**Grep Patterns**: 1–10 per specification  
**Total Findings**: 6 (1 MUST-FIX, 3 SHOULD-FIX, 2 DEFER)  
**Safe Patterns**: 4 (Advisory locks, LISTEN/NOTIFY, WITH HOLD, Named cursors, Prepared statements, Role/search_path)

