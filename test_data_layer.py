#!/usr/bin/env python3
"""
Test: prove the data layer write path works.
Connects to the production database and tests INSERT/SELECT/DELETE
on entity_snapshots_hourly.

Run: python test_data_layer.py
Requires: DATABASE_URL environment variable
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import init_pool, close_pool, get_cursor, fetch_one

def main():
    print("Initializing database pool...")
    init_pool()

    # 1. Check if table exists
    print("\n--- Table existence check ---")
    try:
        row = fetch_one(
            "SELECT COUNT(*) as cnt FROM information_schema.tables "
            "WHERE table_name = 'entity_snapshots_hourly'"
        )
        exists = row["cnt"] > 0 if row else False
        print(f"entity_snapshots_hourly exists: {exists}")
        if not exists:
            print("TABLE DOES NOT EXIST. Migration 058 has not been applied.")
            close_pool()
            return
    except Exception as e:
        print(f"Table check failed: {e}")
        close_pool()
        return

    # 2. Check current row count
    print("\n--- Current row count ---")
    row = fetch_one("SELECT COUNT(*) as cnt FROM entity_snapshots_hourly")
    before = row["cnt"] if row else 0
    print(f"Rows before: {before}")

    # 3. Insert a test row
    print("\n--- INSERT test row ---")
    try:
        with get_cursor() as cur:
            cur.execute(
                """INSERT INTO entity_snapshots_hourly
                   (entity_id, entity_type, market_cap, price_usd, snapshot_at)
                   VALUES ('__test__', 'test', 999, 1.0, NOW())"""
            )
        print("INSERT succeeded and committed")
    except Exception as e:
        print(f"INSERT FAILED: {type(e).__name__}: {e}")
        close_pool()
        return

    # 4. Verify row count increased
    print("\n--- Verify write ---")
    row = fetch_one("SELECT COUNT(*) as cnt FROM entity_snapshots_hourly")
    after = row["cnt"] if row else 0
    print(f"Rows after: {after}")
    if after > before:
        print(f"SUCCESS: {after - before} new row(s) written and committed")
    else:
        print("FAILURE: row count did not increase — commit may not be working")

    # 5. Read back the test row
    print("\n--- Read back ---")
    test_row = fetch_one(
        "SELECT entity_id, entity_type, market_cap, price_usd, snapshot_at "
        "FROM entity_snapshots_hourly WHERE entity_id = '__test__'"
    )
    if test_row:
        print(f"Read back: {dict(test_row)}")
    else:
        print("FAILURE: test row not found after INSERT")

    # 6. Clean up
    print("\n--- Cleanup ---")
    try:
        with get_cursor() as cur:
            cur.execute("DELETE FROM entity_snapshots_hourly WHERE entity_id = '__test__'")
        print("Test row deleted")
    except Exception as e:
        print(f"Cleanup failed: {e}")

    # 7. Final count
    row = fetch_one("SELECT COUNT(*) as cnt FROM entity_snapshots_hourly")
    final = row["cnt"] if row else 0
    print(f"Final row count: {final}")

    # 8. Also check a few other tables
    print("\n--- Other data layer tables ---")
    for table in ["liquidity_depth", "exchange_snapshots", "yield_snapshots",
                   "bridge_flows", "peg_snapshots_5m", "mint_burn_events",
                   "dex_pool_ohlcv", "market_chart_history"]:
        try:
            r = fetch_one(f"SELECT COUNT(*) as cnt FROM {table}")
            cnt = r["cnt"] if r else "?"
            # Also check if table exists
            exists = fetch_one(
                "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
                (table,)
            )
            print(f"  {table}: {cnt} rows {'(exists)' if exists else '(MISSING)'}")
        except Exception as e:
            print(f"  {table}: ERROR — {e}")

    close_pool()
    print("\nDone.")


if __name__ == "__main__":
    main()
