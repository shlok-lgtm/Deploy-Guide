#!/usr/bin/env python3
"""
Basis Protocol - Historical Data Import
=========================================
Imports data from pg_dump SQL files into the new database.
Handles deduplication when merging Neon + Replit dumps.

Usage:
    python import_history.py --dir ./dumps/

Expected files in --dir:
    neon_score_history.sql
    replit_score_history.sql
    neon_score_events.sql
    replit_score_events.sql
    historical_prices.sql
    deviation_events.sql
"""

import os
import sys
import argparse
import re
import psycopg2
from psycopg2 import sql
from datetime import datetime


def get_connection():
    url = os.environ.get("DATABASE_URL")
    if not url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)
    return psycopg2.connect(url)


def count_rows(conn, table):
    cur = conn.cursor()
    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table)))
    count = cur.fetchone()[0]
    cur.close()
    return count


def import_sql_file(conn, filepath, table_name):
    """
    Import a pg_dump --data-only SQL file.
    Uses ON CONFLICT DO NOTHING for dedup when importing overlapping dumps.
    """
    if not os.path.exists(filepath):
        print(f"  SKIP: {filepath} not found")
        return 0
    
    filesize = os.path.getsize(filepath) / (1024 * 1024)
    print(f"  Importing {filepath} ({filesize:.1f} MB)...")
    
    before = count_rows(conn, table_name)
    
    # For COPY-based dumps, we need to extract and re-insert the data
    # pg_dump uses COPY ... FROM stdin format
    cur = conn.cursor()
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    # Find the COPY block
    copy_match = re.search(r'(COPY public\.\w+ \([^)]+\) FROM stdin;)\n(.*?)\n\\\.', content, re.DOTALL)
    
    if not copy_match:
        print(f"  WARNING: No COPY block found in {filepath}")
        return 0
    
    copy_header = copy_match.group(1)
    data_block = copy_match.group(2)
    
    # Parse column names from COPY header
    cols_match = re.search(r'\(([^)]+)\)', copy_header)
    if not cols_match:
        print(f"  WARNING: Can't parse columns from {filepath}")
        return 0
    
    columns = [c.strip().strip('"') for c in cols_match.group(1).split(',')]
    
    # Build INSERT ... ON CONFLICT DO NOTHING
    insert_sql = sql.SQL('INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING').format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(sql.Identifier(c) for c in columns),
        sql.SQL(', ').join(sql.Placeholder() for _ in columns)
    )
    
    # Parse tab-separated rows
    rows_imported = 0
    rows_skipped = 0
    batch = []
    
    for line in data_block.strip().split('\n'):
        if not line or line.startswith('\\'):
            continue
        
        values = line.split('\t')
        
        # Convert \N to None, handle types
        processed = []
        for v in values:
            if v == '\\N':
                processed.append(None)
            elif v.startswith('{') and v.endswith('}'):
                # PostgreSQL array literal — keep as-is for TEXT[] columns
                processed.append(v)
            else:
                processed.append(v)
        
        # Pad or trim to match column count
        while len(processed) < len(columns):
            processed.append(None)
        processed = processed[:len(columns)]
        
        batch.append(tuple(processed))
        
        # Execute in batches of 1000
        if len(batch) >= 1000:
            for row in batch:
                try:
                    cur.execute(insert_sql, row)
                    rows_imported += 1
                except Exception as e:
                    rows_skipped += 1
            conn.commit()
            batch = []
    
    # Final batch
    for row in batch:
        try:
            cur.execute(insert_sql, row)
            rows_imported += 1
        except Exception as e:
            rows_skipped += 1
    conn.commit()
    
    after = count_rows(conn, table_name)
    new_rows = after - before
    
    cur.close()
    print(f"  Done: {new_rows} new rows added ({rows_skipped} duplicates skipped)")
    return new_rows


def main():
    parser = argparse.ArgumentParser(description="Import historical data into Basis database")
    parser.add_argument("--dir", required=True, help="Directory containing SQL dump files")
    args = parser.parse_args()
    
    dump_dir = args.dir
    if not os.path.isdir(dump_dir):
        print(f"ERROR: {dump_dir} is not a directory")
        sys.exit(1)
    
    conn = get_connection()
    print(f"Connected to database")
    print(f"Import directory: {dump_dir}\n")
    
    total_new = 0
    
    # 1. Score History (merge both sources)
    print("=== Score History ===")
    total_new += import_sql_file(conn, os.path.join(dump_dir, "neon_score_history.sql"), "score_history")
    total_new += import_sql_file(conn, os.path.join(dump_dir, "replit_score_history.sql"), "score_history")
    
    # 2. Score Events (merge both sources)
    print("\n=== Score Events ===")
    total_new += import_sql_file(conn, os.path.join(dump_dir, "neon_score_events.sql"), "score_events")
    total_new += import_sql_file(conn, os.path.join(dump_dir, "replit_score_events.sql"), "score_events")
    
    # 3. Historical Prices (same in both DBs, import once)
    print("\n=== Historical Prices ===")
    total_new += import_sql_file(conn, os.path.join(dump_dir, "historical_prices.sql"), "historical_prices")
    
    # 4. Deviation Events (same in both DBs, import once)
    print("\n=== Deviation Events ===")
    total_new += import_sql_file(conn, os.path.join(dump_dir, "deviation_events.sql"), "deviation_events")
    
    # 5. Data Provenance (if available)
    print("\n=== Data Provenance ===")
    total_new += import_sql_file(conn, os.path.join(dump_dir, "neon_data_provenance.sql"), "data_provenance")
    total_new += import_sql_file(conn, os.path.join(dump_dir, "replit_data_provenance.sql"), "data_provenance")
    
    # Summary
    print(f"\n{'='*50}")
    print(f"Import complete. Total new rows: {total_new}")
    print(f"\nFinal row counts:")
    for table in ["score_history", "score_events", "historical_prices", "deviation_events", "data_provenance"]:
        try:
            count = count_rows(conn, table)
            print(f"  {table}: {count:,}")
        except:
            print(f"  {table}: (table not found)")
    
    conn.close()


if __name__ == "__main__":
    main()
