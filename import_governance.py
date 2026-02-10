"""
Import existing governance data from standalone crawler dump.

Usage:
    python import_governance.py --sql-file exports/governance_export.sql

This migrates data from the old schema (documents, stablecoin_mentions, etc.)
into the new gov_* tables in the unified Basis database.
"""

import os
import sys
import re
import logging
import argparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.database import init_pool, get_conn
from app.governance import apply_gov_migration

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gov-import")


def import_from_sql(sql_file: str):
    """
    Import governance data from pg_dump SQL file.
    
    The old schema used tables: documents, stablecoin_mentions, metric_mentions,
    analysis_tags, crawl_logs, sii_components, proposals.
    
    We remap to: gov_documents, gov_stablecoin_mentions, gov_metric_mentions,
    gov_analysis_tags, gov_crawl_logs.
    """
    if not os.path.exists(sql_file):
        logger.error(f"File not found: {sql_file}")
        return
    
    logger.info(f"Reading {sql_file}...")
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    # Table name remapping
    remap = {
        'public.documents': 'gov_documents',
        'public.stablecoin_mentions': 'gov_stablecoin_mentions',
        'public.metric_mentions': 'gov_metric_mentions',
        'public.analysis_tags': 'gov_analysis_tags',
        'public.crawl_logs': 'gov_crawl_logs',
    }
    
    # Column remapping for documents table
    # Old: extra_data was called extra_data (same), but some older dumps used metadata
    
    # Extract COPY blocks
    copy_pattern = re.compile(
        r'COPY\s+(public\.\w+)\s+\(([^)]+)\)\s+FROM\s+stdin;\n(.*?)\n\\\.', 
        re.DOTALL
    )
    
    conn = get_conn()
    imported = {}
    
    for match in copy_pattern.finditer(sql_content):
        table_name = match.group(1)
        columns = match.group(2)
        data = match.group(3)
        
        new_table = remap.get(table_name)
        if not new_table:
            logger.info(f"Skipping table {table_name} (not mapped)")
            continue
        
        rows = data.strip().split('\n')
        if not rows or rows[0] == '':
            continue
        
        logger.info(f"Importing {len(rows)} rows into {new_table}...")
        
        # For documents table, remap column if needed
        col_list = columns.strip()
        
        # Skip sii_component_id FK references that won't exist in new schema
        if new_table == 'gov_metric_mentions' and 'sii_component_id' in col_list:
            # Remove sii_component_id column and corresponding data
            cols = [c.strip() for c in col_list.split(',')]
            fk_idx = cols.index('sii_component_id')
            cols.pop(fk_idx)
            col_list = ', '.join(cols)
            
            new_rows = []
            for row in rows:
                fields = row.split('\t')
                if len(fields) > fk_idx:
                    fields.pop(fk_idx)
                new_rows.append('\t'.join(fields))
            rows = new_rows
        
        try:
            with conn.cursor() as cur:
                # Use COPY for speed
                from io import StringIO
                copy_sql = f"COPY {new_table} ({col_list}) FROM STDIN"
                data_buffer = StringIO('\n'.join(rows))
                cur.copy_expert(copy_sql, data_buffer)
            
            conn.commit()
            imported[new_table] = len(rows)
            logger.info(f"  ✓ {new_table}: {len(rows)} rows")
            
        except Exception as e:
            conn.rollback()
            logger.error(f"  ✗ {new_table} failed: {e}")
            
            # Try row-by-row fallback
            logger.info(f"  Trying row-by-row insert for {new_table}...")
            success = 0
            cols = [c.strip() for c in col_list.split(',')]
            placeholders = ', '.join(['%s'] * len(cols))
            insert_sql = f"INSERT INTO {new_table} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            
            for row in rows:
                fields = row.split('\t')
                # Convert \N to None
                fields = [None if f == '\\N' else f for f in fields]
                if len(fields) != len(cols):
                    continue
                try:
                    with conn.cursor() as cur:
                        cur.execute(insert_sql, fields)
                    conn.commit()
                    success += 1
                except Exception as e2:
                    conn.rollback()
            
            imported[new_table] = success
            logger.info(f"  ✓ {new_table}: {success}/{len(rows)} rows (row-by-row)")
    
    conn.close()
    
    # Fix sequences
    fix_sequences()
    
    logger.info("\n=== Import Summary ===")
    for table, count in imported.items():
        logger.info(f"  {table}: {count} rows")


def fix_sequences():
    """Fix auto-increment sequences after COPY import."""
    conn = get_conn()
    tables = ['gov_documents', 'gov_stablecoin_mentions', 'gov_metric_mentions',
              'gov_analysis_tags', 'gov_crawl_logs']
    
    for table in tables:
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT setval(pg_get_serial_sequence('{table}', 'id'),
                                  COALESCE(MAX(id), 1))
                    FROM {table}
                """)
            conn.commit()
        except Exception:
            conn.rollback()
    
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import governance data")
    parser.add_argument("--sql-file", required=True, help="Path to governance_export.sql")
    args = parser.parse_args()
    
    init_pool()
    apply_gov_migration()
    import_from_sql(args.sql_file)
