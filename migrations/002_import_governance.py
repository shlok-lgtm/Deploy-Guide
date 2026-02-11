"""
Migration 002: Import governance data from exports/governance_export.sql
This runs once via the migration system (tracked in the migrations table).
"""

import os
import re
import logging
from io import StringIO

logger = logging.getLogger("migration-002")


def run(get_conn):
    """Import governance data from the SQL export file."""
    sql_file = os.path.join(os.path.dirname(__file__), "..", "exports", "governance_export.sql")
    
    if not os.path.exists(sql_file):
        logger.warning(f"Governance export file not found: {sql_file}")
        return False

    with open(sql_file, 'r') as f:
        sql_content = f.read()

    remap = {
        'public.documents': 'gov_documents',
        'public.stablecoin_mentions': 'gov_stablecoin_mentions',
        'public.metric_mentions': 'gov_metric_mentions',
        'public.analysis_tags': 'gov_analysis_tags',
        'public.crawl_logs': 'gov_crawl_logs',
    }

    copy_pattern = re.compile(
        r'COPY\s+(public\.\w+)\s+\(([^)]+)\)\s+FROM\s+stdin;\n(.*?)\n\\\.', 
        re.DOTALL
    )

    imported = {}

    for match in copy_pattern.finditer(sql_content):
        table_name = match.group(1)
        columns = match.group(2)
        data = match.group(3)

        new_table = remap.get(table_name)
        if not new_table:
            continue

        rows = data.strip().split('\n')
        if not rows or rows[0] == '':
            continue

        col_list = columns.strip()

        cols_to_drop = []
        if new_table == 'gov_metric_mentions' and 'sii_component_id' in col_list:
            cols_to_drop.append('sii_component_id')
        if new_table == 'gov_documents' and 'author_reputation' in col_list:
            cols_to_drop.append('author_reputation')

        if cols_to_drop:
            cols = [c.strip() for c in col_list.split(',')]
            drop_indices = sorted([cols.index(c) for c in cols_to_drop if c in cols], reverse=True)
            for idx in drop_indices:
                cols.pop(idx)
            col_list = ', '.join(cols)

            new_rows = []
            for row in rows:
                fields = row.split('\t')
                for idx in drop_indices:
                    if len(fields) > idx:
                        fields.pop(idx)
                new_rows.append('\t'.join(fields))
            rows = new_rows

        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    copy_sql = f"COPY {new_table} ({col_list}) FROM STDIN"
                    data_buffer = StringIO('\n'.join(rows))
                    cur.copy_expert(copy_sql, data_buffer)

            imported[new_table] = len(rows)
            logger.info(f"  {new_table}: {len(rows)} rows")

        except Exception as e:
            logger.error(f"  {new_table} COPY failed: {e}, trying row-by-row...")
            success = 0
            cols = [c.strip() for c in col_list.split(',')]
            placeholders = ', '.join(['%s'] * len(cols))
            insert_sql = f"INSERT INTO {new_table} ({col_list}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

            for row in rows:
                fields = row.split('\t')
                fields = [None if f == '\\N' else f for f in fields]
                if len(fields) != len(cols):
                    continue
                try:
                    with get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute(insert_sql, fields)
                    success += 1
                except Exception:
                    pass

            imported[new_table] = success
            logger.info(f"  {new_table}: {success}/{len(rows)} rows (row-by-row)")

    tables = ['gov_documents', 'gov_stablecoin_mentions', 'gov_metric_mentions',
              'gov_analysis_tags', 'gov_crawl_logs']
    for table in tables:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT setval(pg_get_serial_sequence('{table}', 'id'),
                                      COALESCE(MAX(id), 1))
                        FROM {table}
                    """)
        except Exception:
            pass

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO migrations (name, applied_at)
                VALUES ('002_import_governance', NOW())
                ON CONFLICT DO NOTHING
            """)

    logger.info(f"Governance import complete: {imported}")
    return True
