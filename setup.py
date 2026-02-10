#!/usr/bin/env python3
"""
First-time setup: applies database migration and verifies everything works.
Run once after cloning: python setup.py
"""

import os
import sys
import subprocess

def main():
    print("=" * 60)
    print("Basis Protocol — Setup")
    print("=" * 60)
    print()

    # 1. Check .env
    if not os.path.exists(".env"):
        if os.path.exists(".env.example"):
            print("No .env found. Creating from .env.example...")
            print("You MUST edit .env and set your DATABASE_URL and API keys.")
            subprocess.run(["cp", ".env.example", ".env"])
            print()
            print("→ Edit .env now, then run this script again.")
            sys.exit(1)
        else:
            print("ERROR: No .env or .env.example found.")
            sys.exit(1)

    # Load .env
    from dotenv import load_dotenv
    load_dotenv()

    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url or "your" in db_url.lower() or "password" in db_url:
        print("ERROR: DATABASE_URL not configured in .env")
        print("Set it to your Neon PostgreSQL connection string.")
        sys.exit(1)

    print(f"Database: {db_url[:db_url.index('@')]}@...")
    print()

    # 2. Apply migration
    print("Applying database migration...")
    migration_file = "migrations/001_initial_schema.sql"
    if not os.path.exists(migration_file):
        print(f"ERROR: {migration_file} not found")
        sys.exit(1)

    try:
        import psycopg2
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()

        with open(migration_file) as f:
            sql = f.read()
        cur.execute(sql)
        print("  Migration applied ✓")

        # Check tables
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = [r[0] for r in cur.fetchall()]
        print(f"  Tables: {', '.join(tables)}")

        # Check seeded stablecoins
        cur.execute("SELECT COUNT(*) FROM stablecoins")
        count = cur.fetchone()[0]
        print(f"  Stablecoins seeded: {count}")

        cur.close()
        conn.close()
        print()
    except Exception as e:
        print(f"  Migration error: {e}")
        print("  This might be fine if tables already exist.")
        print()

    # 3. Check for historical data
    print("Checking for historical data to import...")
    dumps_dir = os.environ.get("DUMPS_DIR", "./dumps")
    if os.path.exists(dumps_dir):
        sql_files = [f for f in os.listdir(dumps_dir) if f.endswith(".sql")]
        if sql_files:
            print(f"  Found {len(sql_files)} dump files in {dumps_dir}/")
            print(f"  Run: python import_history.py --dir {dumps_dir}")
        else:
            print(f"  No .sql files in {dumps_dir}/")
    else:
        print(f"  No dumps directory found ({dumps_dir})")
        print("  If you have pg_dump files from the old system, put them in ./dumps/")
    print()

    # 4. Test API import
    print("Testing imports...")
    try:
        from app.config import STABLECOIN_REGISTRY, get_scoring_ids
        from app.scoring import calculate_sii, FORMULA_VERSION
        from app.server import app as fastapi_app
        from app.worker import compute_sii_from_components
        print(f"  All modules load ✓")
        print(f"  Formula version: {FORMULA_VERSION}")
        print(f"  Stablecoins: {', '.join(get_scoring_ids())}")
    except ImportError as e:
        print(f"  Import error: {e}")
        print("  Run: pip install -r requirements.txt")
        sys.exit(1)

    print()
    print("=" * 60)
    print("Setup complete! Start with: python main.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
