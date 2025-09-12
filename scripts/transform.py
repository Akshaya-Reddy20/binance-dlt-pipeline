import os
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

load_dotenv()  # expects PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
ORDER = [
    "00_create_schemas.sql",
    "10_dim_symbols.sql",
    "20_stg_klines_1h.sql",
]

def run_sql(cur, path: Path):
    sql = path.read_text()
    cur.execute(sql)

def main():
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "postgres"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "")
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for fname in ORDER:
                fpath = SQL_DIR / fname
                print(f"▶ Running {fname}")
                run_sql(cur, fpath)

            # Build the materialized view BEFORE using it in downstream SQL
            print("▶ Refreshing stg.klines_1h (plain refresh first time)")
            try:
                cur.execute("refresh materialized view stg.klines_1h;")
            except Exception as e:
                # As an option, attempt CONCURRENTLY if a unique index exists
                print(f"   plain refresh failed: {e}. Trying CONCURRENTLY...")
                cur.execute("refresh materialized view concurrently stg.klines_1h;")

            # Now run the daily fact upsert which depends on populated stg.klines_1h
            print("▶ Upserting core.fact_klines_1d")
            cur.execute((SQL_DIR / "30_fact_klines_1d.sql").read_text())

        print("✓ Done.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()