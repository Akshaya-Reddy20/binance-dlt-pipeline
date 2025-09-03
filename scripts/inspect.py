# scripts/inspect.py
import duckdb, os, sys

DB = os.environ.get("DUCKDB_DATABASE", "./binance.duckdb")
con = duckdb.connect(DB)

def show_tables():
    print("== Schemas ==")
    print(con.sql("SELECT schema_name FROM information_schema.schemata ORDER BY 1").fetchall())
    print("\n== Tables in binance_raw ==")
    for row in con.sql("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='binance_raw' ORDER BY table_name
    """).fetchall():
        print("-", row[0])

def sample_symbols():
    print("\n== Sample symbols ==")
    for r in con.sql("""
        SELECT symbol, status, base_asset, quote_asset
        FROM binance_raw.binance_symbols
        ORDER BY symbol LIMIT 10
    """).fetchall():
        print(r)

def klines_coverage():
    print("\n== Klines coverage ==")
    for r in con.sql("""
        SELECT symbol, interval,
               MIN(to_timestamp(open_time/1000)) AS first_ts,
               MAX(to_timestamp(open_time/1000)) AS last_ts,
               COUNT(*) AS rows
        FROM binance_raw.binance_klines
        GROUP BY 1,2 ORDER BY symbol, interval
    """).fetchall():
        print(r)

def last_cursor():
    print("\n== Recent pipeline state (top 1) ==")
    r = con.sql("""
        SELECT created_at, state
        FROM binance_raw._dlt_pipeline_state
        ORDER BY created_at DESC LIMIT 1
    """).fetchall()
    if r:
        print(r[0][0], "\nstate=", r[0][1][:200], "...")
    else:
        print("No state found")

if __name__ == "__main__":
    show_tables()
    sample_symbols()
    klines_coverage()
    last_cursor()