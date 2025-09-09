from __future__ import annotations
import argparse
import dlt
from dlt.destinations import postgres as dlt_postgres, duckdb as dlt_duckdb
from . import settings
from .resources.symbols import exchange_info
from .resources.klines import klines

def _make_pipeline() -> dlt.Pipeline:
    dest_name = (settings.DESTINATION or "").lower()

    if dest_name == "postgres":
        dest = dlt_postgres(
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            database=settings.POSTGRES_DATABASE,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASSWORD,
        )
        return dlt.pipeline(
            pipeline_name="binance_pipeline_pg",   # <— new name to avoid old CH state
            destination=dest,
            dataset_name="binance_raw",
        )

    elif dest_name == "duckdb":
        dest = dlt_duckdb(settings.DUCKDB_DATABASE)
        return dlt.pipeline(
            pipeline_name="binance_pipeline_duckdb",
            destination=dest,
            dataset_name="binance_raw",
        )

    else:
        raise ValueError(f"Unknown DESTINATION={settings.DESTINATION}")

def run(load_symbols: bool, load_klines: bool):
    pipe = _make_pipeline()

    resources = []
    if load_symbols:
        resources.append(exchange_info())
    if load_klines:
        resources.append(klines())

    if not resources:
        print("Nothing to load. Use --symbols and/or --klines.")
        return

    info = pipe.run(resources)

    print("\n=== Load Summary ===")
    try:
        for lp in getattr(info, "load_packages", []) or []:
            pkg = getattr(lp, "package_path", None) or getattr(lp, "package_id", "n/a")
            state_obj = getattr(lp, "state", None)
            state = getattr(state_obj, "value", state_obj) if state_obj is not None else "n/a"
            jobs = getattr(lp, "jobs", None)
            jobs_count = len(jobs) if isinstance(jobs, (list, tuple)) else (jobs or 0)
            print(f"- package={pkg} state={state} jobs={jobs_count}")
    except Exception as e:
        print(f"(could not render detailed summary: {e})")
        print(repr(info))

    print(f"Destination: {settings.DESTINATION}")
    if settings.DESTINATION.lower() == "duckdb":
        print(f"DuckDB file: {settings.DUCKDB_DATABASE}")

def main():
    p = argparse.ArgumentParser(description="Binance → DLT → (Postgres/DuckDB)")
    p.add_argument("--symbols", action="store_true", help="Load /api/v3/exchangeInfo")
    p.add_argument("--klines", action="store_true", help="Load historical klines incrementally")
    p.add_argument("--full", action="store_true", help="Load both")
    args = p.parse_args()

    if args.full:
        run(load_symbols=True, load_klines=True)
    else:
        run(load_symbols=args.symbols, load_klines=args.klines)

if __name__ == "__main__":
    main()