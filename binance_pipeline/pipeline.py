from __future__ import annotations
import argparse
import dlt
from dlt.destinations import duckdb as dlt_duckdb, clickhouse as dlt_clickhouse
from . import settings
from .resources.symbols import exchange_info
from .resources.klines import klines

def _make_pipeline() -> dlt.Pipeline:
    if settings.DESTINATION == "duckdb":
        # Pass a destination object, not credentials
        dest = dlt_duckdb(settings.DUCKDB_DATABASE)  # e.g. "./binance.duckdb"
        return dlt.pipeline(
            pipeline_name="binance_pipeline",
            destination=dest,
            dataset_name="binance_raw",
        )

    elif settings.DESTINATION == "clickhouse":
        # Requires: pip install clickhouse-connect
        dest = dlt_clickhouse(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            database=settings.CLICKHOUSE_DATABASE,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            secure=False,  # set True if you terminate TLS
        )
        return dlt.pipeline(
            pipeline_name="binance_pipeline",
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

    # Load all selected resources in one run
    info = pipe.run(resources)

    # Pretty print a small summary
        # Pretty print a small summary (robust to dlt version changes)
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
        # fallback summary
        print(f"(could not render detailed summary: {e})")
        print(repr(info))

    print(f"Destination: {settings.DESTINATION}")
    if settings.DESTINATION == "duckdb":
        print(f"DuckDB file: {settings.DUCKDB_DATABASE}")

def main():
    p = argparse.ArgumentParser(description="Binance → DLT → (DuckDB/ClickHouse)")
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