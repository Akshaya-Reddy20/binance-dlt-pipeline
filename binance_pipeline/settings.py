from __future__ import annotations
import os

# ---- Destination selection ----
DESTINATION = os.getenv("DESTINATION", "postgres").lower()

# ---- Postgres connection ----
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE", "binance_dw")
POSTGRES_USER = os.getenv("POSTGRES_USER", "dlt_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "root")

# ---- DuckDB (optional) ----
DUCKDB_DATABASE = os.getenv("DUCKDB_DATABASE", "./binance.duckdb")

# ---- Binance API ----
# Public REST base (no key needed for the endpoints we use)
BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")

# Start time for klines incremental (default: 2018-01-01 UTC)
START_TIME_MS = int(os.getenv("START_TIME_MS", "1514764800000"))

# HTTP behaviour
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# ---- Binance HTTP fallbacks / TLS options ----
# Comma-separated list of base URLs we'll try in order
BINANCE_BASE_URLS = [
    u.strip() for u in os.getenv(
        "BINANCE_BASE_URLS",
        "https://api.binance.com,https://api1.binance.com,https://api2.binance.com,https://api3.binance.com",
    ).split(",")
    if u.strip()
]

# Set to "false" to temporarily skip TLS verification (ONLY if you must)
VERIFY_SSL = os.getenv("VERIFY_SSL", "true").lower() not in ("0", "false", "no")