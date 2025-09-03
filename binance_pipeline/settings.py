import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

def getenv(key: str, default: str | None = None) -> str | None:
    v = os.getenv(key, default)
    return v if v not in ("", None) else default

ENV = getenv("ENV", "dev")
BASE_URL = getenv("BASE_URL", "https://api.binance.com")

# Symbols list as ["BTCUSDT", "ETHUSDT", ...]
SYMBOLS = [s.strip().upper() for s in getenv("SYMBOLS", "BTCUSDT").split(",") if s.strip()]

INTERVAL = getenv("INTERVAL", "1h")

# Start time seed: ISO8601 or ms since epoch
START_TIME_RAW = getenv("START_TIME", "")

def _resolve_start_time_ms() -> int:
    if START_TIME_RAW:
        # try ms epoch
        if START_TIME_RAW.isdigit():
            return int(START_TIME_RAW)
        # try ISO8601
        try:
            dt = datetime.fromisoformat(START_TIME_RAW.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            pass
    # default: 30 days back from now
    dt = datetime.now(timezone.utc) - timedelta(days=30)
    return int(dt.timestamp() * 1000)

START_TIME_MS = _resolve_start_time_ms()

# Destination
DESTINATION = getenv("DESTINATION", "duckdb").lower()

# DuckDB config
DUCKDB_DATABASE = getenv("DUCKDB_DATABASE", "./binance.duckdb")

# ClickHouse (for later)
CLICKHOUSE_HOST = getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(getenv("CLICKHOUSE_PORT", "8123") or 8123)
CLICKHOUSE_DATABASE = getenv("CLICKHOUSE_DATABASE", "binance")
CLICKHOUSE_USER = getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = getenv("CLICKHOUSE_PASSWORD", "")

# Simple HTTP settings
HTTP_MAX_RETRIES = int(getenv("HTTP_MAX_RETRIES", "3") or 3)
HTTP_RETRY_SLEEP = float(getenv("HTTP_RETRY_SLEEP", "1.0") or 1.0)  # seconds
HTTP_PER_REQUEST_PAUSE = float(getenv("HTTP_PER_REQUEST_PAUSE", "0.2") or 0.2)  # seconds