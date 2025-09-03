from __future__ import annotations
from datetime import datetime, timezone
import dlt

from .. import settings
from ..utils.http import get_json

_LIMIT = 1000

_INTERVAL_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000, "3d": 259_200_000,
    "1w": 604_800_000, "1M": 2_592_000_000,
}

def _to_int(x):
    try: return int(x)
    except: return None

def _to_float(x):
    try: return float(x)
    except: return None

def _now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def _kline_row(arr: list) -> dict:
    return {
        "open_time": _to_int(arr[0]),
        "open": _to_float(arr[1]),
        "high": _to_float(arr[2]),
        "low": _to_float(arr[3]),
        "close": _to_float(arr[4]),
        "volume": _to_float(arr[5]),
        "close_time": _to_int(arr[6]),
        "quote_asset_volume": _to_float(arr[7]),
        "number_of_trades": _to_int(arr[8]),
        "taker_buy_base_volume": _to_float(arr[9]),
        "taker_buy_quote_volume": _to_float(arr[10]),
    }

@dlt.resource(
    name="binance_klines",
    primary_key=["symbol", "interval", "open_time"],
    write_disposition="merge",
)
def klines(
    symbols: list[str] = None,
    interval: str = None,
    start_time_ms: int | None = None,
    incremental=dlt.sources.incremental("open_time", initial_value=settings.START_TIME_MS, last_value_func=max),
):
    if symbols is None:
        symbols = settings.SYMBOLS
    if interval is None:
        interval = settings.INTERVAL

    if interval not in _INTERVAL_MS:
        raise ValueError(f"Unsupported INTERVAL={interval}. Choose one of: {', '.join(_INTERVAL_MS)}")

    # Determine where to start from: saved cursor -> explicit arg -> settings
    cursor_start = (getattr(incremental, "last_value", None)
                    if getattr(incremental, "last_value", None) is not None
                    else settings.START_TIME_MS)
    if start_time_ms is not None:
        cursor_start = start_time_ms

    step_ms = _INTERVAL_MS[interval]
    window_ms = _LIMIT * step_ms
    end_wall_ms = _now_ms()

    for sym in symbols:
        start_ms = cursor_start
        while start_ms < end_wall_ms:
            end_ms = min(start_ms + window_ms - step_ms, end_wall_ms)
            params = {
                "symbol": sym,
                "interval": interval,
                "startTime": start_ms,
                "endTime": end_ms,
                "limit": _LIMIT,
            }
            data = get_json("/api/v3/klines", params=params)
            if not data:
                break

            # Yield rows in ascending open_time; dlt will update the cursor to max(open_time)
            for arr in data:
                row = _kline_row(arr)
                row["symbol"] = sym
                row["interval"] = interval
                yield row

            # Advance window
            if len(data) < _LIMIT:
                start_ms = data[-1][0] + step_ms
            else:
                start_ms = end_ms + step_ms