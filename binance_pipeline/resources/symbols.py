from __future__ import annotations
import dlt

from ..utils.http import get_json

@dlt.resource(
    name="binance_symbols",
    write_disposition="replace",  # full refresh; small table
    primary_key="symbol"
)
def exchange_info():
    """Fetch symbols/filters from /api/v3/exchangeInfo"""
    data = get_json("/api/v3/exchangeInfo")
    symbols = data.get("symbols", [])
    # you can trim/normalize fields here if you want
    for s in symbols:
        # keep only a subset to make the schema sane
        yield {
            "symbol": s.get("symbol"),
            "status": s.get("status"),
            "baseAsset": s.get("baseAsset"),
            "baseAssetPrecision": s.get("baseAssetPrecision"),
            "quoteAsset": s.get("quoteAsset"),
            "quotePrecision": s.get("quotePrecision"),
            "quoteAssetPrecision": s.get("quoteAssetPrecision"),
            "orderTypes": s.get("orderTypes"),
            "isSpotTradingAllowed": s.get("isSpotTradingAllowed"),
            "isMarginTradingAllowed": s.get("isMarginTradingAllowed"),
        }