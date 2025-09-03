from __future__ import annotations
import time
import typing as t
import requests

from .. import settings

class HttpError(RuntimeError):
    pass

def get_json(path: str, params: dict[str, t.Any] | None = None) -> t.Any:
    """Simple GET with basic retry/backoff against Binance REST."""
    url = path if path.startswith("http") else settings.BASE_URL.rstrip("/") + path
    retries = settings.HTTP_MAX_RETRIES
    last_exc: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            time.sleep(settings.HTTP_PER_REQUEST_PAUSE)
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 429:
                # basic respect of rate limit headers if present
                retry_after = float(resp.headers.get("Retry-After", "1"))
                time.sleep(max(retry_after, 1.0))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_exc = e
            if attempt < retries:
                time.sleep(settings.HTTP_RETRY_SLEEP * attempt)
                continue
            break

    raise HttpError(f"GET {url} failed after {retries} attempts: {last_exc!r}")