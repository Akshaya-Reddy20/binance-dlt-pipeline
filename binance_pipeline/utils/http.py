from __future__ import annotations
from typing import Any, Dict, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .. import settings

# A single Session with sensible retries/backoff.
_session = requests.Session()
_retries = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)
_session.mount("https://", HTTPAdapter(max_retries=_retries))
_session.headers.update({
    "Accept": "application/json",
    # Some environments/networks are picky; a UA helps avoid weird blocks.
    "User-Agent": "binance-pipeline/1.0 (+requests)",
    # Avoid lingering sockets in flaky networks
    "Connection": "close",
})

def _normalize_path(path: str) -> str:
    p = path or "/"
    if not p.startswith("/"):
        p = "/" + p
    return p

def get_json(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    Try each configured Binance base URL in order until one works.
    """
    p = _normalize_path(path)

    last_err: Optional[Exception] = None
    for base in settings.BINANCE_BASE_URLS:
        base = base.rstrip("/")
        url = base + p
        try:
            resp = _session.get(
                url,
                params=params,
                timeout=settings.REQUEST_TIMEOUT,
                verify=settings.VERIFY_SSL,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.SSLError as e:
            # TLS handshake/EOF issues—try the next mirror host
            last_err = e
            continue
        except requests.exceptions.RequestException as e:
            # Network/timeouts/etc—try the next mirror host
            last_err = e
            continue

    # If we exhausted all hosts, bubble up the last error with a clearer hint.
    hint = (
        "All Binance hosts failed. If this keeps happening in your environment, you can try:\n"
        "  - export VERIFY_SSL=false   (TEMPORARY ONLY; not recommended in production)\n"
        "  - or set BINANCE_BASE_URLS to a working mirror.\n"
    )
    if last_err:
        raise RuntimeError(f"{hint}Last error: {last_err}") from last_err
    raise RuntimeError(hint + "No hosts configured.")