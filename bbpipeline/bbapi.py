"""Best Buy Products API client.

Handles rate limiting (observed ~1 req/s cap), retries with exponential
backoff, and full-catalog pagination.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from . import config


class RateLimitError(Exception):
    """Raised when Best Buy returns HTTP 403 'per second limit' error."""


class BestBuyAPI:
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client = httpx.Client(
            timeout=config.BESTBUY_TIMEOUT_SECONDS,
            headers={"User-Agent": "ai-garp-bbpipeline/0.1"},
        )
        self._last_fetch_at = 0.0
        self._call_count = 0

    @property
    def call_count(self) -> int:
        return self._call_count

    def _pace(self) -> None:
        now = time.monotonic()
        wait = config.BESTBUY_MIN_INTERVAL_SECONDS - (now - self._last_fetch_at)
        if wait > 0:
            time.sleep(wait)
        self._last_fetch_at = time.monotonic()

    @retry(
        retry=retry_if_exception_type(
            (RateLimitError, httpx.TransportError, httpx.HTTPStatusError)
        ),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=3, max=30),
        reraise=True,
    )
    def _request(self, path: str, params: dict[str, Any]) -> dict:
        self._pace()
        full_params = {**params, "apiKey": self._api_key, "format": "json"}
        url = f"{config.BESTBUY_BASE_URL}{path}"
        resp = self._client.get(url, params=full_params)
        self._call_count += 1
        if resp.status_code == 403 and "per second limit" in resp.text.lower():
            raise RateLimitError(resp.text[:200])
        resp.raise_for_status()
        return resp.json()

    def fetch_page(
        self,
        page: int,
        *,
        show: str = config.SHOW_FIELDS,
        path: str = config.BESTBUY_PRODUCT_PATH,
        page_size: int = config.BESTBUY_MAX_PAGE_SIZE,
    ) -> dict:
        return self._request(
            path, {"page": page, "pageSize": page_size, "show": show}
        )

    def iter_all_products(
        self,
        *,
        show: str = config.SHOW_FIELDS,
        path: str = config.BESTBUY_PRODUCT_PATH,
        page_size: int = config.BESTBUY_MAX_PAGE_SIZE,
        max_pages: int | None = None,
    ) -> Iterator[dict]:
        page = 1
        total_pages: int | None = None
        while True:
            if max_pages is not None and page > max_pages:
                return
            resp = self.fetch_page(page, show=show, path=path, page_size=page_size)
            if total_pages is None:
                total_pages = resp.get("totalPages", 0)
            for product in resp.get("products", []):
                yield product
            if total_pages and page >= total_pages:
                return
            page += 1

    def close(self) -> None:
        self._client.close()
