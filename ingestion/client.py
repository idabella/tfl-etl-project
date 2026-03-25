"""
client.py
─────────────────────────────────────────────────────────────────────────────
TFL Unified API HTTP client.

Features
────────
- TFL app_key injected on every request (query param)
- Automatic retry with exponential back-off (configurable)
- 429 rate-limit handling (honours Retry-After header)
- Optional pagination support for paginated endpoints
- Per-request and global request timeouts
- Structured error logging

Usage
─────
    from ingestion.client import TFLClient

    client = TFLClient(app_key="YOUR_KEY")
    data = client.get("/AirQuality")
    stops = client.get_paginated("/StopPoint/Mode/tube", page_size=50)
"""

from __future__ import annotations

import logging
import time
from typing import Any, Generator, Iterator

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_BASE_URL = "https://api.tfl.gov.uk"
DEFAULT_CONNECT_TIMEOUT = 10       # seconds until connection established
DEFAULT_READ_TIMEOUT = 60          # seconds to wait for response body
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = 1.5
DEFAULT_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)
DEFAULT_PAGE_SIZE = 100


class TFLClient:
    """
    Thin, resilient HTTP wrapper for the TFL Unified API.

    Parameters
    ----------
    app_key : str
        Your TFL API application key.
    base_url : str
        Base URL, default https://api.tfl.gov.uk.
    max_retries : int
        Total retry attempts for transient failures.
    backoff_factor : float
        Multiplier for exponential back-off between retries.
    connect_timeout : float
        Seconds before giving up on establishing a connection.
    read_timeout : float
        Seconds to wait for the server to send response data.
    """

    def __init__(
        self,
        app_key: str,
        base_url: str = DEFAULT_BASE_URL,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = DEFAULT_READ_TIMEOUT,
    ) -> None:
        if not app_key:
            raise ValueError("TFLClient: 'app_key' must not be empty.")
        self._app_key = app_key
        self._base_url = base_url.rstrip("/")
        self._timeout = (connect_timeout, read_timeout)
        self._session = self._build_session(max_retries, backoff_factor)
        logger.debug("TFLClient initialised (base=%s, max_retries=%d)", base_url, max_retries)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _build_session(self, max_retries: int, backoff_factor: float) -> requests.Session:
        session = requests.Session()
        session.params = {"app_key": self._app_key}  # type: ignore[assignment]
        session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "tfl-etl-pipeline/1.0",
            }
        )
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=DEFAULT_RETRY_STATUS_CODES,
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _full_url(self, path: str) -> str:
        return f"{self._base_url}/{path.lstrip('/')}"

    def _handle_rate_limit(self, response: requests.Response, path: str) -> None:
        retry_after = int(response.headers.get("Retry-After", 60))
        logger.warning(
            "Rate limited (429) on %s. Sleeping %d s …", path, retry_after
        )
        time.sleep(retry_after)

    # ── Public API ────────────────────────────────────────────────────────────

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        retries_on_429: int = 3,
    ) -> Any:
        """
        Execute a GET request against *path* (relative or absolute).

        Parameters
        ----------
        path : str
            API path, e.g. ``/AirQuality``.
        params : dict, optional
            Additional query parameters (merged with app_key).
        retries_on_429 : int
            How many times to retry after a 429 before giving up.

        Returns
        -------
        Any
            Parsed JSON body (list or dict).

        Raises
        ------
        requests.HTTPError
            On non-retriable HTTP errors (4xx except 429, 5xx after retries).
        requests.RequestException
            On network-level failures.
        """
        url = path if path.startswith("http") else self._full_url(path)
        attempts = 0

        while True:
            logger.debug("GET %s (params=%s)", url, params)
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
            except requests.exceptions.RequestException as exc:
                logger.error("Network error on GET %s: %s", url, exc)
                raise

            if resp.status_code == 429:
                attempts += 1
                if attempts > retries_on_429:
                    logger.error("Exceeded 429 retry limit for %s.", url)
                    resp.raise_for_status()
                self._handle_rate_limit(resp, url)
                continue

            if not resp.ok:
                logger.error("HTTP %d on GET %s: %s", resp.status_code, url, resp.text[:300])
                resp.raise_for_status()

            try:
                data = resp.json()
            except ValueError as exc:
                logger.error("Failed to decode JSON from %s: %s", url, exc)
                raise

            logger.debug("GET %s → %d bytes", url, len(resp.content))
            return data

    def get_paginated(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        page_param: str = "page",
        results_key: str | None = None,
    ) -> list[Any]:
        """
        Fetch all pages of a paginated TFL endpoint.

        The TFL API uses integer page numbers starting at 1. This method
        collects all pages until an empty page is returned.

        Parameters
        ----------
        path : str
            API path.
        params : dict, optional
            Base query parameters.
        page_size : int
            Records per page (passed as ``pageSize`` param).
        page_param : str
            Name of the page-number query parameter.
        results_key : str, optional
            If the response is a dict, extract results from this key.

        Returns
        -------
        list
            Concatenated results from all pages.
        """
        base_params: dict[str, Any] = dict(params or {})
        base_params["pageSize"] = page_size

        all_results: list[Any] = []
        page = 1

        while True:
            base_params[page_param] = page
            data = self.get(path, params=base_params)

            if results_key and isinstance(data, dict):
                page_results = data.get(results_key, [])
            elif isinstance(data, list):
                page_results = data
            else:
                page_results = []

            if not page_results:
                break

            all_results.extend(page_results)
            logger.debug("Paginated fetch %s page=%d → %d items", path, page, len(page_results))

            if len(page_results) < page_size:
                break  # last page (partial)

            page += 1

        logger.info("Paginated fetch %s → %d total items", path, len(all_results))
        return all_results

    def get_all_from_generator(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Iterator[Any]:
        """
        Lazy generator that yields individual items from a list response.
        Useful for large endpoints to avoid loading everything into memory.
        """
        data = self.get(path, params=params)
        if isinstance(data, list):
            yield from data
        else:
            yield data

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()
        logger.debug("TFLClient session closed.")

    def __enter__(self) -> "TFLClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
