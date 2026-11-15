"""HTTP client for the Prefect 3 REST API (Prefect Cloud or a self-hosted server).

Everything that touches the network lives here:

* **Configuration** comes from a DuckDB secret of type ``prefect`` (see
  ``PREFECT_SECRET``). Nothing is read from the environment.
* **Endpoints**: ``API_URL`` is what ``PREFECT_API_URL`` would be: a Prefect Cloud
  workspace (``https://api.prefect.cloud/api/accounts/<account>/workspaces/<workspace>``)
  or a server's ``http://host:4200/api``.
* **Authentication**: a Prefect Cloud API key (``API_KEY``, sent as a bearer token)
  or a self-hosted server's ``PREFECT_SERVER_API_AUTH_STRING`` (``AUTH_STRING``,
  ``'user:password'``, sent as HTTP basic auth). Neither is needed for an open server.
* **Politeness**: every request has a timeout; 429 and 502/503/504 are retried with
  backoff that honours ``Retry-After`` (Prefect Cloud's rate limiter sends it).

Credentials are never written to table-function state or logs: each ``process``
call rebuilds the client from the secret the SDK hands it.
"""

from __future__ import annotations

import base64
import datetime as dt
import email.utils
import json
import logging
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
from vgi.catalog import SecretTypeSpec

log = logging.getLogger(__name__)

PREFECT_SECRET_TYPE = "prefect"
USER_AGENT = "vgi-prefect-worker/0.1"
DEFAULT_PAGE_SIZE = 200
MAX_PAGE_SIZE = 200
MAX_ATTEMPTS = 6
MAX_BACKOFF_SECONDS = 60.0
RETRY_STATUSES = frozenset({429, 502, 503, 504})
DOC_URL = "https://github.com/Query-farm/vgi-prefect#credentials"
_CLOUD_API = re.compile(r"^https://api\.prefect\.(cloud|dev)/api/accounts/([^/]+)/workspaces/([^/]+)$")


def _field(
    name: str, doc: str, type_: pa.DataType | None = None, *, redact: bool = False, required: bool = False
) -> Any:
    meta = {"vgi_doc": doc}
    if redact:
        meta["redact"] = "true"
    if required:
        meta["vgi_required"] = "true"
    return pa.field(name, type_ or pa.string(), metadata=meta)


PREFECT_SECRET = SecretTypeSpec(
    name=PREFECT_SECRET_TYPE,
    description=(
        "Connection to one Prefect 3 API: a Prefect Cloud workspace (API URL plus API key) or a self-hosted "
        "Prefect server (API URL, plus the server's basic-auth string when it sets one)."
    ),
    schema=pa.schema(
        [
            _field(
                "api_url",
                "The PREFECT_API_URL: 'https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>' "
                "for Prefect Cloud, or 'http://host:4200/api' for a self-hosted server.",
                required=True,
            ),
            _field("api_key", "Prefect Cloud API key (pnu_... or a service-account key), sent as a bearer token.", redact=True),
            _field(
                "auth_string",
                "Self-hosted servers started with PREFECT_SERVER_API_AUTH_STRING: the same 'user:password' string, "
                "sent as HTTP basic auth.",
                redact=True,
            ),
            _field(
                "ui_url",
                "Base URL of the Prefect UI for the url columns. Default: derived from api_url "
                "(https://app.prefect.cloud/account/<a>/workspace/<w>, or api_url without /api).",
            ),
            _field("page_size", "Objects requested per API page, 1 to 200 (default 200; events use at most 50).", pa.int64()),
            _field("timeout", "Per-request timeout in seconds (default 30).", pa.float64()),
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)


class PrefectError(RuntimeError):
    """The Prefect API returned an error, or the secret is unusable."""


class NotFoundError(PrefectError):
    """HTTP 404 from the API."""


def _ui_from_api(api_url: str) -> str:
    m = _CLOUD_API.match(api_url)
    if m:
        return f"https://app.prefect.{m.group(1)}/account/{m.group(2)}/workspace/{m.group(3)}"
    return api_url.removesuffix("/api")


@dataclass(frozen=True, slots=True)
class PrefectConfig:
    """Connection settings parsed from a ``prefect`` secret."""

    api_url: str
    api_key: str | None = field(default=None, repr=False)
    auth_string: str | None = field(default=None, repr=False)
    ui_url: str = ""
    page_size: int = DEFAULT_PAGE_SIZE
    timeout: float = 30.0

    @classmethod
    def from_secret(cls, secret: Mapping[str, Any] | None) -> PrefectConfig:
        """Build and validate a config from a resolved secret's key/value pairs.

        Args:
            secret: Secret fields; values may be ``pa.Scalar`` or plain Python.

        Returns:
            The validated configuration.

        Raises:
            PrefectError: No secret, or an invalid field value.

        """
        if not secret:
            raise PrefectError(
                "no Prefect credentials: run CREATE SECRET (TYPE prefect, API_URL "
                "'https://api.prefect.cloud/api/accounts/<a>/workspaces/<w>', API_KEY '...') or (TYPE prefect, "
                f"API_URL 'http://localhost:4200/api') after ATTACH. See {DOC_URL}"
            )

        def get(name: str) -> Any:
            value = secret.get(name)
            if isinstance(value, pa.Scalar):
                value = value.as_py()
            return None if value == "" else value

        api_url = get("api_url")
        if not api_url or not str(api_url).startswith(("http://", "https://")):
            raise PrefectError("prefect secret: API_URL must be set to an http:// or https:// URL")
        api = str(api_url).rstrip("/")
        auth_string = get("auth_string")
        if auth_string is not None and ":" not in str(auth_string):
            raise PrefectError("prefect secret: AUTH_STRING must look like 'user:password'")
        page_size = int(get("page_size") or DEFAULT_PAGE_SIZE)
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise PrefectError(f"prefect secret: PAGE_SIZE must be between 1 and {MAX_PAGE_SIZE}")
        timeout = float(get("timeout") or 30.0)
        ui = str(get("ui_url") or _ui_from_api(api)).rstrip("/")
        api_key = get("api_key")
        return cls(
            api_url=api,
            api_key=str(api_key) if api_key else None,
            auth_string=str(auth_string) if auth_string else None,
            ui_url=ui,
            page_size=page_size,
            timeout=timeout,
        )


def retry_delay(header: str | None, attempt: int) -> float:
    """Seconds to wait before retry ``attempt`` given a ``Retry-After`` header value."""
    if header:
        header = header.strip()
        if header.replace(".", "", 1).isdigit():
            return min(float(header), MAX_BACKOFF_SECONDS)
        try:
            when = email.utils.parsedate_to_datetime(header)
            return max(0.0, min((when - dt.datetime.now(dt.UTC)).total_seconds(), MAX_BACKOFF_SECONDS))
        except (TypeError, ValueError):
            pass
    return min(0.5 * float(2**attempt), MAX_BACKOFF_SECONDS)


class PrefectClient:
    """Authenticated JSON requests against one Prefect API."""

    def __init__(self, config: PrefectConfig) -> None:
        """Bind to a configuration."""
        self.config = config

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        elif self.config.auth_string:
            headers["Authorization"] = "Basic " + base64.b64encode(self.config.auth_string.encode()).decode()
        return headers

    def request(self, method: str, path: str, body: Any = None, query: Mapping[str, str] | None = None) -> Any:
        """Send one request below ``api_url`` with retries; return the decoded JSON body.

        Args:
            method: ``GET`` or ``POST``.
            path: Path below the API URL, e.g. ``/flow_runs/filter``.
            body: JSON body for POST.
            query: Query-string parameters.

        Returns:
            The decoded JSON response (``None`` for an empty body).

        Raises:
            NotFoundError: HTTP 404.
            PrefectError: Any other non-2xx status, or the server is unreachable.

        """
        url = self.config.api_url + path + ("?" + urllib.parse.urlencode(query) if query else "")
        data = None if body is None else json.dumps(body).encode()
        headers = self._headers()
        if data is not None:
            headers["Content-Type"] = "application/json"
        for attempt in range(1, MAX_ATTEMPTS + 1):
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                    raw = resp.read()
                    return json.loads(raw) if raw else None
            except urllib.error.HTTPError as err:
                if err.code in RETRY_STATUSES and attempt < MAX_ATTEMPTS:
                    delay = retry_delay(err.headers.get("Retry-After"), attempt)
                    log.info("prefect: HTTP %s from %s %s; retrying in %.1fs", err.code, method, path, delay)
                    time.sleep(delay)
                    continue
                detail = err.read(800).decode(errors="replace")
                try:
                    parsed = json.loads(detail)
                    if isinstance(parsed, dict):
                        detail = str(parsed.get("detail") or parsed.get("exception_detail") or parsed)
                except ValueError:
                    pass
                where = f"{method} {self.config.api_url}{path}"
                if err.code == 404:
                    raise NotFoundError(f"{where} failed with HTTP 404: {detail}") from None
                if err.code in (401, 403):
                    raise PrefectError(
                        f"{where} failed with HTTP {err.code} (check the prefect secret's API_KEY / AUTH_STRING)"
                    ) from None
                raise PrefectError(f"{where} failed with HTTP {err.code}: {detail}") from None
            except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
                if attempt == MAX_ATTEMPTS:
                    reason = getattr(err, "reason", err)
                    raise PrefectError(f"{method} {url} failed: {reason}") from err
                time.sleep(min(0.5 * 2**attempt, MAX_BACKOFF_SECONDS))
        raise PrefectError(f"{method} {url}: gave up after {MAX_ATTEMPTS} attempts")

    def post(self, path: str, body: Any) -> Any:
        """POST a JSON body (a ``/filter`` call)."""
        return self.request("POST", path, body)

    def ui(self, path: str) -> str:
        """Absolute URL of a page in the Prefect UI."""
        return f"{self.config.ui_url}{path}"


def quote(value: str) -> str:
    """Quote one path segment (work pool names may contain spaces)."""
    return urllib.parse.quote(value, safe="")
