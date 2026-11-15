"""Shared machinery for the API-backed tables: pushdown, paging, row conversion.

Every table is a ``TableFunctionGenerator`` subclass of ``ApiScan``. A subclass
declares its ``FIXED_SCHEMA`` and implements two hooks:

* ``requests(client, pushdown)`` turns the pushed-down SQL predicates into one or
  more ``Listing`` objects: an endpoint plus the JSON *filter body* Prefect's
  ``POST /<collection>/filter`` endpoints take. ``=``/``IN`` become ``any_``,
  timestamp ranges ``after_``/``before_``, ``list_contains(tags, 'x')`` becomes
  ``tags.all_``, and so on.
* ``rows(client, items, wanted, listing)`` maps one page of API objects to rows.

The cursor (``ScanState``) is ``(listing index, offset, events page token)``. It
travels with each batch over the HTTP transport, holds no credentials, and lets
a scan resume on any worker process. ``auto_apply_filters`` re-applies every
predicate to each batch, so a translated filter only ever has to *narrow* what
the API returns (a superset is fine), never be exact: Prefect's ``before_`` /
``after_`` are inclusive, and strict SQL bounds are trimmed by DuckDB.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Secret
from vgi.filter_v2 import Call, ColumnRef, Literal, StandardFilterFunction
from vgi.invocation import BindResponse
from vgi.table_filter_pushdown import AndFilter, PushdownFilters, V2ExpressionFilter
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import PREFECT_SECRET_TYPE, NotFoundError, PrefectClient, PrefectConfig

log = logging.getLogger(__name__)

EVENTS_PAGE_LIMIT = 50


@dataclass(slots=True, frozen=True, kw_only=True)
class NoArgs:
    """Catalog tables take no arguments."""


@dataclass(kw_only=True)
class ScanState(ArrowSerializableDataclass):
    """Resumable cursor: which listing, how far into it, and the events page token."""

    listing: int = 0
    offset: int = 0
    token: str = ""


@dataclass(frozen=True, slots=True)
class Listing:
    """One paged API collection: endpoint, JSON filter body, and paging style."""

    path: str
    body_json: str = "{}"
    # "offset": limit/offset paging; "events": /events/filter + /events/filter/next tokens.
    paging: str = "offset"
    # Column values implied by the endpoint (e.g. work_pool_name for /work_pools/<name>/workers).
    context: tuple[tuple[str, Any], ...] = ()
    missing_ok: bool = False

    @property
    def body(self) -> dict[str, Any]:
        """The filter body as a dict."""
        parsed: dict[str, Any] = json.loads(self.body_json)
        return parsed


def listing(
    path: str, body: dict[str, Any] | None = None, *, paging: str = "offset", missing_ok: bool = False, **context: Any
) -> Listing:
    """Build a ``Listing`` from a filter-body dict."""
    return Listing(path, json.dumps(body or {}, sort_keys=True), paging, tuple(sorted(context.items())), missing_ok)


def put(body: dict[str, Any], dotted: str, value: Any) -> None:
    """Set ``body['a']['b']['c'] = value`` for ``dotted='a.b.c'``, creating levels."""
    keys = dotted.split(".")
    node = body
    for key in keys[:-1]:
        node = node.setdefault(key, {})
    node[keys[-1]] = value


def secret_from(params: ProcessParams[Any]) -> dict[str, Any] | None:
    """Return the first resolved ``prefect`` secret, if any."""
    for secret in params.secrets.of_type(PREFECT_SECRET_TYPE):
        return dict(secret)
    return None


def client_from(params: ProcessParams[Any]) -> PrefectClient:
    """Build a client from the secret DuckDB resolved for this call."""
    return PrefectClient(PrefectConfig.from_secret(secret_from(params)))


# --------------------------------------------------------------------- conversions


def ts(value: Any) -> dt.datetime | None:
    """Parse a Prefect ISO-8601 timestamp into an aware datetime."""
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def json_text(value: Any) -> str | None:
    """Serialize a JSON-able value for a JSON column (``None`` stays NULL)."""
    if value is None:
        return None
    return json.dumps(value, sort_keys=True)


def iso(value: Any) -> str:
    """Format a timestamp/date filter constant the way Prefect parses it (UTC, ``Z``)."""
    if isinstance(value, dt.datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        return aware.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day, tzinfo=dt.UTC).isoformat().replace("+00:00", "Z")
    return str(value)


# ------------------------------------------------------------------------ pushdown


class Pushdown:
    """Read-only helpers over the predicates DuckDB pushed into a scan."""

    def __init__(self, filters: PushdownFilters | None) -> None:
        """Wrap the (possibly absent) pushdown filters."""
        self.filters = filters

    def values(self, column: str) -> list[Any] | None:
        """Exact values a column is constrained to (``=`` or ``IN``), else None."""
        if not self.filters:
            return None
        arr = self.filters.get_column_values(column)
        if arr is None:
            return None
        return [v for v in arr.to_pylist() if v is not None]

    def one(self, column: str) -> Any | None:
        """Return the single value a column must equal, else None."""
        vals = self.values(column)
        return vals[0] if vals and len(vals) == 1 else None

    def bounds(self, column: str) -> tuple[Any, bool, Any, bool] | None:
        """``(low, low_inclusive, high, high_inclusive)`` for a column, else None."""
        if not self.filters:
            return None
        b = self.filters.get_column_bounds(column)
        if b is None:
            return None
        lo = b.min_value.as_py() if b.min_value is not None else None
        hi = b.max_value.as_py() if b.max_value is not None else None
        if lo is None and hi is None:
            return None
        return lo, b.min_inclusive, hi, b.max_inclusive

    def _call_constants(self, column: str, function: StandardFilterFunction) -> list[str]:
        """String constants ``c`` of every top-level ``function(column, c)`` predicate."""
        if not self.filters:
            return []
        found: list[str] = []
        stack = list(self.filters.filters)
        while stack:
            f = stack.pop()
            if isinstance(f, AndFilter):
                stack.extend(f.children)
                continue
            if not isinstance(f, V2ExpressionFilter):
                continue
            expr = f.expression
            if (
                isinstance(expr, Call)
                and expr.function == function
                and len(expr.arguments) == 2
                and isinstance(expr.arguments[0], ColumnRef)
                and expr.arguments[0].column_name == column
                and isinstance(expr.arguments[1], Literal)
            ):
                value = expr.arguments[1].value.as_py()
                if isinstance(value, str):
                    found.append(value)
        return sorted(set(found))

    def contains(self, column: str) -> list[str]:
        """Constants of every top-level ``list_contains(column, c)`` predicate."""
        return self._call_constants(column, StandardFilterFunction.LIST_CONTAINS)

    def starts_with(self, column: str) -> list[str]:
        """Constants of every top-level ``starts_with(column, c)`` / ``column LIKE 'c%'`` predicate."""
        return self._call_constants(column, StandardFilterFunction.STARTS_WITH)

    def time_range(self, column: str) -> dict[str, str]:
        """``{'after_': ..., 'before_': ...}`` for a timestamp range (inclusive; DuckDB trims strict edges)."""
        b = self.bounds(column)
        if b is None:
            return {}
        lo, _, hi, _ = b
        out: dict[str, str] = {}
        if isinstance(lo, dt.date):
            out["after_"] = iso(lo)
        if isinstance(hi, dt.date):
            out["before_"] = iso(hi)
        return out


def push_any(body: dict[str, Any], pd: Pushdown, column: str, dotted: str, *, cast: type = str) -> bool:
    """``column = / IN`` → ``dotted.any_``. Returns False when the constraint admits no value."""
    vals = pd.values(column)
    if vals is None:
        return True
    if not vals:
        return False
    put(body, f"{dotted}.any_", sorted({cast(v) for v in vals}))
    return True


def push_range(body: dict[str, Any], pd: Pushdown, column: str, dotted: str) -> None:
    """Timestamp range on ``column`` → ``dotted.after_`` / ``dotted.before_``."""
    rng = pd.time_range(column)
    if rng:
        put(body, dotted, rng)


def push_tags(body: dict[str, Any], pd: Pushdown, column: str, dotted: str) -> None:
    """``list_contains(column, 'x') AND ...`` → ``dotted.all_ = ['x', ...]``."""
    tags = pd.contains(column)
    if tags:
        put(body, f"{dotted}.all_", tags)


# --------------------------------------------------------------------------- base


@init_single_worker
class ApiScan(TableFunctionGenerator[NoArgs, ScanState]):
    """Base class for tables that page through one or more Prefect ``/filter`` listings."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema([])

    class Meta:
        """Overridden by every concrete table."""

        name = "api_scan"
        description = "Base class for Prefect API scans"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[NoArgs],
        *,
        prefect: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(PREFECT_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Declare the fixed schema and ask DuckDB for the ``prefect`` secret."""
        return BindResponse(output_schema=cls.FIXED_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[NoArgs]) -> ScanState:
        """Start at the first listing's first page."""
        return ScanState()

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """Return the listings to read for these pushed-down predicates."""
        raise NotImplementedError

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map one page of API items to rows (dicts keyed by column name)."""
        raise NotImplementedError

    @classmethod
    def emit(cls, params: ProcessParams[Any], out: OutputCollector, rows: list[dict[str, Any]]) -> None:
        """Emit rows as one batch in the bound (projected) schema."""
        schema = params.output_schema
        columns = {f.name: [row.get(f.name) for row in rows] for f in schema}
        out.emit(pa.RecordBatch.from_pydict(columns, schema=schema))

    @classmethod
    def fetch(cls, client: PrefectClient, listing: Listing, state: ScanState) -> tuple[list[dict[str, Any]], bool]:
        """Read the page ``state`` points at; return (items, listing exhausted) and advance ``state``."""
        if listing.paging == "events":
            limit = min(client.config.page_size, EVENTS_PAGE_LIMIT)
            if state.token:
                body = client.request("GET", "/events/filter/next", query={"page-token": state.token})
            else:
                body = client.post(listing.path, {**listing.body, "limit": limit})
            items = list(body.get("events") or [])
            next_page = body.get("next_page")
            token = ""
            if next_page:
                from urllib.parse import parse_qs, urlparse

                token = (parse_qs(urlparse(str(next_page)).query).get("page-token") or [""])[0]
            state.token = token
            return items, not token or not items
        limit = client.config.page_size
        items = list(client.post(listing.path, {**listing.body, "limit": limit, "offset": state.offset}) or [])
        state.offset += len(items)
        return items, len(items) < limit

    @classmethod
    def process(cls, params: ProcessParams[NoArgs], state: ScanState, out: OutputCollector) -> None:
        """Read one page of the current listing, emit it, and advance the cursor."""
        client = client_from(params)
        listings = cls.requests(client, Pushdown(params.current_pushdown_filters))
        wanted = set(params.output_schema.names)
        while state.listing < len(listings):
            current = listings[state.listing]
            try:
                items, done = cls.fetch(client, current, state)
            except NotFoundError:
                if not current.missing_ok:
                    raise
                items, done = [], True
            if done:
                state.listing, state.offset, state.token = state.listing + 1, 0, ""
            if items:
                cls.emit(params, out, cls.rows(client, items, wanted, current))
                return
        out.finish()
