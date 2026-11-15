#!/usr/bin/env python3
"""Offline replay of the Prefect 3 REST API, seeded from a recorded snapshot.

``test/fixtures/prefect3.json`` was recorded by ``test/prefect/record.py`` from a
real Prefect 3.9.3 server (SQLite, ``prefect server start``) that
``test/prefect/seed.py`` filled with the flows in ``test/prefect/flows.py``. This
server replays those objects verbatim and re-implements the parts of Prefect the
worker relies on, so the tests need neither Prefect nor network:

* two mounts of the same data: ``/api`` (a self-hosted server with
  ``PREFECT_SERVER_API_AUTH_STRING=admin:prefect-fixture-pw``, HTTP basic auth)
  and ``/api/accounts/<a>/workspaces/<w>`` (Prefect Cloud, bearer
  ``MOCK_API_KEY``, default ``pnu_fixture_key``); anything else is 401,
* ``POST …/filter`` with ``limit``/``offset`` paging (limit > 200 is a 422, as
  on the real server), ``/events/filter`` + ``/events/filter/next`` page tokens
  (limit > 50 is a 422),
* **filter-body validation**: every key of the JSON body, at every depth, must
  exist in the recorded OpenAPI grammar (``grammar`` in the fixture) or the
  request fails with 422 ``extra_forbidden``, like Prefect's pydantic models.
  So a mistranslated pushdown fails loudly instead of silently returning
  everything,
* the filter semantics the worker pushes down (``any_``, ``all_``, ``like_``,
  ``is_null_``, ``before_``/``after_``, ``ge_``/``le_``, ``eq_``, joins between
  flows / flow runs / task runs / deployments / work pools, event name,
  prefix, resource and occurred window).

Switches (environment): ``MOCK_FIXTURE`` (path), ``MOCK_FLAKY=1`` (the first
request to every path answers 429 + ``Retry-After: 0``), ``MOCK_API_KEY``.
``GET /__requests`` returns the request log (method, path below the mount, JSON
body, query); ``POST /__reset`` clears it.

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
import sys
import threading
import urllib.parse
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
DEFAULT_FIXTURE = HERE.parent / "fixtures" / "prefect3.json"
AUTH_STRING = "admin:prefect-fixture-pw"
CLOUD_MOUNT = re.compile(r"^/api/accounts/[^/]+/workspaces/[^/]+(/.*)$")

# Nested filter keys that live in a differently named object field.
FIELD_ALIASES = {
    ("flow_runs", "state.type"): "state_type",
    ("flow_runs", "state.name"): "state_name",
    ("task_runs", "state.type"): "state_type",
    ("task_runs", "state.name"): "state_name",
}


class HttpError(Exception):
    """An HTTP error response."""

    def __init__(self, status: int, body: Any) -> None:
        super().__init__(str(body))
        self.status = status
        self.body = body


def _ts(value: Any) -> dt.datetime | None:
    if not value:
        return None
    parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.UTC)


def _op(value: Any, op: str, arg: Any) -> bool:
    """Apply one Prefect filter operator to an object's field value."""
    if op == "any_":
        if isinstance(value, list):
            return bool(set(value) & set(arg))
        return value in arg
    if op == "not_any_":
        return value not in arg
    if op == "all_":
        return set(arg) <= set(value or [])
    if op == "like_":
        return value is not None and str(arg).lower() in str(value).lower()
    if op == "startswith_":
        return value is not None and str(value).startswith(str(arg))
    if op == "is_null_":
        return (value in (None, [])) == bool(arg)
    if op == "exists_":
        return (value is not None) == bool(arg)
    if op == "eq_":
        return bool(value == arg)
    if op in ("before_", "after_"):
        when, bound = _ts(value), _ts(arg)
        if when is None or bound is None:
            return False
        return when <= bound if op == "before_" else when >= bound
    if op == "ge_":
        return value is not None and value >= arg
    if op == "le_":
        return value is not None and value <= arg
    if op == "query":
        return str(arg).lower() in str(value or "").lower()
    raise HttpError(501, {"detail": f"mock: filter operator {op!r} is not implemented"})


def match(kind: str, obj: dict[str, Any], flt: dict[str, Any] | None, prefix: str = "") -> bool:
    """Does ``obj`` (an object of ``kind``) satisfy a Prefect filter model instance?"""
    if not flt:
        return True
    results = []
    for key, sub in flt.items():
        if key == "operator" or sub is None:
            continue
        dotted = f"{prefix}{key}"
        if isinstance(sub, dict) and any(isinstance(v, dict) for v in sub.values()):
            results.append(match(kind, obj, sub, f"{dotted}."))
            continue
        field = FIELD_ALIASES.get((kind, dotted), dotted)
        value = obj.get(field)
        if field == "start_time" and kind in ("flow_runs", "task_runs") and value is None:
            # Prefect compares coalesce(start_time, expected_start_time).
            value = obj.get("expected_start_time")
        if isinstance(sub, dict):
            results.append(all(_op(value, op, arg) for op, arg in sub.items() if arg is not None and op != "operator"))
        else:
            raise HttpError(501, {"detail": f"mock: unexpected filter leaf {dotted}={sub!r}"})
    operator = flt.get("operator", "and_")
    return any(results) if operator == "or_" and results else all(results)


class Replay:
    """Fixture data plus Prefect-ish filtering."""

    def __init__(self, fixture: Path) -> None:
        self.snap = json.loads(fixture.read_text())
        self.grammar = self.snap["grammar"]
        self.api_key = os.environ.get("MOCK_API_KEY", "pnu_fixture_key")
        self.flaky = os.environ.get("MOCK_FLAKY") == "1"
        self.seen_paths: set[str] = set()
        self.requests: list[dict[str, Any]] = []
        self.lock = threading.Lock()

    # ------------------------------------------------------------ validation

    def validate(self, template: str, body: dict[str, Any]) -> None:
        """422 for any body key outside the recorded OpenAPI grammar."""
        fields = self.grammar["endpoints"].get(template)
        if fields is None:
            raise HttpError(404, {"detail": f"mock: no recorded grammar for {template}"})
        errors: list[dict[str, Any]] = []

        def walk(model_fields: dict[str, str | None], node: dict[str, Any], loc: list[str]) -> None:
            for key, value in node.items():
                if key not in model_fields:
                    errors.append({"type": "extra_forbidden", "loc": ["body", *loc, key], "msg": "Extra inputs"})
                    continue
                child = model_fields[key]
                if child is not None and isinstance(value, dict):
                    walk(self.grammar["models"][child], value, [*loc, key])
                elif child is not None and value is not None:
                    errors.append({"type": "model_type", "loc": ["body", *loc, key], "msg": "Expected an object"})

        walk(fields, body, [])
        if errors:
            raise HttpError(422, {"exception_message": "Invalid request received.", "exception_detail": errors})

    # ------------------------------------------------------------- relations

    def _ids(self, kind: str, flt: dict[str, Any] | None) -> set[str]:
        return {o["id"] for o in self.snap[kind] if match(kind, o, flt)}

    def related(self, kind: str, body: dict[str, Any]) -> Callable[[dict[str, Any]], bool]:
        """Predicate for the cross-collection sections of a ``kind`` listing body."""
        checks: list[Callable[[dict[str, Any]], bool]] = []
        runs = {r["id"]: r for r in self.snap["flow_runs"]}
        for section, flt in body.items():
            if section == kind or not isinstance(flt, dict) or section in ("filter",):
                continue
            if section == "flows" and kind in ("flow_runs", "deployments"):
                ids = self._ids("flows", flt)
                checks.append(lambda o, ids=ids: o.get("flow_id") in ids)
            elif section == "flows" and kind in ("task_runs", "artifacts"):
                ids = self._ids("flows", flt)
                checks.append(lambda o, ids=ids: runs.get(o.get("flow_run_id") or "", {}).get("flow_id") in ids)
            elif section == "deployments" and kind == "flow_runs":
                ids = self._ids("deployments", flt)
                checks.append(lambda o, ids=ids: o.get("deployment_id") in ids)
            elif section == "flow_runs" and kind in ("task_runs", "artifacts"):
                ids = self._ids("flow_runs", flt)
                checks.append(lambda o, ids=ids: o.get("flow_run_id") in ids)
            elif section == "work_pools" and kind in ("flow_runs", "deployments"):
                names = {p["name"] for p in self.snap["work_pools"] if match("work_pools", p, flt)}
                checks.append(lambda o, names=names: o.get("work_pool_name") in names)
            elif section == "block_types" and kind == "block_documents":
                ids = self._ids("block_types", flt)
                checks.append(lambda o, ids=ids: o.get("block_type_id") in ids)
            else:
                raise HttpError(501, {"detail": f"mock: {section} filters on {kind} are not implemented"})
        return lambda o: all(c(o) for c in checks)

    # ---------------------------------------------------------------- routes

    SORTS: dict[str, tuple[str, bool]] = {
        "NAME_ASC": ("name", False),
        "NAME_DESC": ("name", True),
        "ID_DESC": ("id", True),
        "TIMESTAMP_ASC": ("timestamp", False),
        "TIMESTAMP_DESC": ("timestamp", True),
        "CREATED_DESC": ("created", True),
    }

    def page(self, items: list[dict[str, Any]], body: dict[str, Any]) -> list[dict[str, Any]]:
        limit = body.get("limit", 200)
        if not isinstance(limit, int) or limit > 200 or limit < 0:
            raise HttpError(422, {"detail": "Invalid limit: must be less than or equal to 200."})
        offset = int(body.get("offset") or 0)
        sort = body.get("sort")
        if sort in self.SORTS:
            key, rev = self.SORTS[sort]
            items = sorted(items, key=lambda o: (o.get(key) is None, str(o.get(key) or "")), reverse=rev)
        return items[offset : offset + limit]

    def collection(self, kind: str, template: str, body: dict[str, Any]) -> list[dict[str, Any]]:
        self.validate(template, body)
        rel = self.related(kind, body)
        own = body.get(kind)
        items = [o for o in self.snap[kind] if match(kind, o, own) and rel(o)]
        if kind == "block_documents":
            anon = (own or {}).get("is_anonymous", {"eq_": False})
            items = [o for o in items if match(kind, o, {"is_anonymous": anon})]
            if not body.get("include_secrets"):
                items = [{**o, "data": {k: "********" for k in o.get("data") or {}}} for o in items]
        return self.page(items, body)

    def per_pool(self, kind: str, pool: str, template: str, body: dict[str, Any]) -> list[dict[str, Any]]:
        self.validate(template, body)
        if not any(p["name"] == pool for p in self.snap["work_pools"]):
            raise HttpError(404, {"detail": "Work pool not found."})
        section = "workers" if kind == "workers" else "work_queues"
        items = [
            {k: v for k, v in o.items() if not (kind == "workers" and k == "work_pool_name")}
            for o in self.snap[kind]
            if o.get("work_pool_name") == pool and match(section, o, body.get(section))
        ]
        return self.page(items, body)

    def events(self, body: dict[str, Any], host: str, mount: str) -> dict[str, Any]:
        self.validate("/events/filter", body)
        limit = body.get("limit", 50)
        if not isinstance(limit, int) or limit > 50:
            raise HttpError(422, {"exception_message": "Invalid request received.", "detail": "limit le 50"})
        return self._event_page(body.get("filter") or {}, 0, limit, host, mount)

    def _event_page(self, flt: dict[str, Any], offset: int, limit: int, host: str, mount: str) -> dict[str, Any]:
        occurred = flt.get("occurred") or {}
        since = _ts(occurred.get("since")) or (dt.datetime.now(dt.UTC) - dt.timedelta(days=1))
        until = _ts(occurred.get("until")) or dt.datetime.max.replace(tzinfo=dt.UTC)
        ev = flt.get("event") or {}
        res = flt.get("resource") or {}
        out = []
        for e in self.snap["events"]:
            when = _ts(e["occurred"])
            assert when is not None
            if not since <= when <= until:
                continue
            if ev.get("name") and e["event"] not in ev["name"]:
                continue
            if ev.get("prefix") and not any(e["event"].startswith(p) for p in ev["prefix"]):
                continue
            if res.get("id") and e["resource"].get("prefect.resource.id") not in res["id"]:
                continue
            out.append(e)
        out.sort(key=lambda e: (e["occurred"], e["id"]), reverse=flt.get("order", "DESC") == "DESC")
        page = out[offset : offset + limit]
        next_page = None
        if offset + limit < len(out):
            token = base64.b64encode(json.dumps({"filter": flt, "offset": offset + limit, "limit": limit}).encode())
            next_page = f"http://{host}{mount}/events/filter/next?page-token={token.decode()}"
        return {"events": page, "total": len(out), "next_page": next_page}

    def next_events(self, query: dict[str, list[str]], host: str, mount: str) -> dict[str, Any]:
        try:
            state = json.loads(base64.b64decode(query["page-token"][0]))
        except (KeyError, ValueError) as err:
            raise HttpError(403, {"detail": "Invalid page token"}) from err
        return self._event_page(state["filter"], state["offset"], state["limit"], host, mount)

    def route(self, method: str, path: str, query: dict[str, list[str]], body: Any, host: str, mount: str) -> Any:
        if method == "GET" and path == "/health":
            return True
        if method == "GET" and path == "/events/filter/next":
            return self.next_events(query, host, mount)
        if method != "POST":
            raise HttpError(405, {"detail": "Method Not Allowed"})
        if not isinstance(body, dict):
            raise HttpError(422, {"detail": "body must be a JSON object"})
        simple = {
            "/flows/filter": "flows",
            "/flow_runs/filter": "flow_runs",
            "/task_runs/filter": "task_runs",
            "/logs/filter": "logs",
            "/deployments/filter": "deployments",
            "/work_pools/filter": "work_pools",
            "/work_queues/filter": "work_queues",
            "/v2/concurrency_limits/filter": "concurrency_limits",
            "/automations/filter": "automations",
            "/artifacts/filter": "artifacts",
            "/block_types/filter": "block_types",
            "/block_documents/filter": "block_documents",
            "/variables/filter": "variables",
        }
        if path in simple:
            return self.collection(simple[path], path, body)
        if path == "/events/filter":
            return self.events(body, host, mount)
        m = re.match(r"^/work_pools/([^/]+)/(workers|queues)/filter$", path)
        if m:
            pool = urllib.parse.unquote(m.group(1))
            if m.group(2) == "workers":
                return self.per_pool("workers", pool, "/work_pools/{work_pool_name}/workers/filter", body)
            return self.per_pool("work_queues", pool, "/work_pools/{work_pool_name}/queues/filter", body)
        raise HttpError(404, {"detail": "Not Found"})


class Handler(BaseHTTPRequestHandler):
    """HTTP front end of the replay."""

    replay: Replay

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Quiet: the harness keeps stdout for PORT:<n>."""

    def _send(self, status: int, payload: Any, headers: dict[str, str] | None = None) -> None:
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def _authorized(self, cloud: bool) -> bool:
        header = self.headers.get("Authorization", "")
        if cloud:
            return header == f"Bearer {self.replay.api_key}"
        return header == "Basic " + base64.b64encode(AUTH_STRING.encode()).decode()

    def _handle(self, method: str) -> None:
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        replay = self.replay
        if parsed.path == "/__requests" and method == "GET":
            with replay.lock:
                self._send(200, list(replay.requests))
            return
        if parsed.path == "/__reset" and method == "POST":
            with replay.lock:
                replay.requests.clear()
            self._send(200, {"ok": True})
            return
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length) if length else b""
        cloud_match = CLOUD_MOUNT.match(parsed.path)
        if cloud_match:
            cloud, path = True, cloud_match.group(1)
            mount = parsed.path[: -len(path)]
        elif parsed.path.startswith("/api/"):
            cloud, path, mount = False, parsed.path[len("/api") :], "/api"
        else:
            self._send(404, {"detail": "Not Found"})
            return
        try:
            body = json.loads(raw) if raw else None
        except ValueError:
            self._send(422, {"detail": "invalid JSON"})
            return
        with replay.lock:
            replay.requests.append(
                {"method": method, "path": path, "cloud": cloud, "body": body, "query": query}
            )
            first = path not in replay.seen_paths
            replay.seen_paths.add(path)
        if path != "/health" and not self._authorized(cloud):
            self._send(401, {"exception_message": "Unauthorized"})
            return
        if replay.flaky and first:
            self._send(429, {"detail": "rate limited (mock)"}, {"Retry-After": "0"})
            return
        try:
            result = replay.route(method, path, query, body, self.headers.get("Host", "127.0.0.1"), mount)
        except HttpError as err:
            self._send(err.status, err.body)
            return
        self._send(200, result)

    def do_GET(self) -> None:
        """GET requests."""
        self._handle("GET")

    def do_POST(self) -> None:
        """POST requests."""
        self._handle("POST")


def main() -> None:
    """Serve until killed; print ``PORT:<n>`` once listening."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--fixture", default=os.environ.get("MOCK_FIXTURE", str(DEFAULT_FIXTURE)))
    args = parser.parse_args()
    Handler.replay = Replay(Path(args.fixture))
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    sys.stdout.flush()
    server.serve_forever()


if __name__ == "__main__":
    main()
