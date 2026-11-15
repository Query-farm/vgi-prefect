"""Snapshot a seeded Prefect 3 server into ``test/fixtures/prefect3.json`` for offline replay.

Run with ``PREFECT_API_URL`` (and ``PREFECT_API_AUTH_STRING`` for a server started
with ``PREFECT_SERVER_API_AUTH_STRING``) pointing at the server ``seed.py`` filled::

    python test/prefect/record.py test/fixtures/prefect3.json

It stores, verbatim:

* every object of each collection the worker reads (POST ``/<collection>/filter``
  with offset paging; events through ``/events/filter`` + ``/events/filter/next``),
* the filter-body grammar from the server's ``/openapi.json``: for each filter
  endpoint, its body fields and, recursively, every filter model's allowed keys.
  ``test/mock_api/server.py`` rejects any key outside that grammar with HTTP 422,
  exactly as Prefect's pydantic models do, so a mistranslated pushdown fails.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import httpx

COLLECTIONS = {
    "flows": "flows/filter",
    "flow_runs": "flow_runs/filter",
    "task_runs": "task_runs/filter",
    "logs": "logs/filter",
    "deployments": "deployments/filter",
    "work_pools": "work_pools/filter",
    "work_queues": "work_queues/filter",
    "concurrency_limits": "v2/concurrency_limits/filter",
    "automations": "automations/filter",
    "artifacts": "artifacts/filter",
    "block_types": "block_types/filter",
    "block_documents": "block_documents/filter",
    "variables": "variables/filter",
}


def client() -> httpx.Client:
    """Client for the server under ``PREFECT_API_URL``."""
    user, _, pw = os.environ.get("PREFECT_API_AUTH_STRING", ":").partition(":")
    return httpx.Client(base_url=os.environ["PREFECT_API_URL"].rstrip("/") + "/", auth=(user, pw), timeout=60)


def page_all(c: httpx.Client, path: str, body: dict[str, Any] | None = None) -> list[Any]:
    """Every object of an offset-paged collection."""
    out: list[Any] = []
    while True:
        r = c.post(path, json={**(body or {}), "limit": 200, "offset": len(out)})
        r.raise_for_status()
        page = r.json()
        out += page
        if len(page) < 200:
            return out


def events(c: httpx.Client) -> list[Any]:
    """Every retained event, oldest first."""
    r = c.post("events/filter", json={"limit": 50, "filter": {"occurred": {"since": "2000-01-01T00:00:00Z"}}})
    r.raise_for_status()
    body = r.json()
    out = list(body["events"])
    while body.get("next_page"):
        token = httpx.URL(body["next_page"]).params["page-token"]
        r = c.get("events/filter/next", params={"page-token": token})
        r.raise_for_status()
        body = r.json()
        out += body["events"]
    return sorted(out, key=lambda e: (e["occurred"], e["id"]))


def _ref(schema: dict[str, Any]) -> str | None:
    if "$ref" in schema:
        return str(schema["$ref"]).rsplit("/", 1)[-1]
    for key in ("anyOf", "allOf", "oneOf"):
        for sub in schema.get(key, []):
            if found := _ref(sub):
                return found
    return None


def grammar(openapi: dict[str, Any]) -> dict[str, Any]:
    """Body fields per filter endpoint, plus each filter model's keys (child model or null)."""
    schemas = openapi["components"]["schemas"]
    models: dict[str, dict[str, str | None]] = {}

    def walk(name: str) -> None:
        if name in models or "properties" not in schemas.get(name, {}):
            return
        props = schemas[name]["properties"]
        models[name] = {}
        for key, sub in props.items():
            child = _ref(sub)
            is_model = child is not None and "properties" in schemas.get(child, {})
            models[name][key] = child if is_model else None
            if is_model and child:
                walk(child)

    endpoints: dict[str, dict[str, str | None]] = {}
    for path, ops in openapi["paths"].items():
        if not (path.endswith("/filter") and "post" in ops):
            continue
        content = ops["post"].get("requestBody", {}).get("content", {}).get("application/json", {})
        body = _ref(content.get("schema", {}))
        if not body:
            continue
        walk(body)
        endpoints[path] = models[body]
    return {"endpoints": endpoints, "models": models}


def main() -> None:
    """Record the snapshot."""
    target = Path(sys.argv[1])
    with client() as c:
        snap: dict[str, Any] = {"prefect_version": c.get("admin/version").json()}
        for name, path in COLLECTIONS.items():
            snap[name] = page_all(c, path)
        snap["workers"] = [
            {**w, "work_pool_name": pool["name"]}
            for pool in snap["work_pools"]
            for w in page_all(c, f"work_pools/{pool['name']}/workers/filter")
        ]
        snap["events"] = events(c)
        snap["grammar"] = grammar(c.get("openapi.json").json())
    # Deployment paths name the recording machine's checkout; keep them machine-neutral.
    text = json.dumps(snap, indent=1, sort_keys=True).replace(str(Path(__file__).resolve().parent), "/opt/flows")
    target.write_text(text + "\n")
    print(f"wrote {target}: " + ", ".join(f"{k}={len(v)}" for k, v in snap.items() if isinstance(v, list)))


if __name__ == "__main__":
    main()
