"""Seed a disposable Prefect 3 server with the fixture data ``record.py`` snapshots.

Run inside a throwaway venv with ``prefect`` installed and ``PREFECT_API_URL``
(plus ``PREFECT_API_AUTH_STRING``) pointing at the server::

    python test/prefect/seed.py configure   # pools, deployment, variables, blocks, limits, automation
    python test/prefect/seed.py run         # run the fixture flows
    prefect worker start --pool local-pool --run-once   # execute the deployment's run
    python test/prefect/seed.py cancel      # one more deployment run, cancelled

Values planted here are fixture-only; none is a real credential.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import httpx

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))


def api() -> httpx.Client:
    """Raw REST client for the server under ``PREFECT_API_URL``."""
    user, _, pw = os.environ.get("PREFECT_API_AUTH_STRING", ":").partition(":")
    return httpx.Client(base_url=os.environ["PREFECT_API_URL"].rstrip("/") + "/", auth=(user, pw), timeout=30)


def post(c: httpx.Client, path: str, body: dict[str, Any]) -> Any:
    """POST and fail loudly."""
    r = c.post(path, json=body)
    if r.status_code >= 400:
        raise SystemExit(f"POST {path}: {r.status_code} {r.text}")
    return r.json() if r.content else None


def configure() -> None:
    """Work pool, queue, deployment + schedule, variables, blocks, limits, automation."""
    from prefect.blocks.notifications import SlackWebhook
    from prefect.blocks.system import Secret

    with api() as c:
        post(c, "work_pools/", {"name": "local-pool", "type": "process", "description": "Fixture process pool"})
        post(c, "work_pools/local-pool/queues", {"name": "high-priority", "priority": 2, "description": "Urgent"})
        for name, value, tags in (
            ("orders_threshold", 100, ["etl"]),
            ("report_owner", "data-team", []),
            ("warehouse_password", "hunter2-fixture", ["secret"]),
            ("slack_hook", "https://user:pw@hooks.example.com/x", []),
        ):
            post(c, "variables/", {"name": name, "value": value, "tags": tags})
        post(c, "v2/concurrency_limits/", {"name": "warehouse-connections", "limit": 4, "slot_decay_per_second": 0.5})
        post(c, "concurrency_limits/", {"tag": "io", "concurrency_limit": 2})
        post(
            c,
            "automations/",
            {
                "name": "notify-on-failure",
                "description": "Fires when any flow run fails",
                "enabled": True,
                "trigger": {
                    "type": "event",
                    "match": {"prefect.resource.id": "prefect.flow-run.*"},
                    "expect": ["prefect.flow-run.Failed"],
                    "posture": "Reactive",
                    "threshold": 1,
                    "within": 0,
                },
                "actions": [{"type": "do-nothing"}],
            },
        )
        flow = post(c, "flows/", {"name": "etl-orders", "tags": ["etl"]})
        post(
            c,
            "deployments/",
            {
                "name": "nightly",
                "flow_id": flow["id"],
                "work_pool_name": "local-pool",
                "work_queue_name": "default",
                "entrypoint": "flows.py:etl_orders",
                "path": str(HERE),
                "parameters": {"n": 3},
                "tags": ["etl", "nightly"],
                "description": "Nightly orders load",
                "schedules": [{"schedule": {"cron": "0 6 * * *", "timezone": "UTC"}, "active": True}],
                "pull_steps": [
                    {"prefect.deployments.steps.set_working_directory": {"directory": str(HERE)}}
                ],
            },
        )
    Secret(value="postgresql://etl:fixture-pw@db/warehouse").save("warehouse-dsn", overwrite=True)
    SlackWebhook(url="https://hooks.slack.com/services/T000/B000/fixture").save("alerts-slack", overwrite=True)


def deployment_id(c: httpx.Client) -> str:
    """The fixture deployment's id."""
    r = c.get("deployments/name/etl-orders/nightly")
    r.raise_for_status()
    return str(r.json()["id"])


def run() -> None:
    """Run the fixture flows locally (they report to the server), then queue a deployment run."""
    import flows
    from prefect import tags

    flows.COUNTER.unlink(missing_ok=True)
    with tags("etl", "orders"):
        flows.etl_orders(5)
        flows.etl_orders(8)
    with tags("reports"):
        flows.flaky_report()
    try:
        flows.broken_import()
    except ValueError:
        pass
    with api() as c:
        post(c, f"deployments/{deployment_id(c)}/create_flow_run", {"parameters": {"n": 4}, "tags": ["adhoc"]})


def cancel() -> None:
    """Create one more deployment run and cancel it."""
    with api() as c:
        fr = post(c, f"deployments/{deployment_id(c)}/create_flow_run", {"tags": ["adhoc"]})
        post(c, f"flow_runs/{fr['id']}/set_state", {"state": {"type": "CANCELLED", "name": "Cancelled"}, "force": True})


if __name__ == "__main__":
    {"configure": configure, "run": run, "cancel": cancel}[sys.argv[1]]()
