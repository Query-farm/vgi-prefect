"""Fixture flows run against a disposable Prefect 3 server by ``seed.py``.

They exercise what the worker reads: tags, retries, a failing flow, logs at
several levels, markdown/table/link artifacts and task runs with parameters.
"""

from __future__ import annotations

import os
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_link_artifact, create_markdown_artifact, create_table_artifact

COUNTER = Path(os.environ.get("FIXTURE_COUNTER", "/tmp/vgi-prefect-fixture-counter"))


@task(tags=["io"])
def extract(n: int) -> list[int]:
    get_run_logger().info("extracted %d orders", n)
    return list(range(n))


@task
def transform(rows: list[int]) -> list[int]:
    get_run_logger().debug("doubling %d rows", len(rows))
    return [r * 2 for r in rows]


@task(tags=["io"])
def load(rows: list[int]) -> int:
    log = get_run_logger()
    log.info("loaded %d rows", len(rows))
    create_table_artifact(
        key="orders-summary",
        table=[{"bucket": "small", "orders": 3}, {"bucket": "large", "orders": len(rows) - 3}],
        description="Orders per bucket",
    )
    return len(rows)


@flow(name="etl-orders", log_prints=True)
def etl_orders(n: int = 5) -> int:
    rows = transform(extract(n))
    total = load(rows)
    create_markdown_artifact(
        key="orders-report", markdown=f"# Orders\n\nLoaded **{total}** rows.", description="Run report"
    )
    print(f"done: {total}")
    return total


@task(retries=2, retry_delay_seconds=0)
def render() -> str:
    log = get_run_logger()
    attempts = int(COUNTER.read_text()) if COUNTER.exists() else 0
    COUNTER.write_text(str(attempts + 1))
    if attempts == 0:
        log.error("renderer crashed on attempt %d", attempts + 1)
        raise RuntimeError("renderer crashed")
    log.warning("rendered after %d failed attempt(s)", attempts)
    return "ok"


@flow(name="flaky-report")
def flaky_report() -> str:
    result = render()
    create_link_artifact(key="report-link", link="https://example.com/report", link_text="Report")
    return result


@task
def validate(batch: str) -> None:
    get_run_logger().error("batch %s failed validation", batch)
    raise ValueError(f"bad batch {batch}")


@flow(name="broken-import")
def broken_import(batch: str = "b-42") -> None:
    validate(batch)
