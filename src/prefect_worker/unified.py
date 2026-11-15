"""The ``unified`` schema: runs and task runs in the cross-orchestrator shape.

The same two tables (same columns, types and status vocabulary) are served by
the Airflow, Dagster, Prefect, Temporal, Databricks, dbt and GitHub Actions
workers, so ``UNION ALL`` across them just works.

Prefect specifics:

* ``run_id`` is the flow-run id (a UUID, unique in the workspace) and
  ``pipeline`` the flow name.
* ``status`` comes from the state *type*; ``raw_status`` is the state *name*
  (``Late``, ``AwaitingRetry``, ``Cached``, ...). Prefect has no skipped run.
* ``task_runs.attempt`` is Prefect's ``run_count``; task runs outside any flow
  run (background tasks) are left out, because ``run_id`` is NOT NULL.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pyarrow as pa

from .client import PrefectClient
from .meta import column
from .scan import ApiScan, Listing, Pushdown, listing, put, ts
from .tables import flow_run_body, flow_run_url, lookup_names, task_run_body, task_run_url

TSTZ = pa.timestamp("us", tz="UTC")
ORCHESTRATOR = "prefect"
STATUSES = ("queued", "running", "success", "failed", "cancelled", "skipped")

STATE_STATUS = {
    "SCHEDULED": "queued",
    "PENDING": "queued",
    "RUNNING": "running",
    "PAUSED": "running",
    "CANCELLING": "running",
    "COMPLETED": "success",
    "FAILED": "failed",
    "CRASHED": "failed",
    "CANCELLED": "cancelled",
}


def unified_status(state_type: str | None) -> str:
    """Map a Prefect StateType onto the unified vocabulary (NULL / unknown = queued)."""
    return STATE_STATUS.get(str(state_type or "").upper(), "queued")


def trigger(run: dict[str, Any]) -> str:
    """schedule, event, other (subflow), manual or api for one flow run."""
    created_by = run.get("created_by") if isinstance(run.get("created_by"), dict) else {}
    kind = str((created_by or {}).get("type") or "").upper()
    if run.get("auto_scheduled") or kind == "SCHEDULE":
        return "schedule"
    if kind == "AUTOMATION":
        return "event"
    if run.get("parent_task_run_id"):
        return "other"
    if kind == "USER":
        return "manual"
    if run.get("deployment_id"):
        return "api"
    return "manual"


def _inverse(statuses: list[Any]) -> list[str]:
    wanted = {str(s) for s in statuses}
    return sorted(raw for raw, status in STATE_STATUS.items() if status in wanted)


class Remapped(Pushdown):
    """A ``Pushdown`` view whose columns were renamed / values translated for reuse."""

    def __init__(self, values: dict[str, list[Any]], bounds: dict[str, tuple[Any, bool, Any, bool]]) -> None:
        """Hold precomputed constraints."""
        super().__init__(None)
        self._values = values
        self._bounds = bounds

    def values(self, column: str) -> list[Any] | None:
        """Return translated exact values for a native column."""
        return self._values.get(column)

    def bounds(self, column: str) -> tuple[Any, bool, Any, bool] | None:
        """Return the translated range for a native column."""
        return self._bounds.get(column)


def _remap(pd: Pushdown, names: dict[str, str]) -> Remapped | None:
    """Translate unified columns to native ones; None when the status filter admits nothing."""
    values: dict[str, list[Any]] = {}
    for src, dst in names.items():
        vals = pd.values(src)
        if vals is not None:
            values[dst] = vals
    statuses = pd.values("status")
    if statuses is not None:
        raw = _inverse(statuses)
        if not raw:
            return None
        values["state_type"] = raw
    bounds = {}
    for src, dst in (("started_at", "start_time"), ("ended_at", "end_time")):
        b = pd.bounds(src)
        if b is not None:
            bounds[dst] = b
    return Remapped(values, bounds)


def _duration(item: dict[str, Any]) -> float | None:
    started, ended = ts(item.get("start_time")), ts(item.get("end_time"))
    return (ended - started).total_seconds() if started and ended else None


def task_name(task_run: dict[str, Any]) -> str:
    """Return the task's name: the run name without the ``-<3 chars of dynamic_key>`` suffix Prefect appends."""
    name = str(task_run.get("name") or "")
    suffix = "-" + str(task_run.get("dynamic_key") or "")[:3]
    return name.removesuffix(suffix) if len(suffix) > 1 and name.endswith(suffix) else name


RUN_FIELDS = [
    column("orchestrator", pa.string(), "Always 'prefect'.", nullable=False),
    column("run_id", pa.string(), "Flow-run id (UUID).", nullable=False),
    column("pipeline", pa.string(), "Flow name."),
    column("status", pa.string(), "queued, running, success, failed or cancelled.", nullable=False),
    column("raw_status", pa.string(), "Prefect's state name, e.g. 'Completed', 'Crashed', 'Late'."),
    column("trigger", pa.string(), "schedule, event (automation), api (deployment run), manual or other (subflow)."),
    column("queued_at", TSTZ, "When the flow run was created."),
    column("started_at", TSTZ, "When the run started."),
    column("ended_at", TSTZ, "When the run finished."),
    column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
    column("url", pa.string(), "Link to the flow run in the Prefect UI."),
]

TASK_RUN_FIELDS = [
    column("orchestrator", pa.string(), "Always 'prefect'.", nullable=False),
    column("run_id", pa.string(), "Flow-run id, joins unified.runs.run_id.", nullable=False),
    column("task_run_id", pa.string(), "Task-run id (UUID).", nullable=False),
    column("task_name", pa.string(), "Task name (the run name without Prefect's 3-character suffix)."),
    column("status", pa.string(), "queued, running, success, failed or cancelled.", nullable=False),
    column("raw_status", pa.string(), "Prefect's state name, e.g. 'Completed', 'Retrying', 'Failed'."),
    column("attempt", pa.int32(), "Attempts made so far (Prefect run_count; 0 = never started)."),
    column("started_at", TSTZ, "When the task run first started."),
    column("ended_at", TSTZ, "When the task run finished."),
    column("duration_seconds", pa.float64(), "ended_at - started_at in seconds."),
    column("url", pa.string(), "Link to the task run in the Prefect UI."),
]


class UnifiedRuns(ApiScan):
    """Rows for ``prefect.unified.runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(RUN_FIELDS)

    class Meta:
        """Table metadata."""

        name = "unified_runs_scan"
        description = "Scan behind the unified.runs table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """run_id → id, pipeline → flow name, status → state types, raw_status → state names, time ranges."""
        native = _remap(pd, {"run_id": "id", "pipeline": "flow_name", "raw_status": "state_name"})
        body = None if native is None else flow_run_body(native)
        return [] if body is None else [listing("/flow_runs/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map flow runs to unified rows."""
        flows = lookup_names(client, "flows", {r["flow_id"] for r in items}) if "pipeline" in wanted else {}
        return [
            {
                "orchestrator": ORCHESTRATOR,
                "run_id": r["id"],
                "pipeline": flows.get(r["flow_id"]),
                "status": unified_status(r.get("state_type")),
                "raw_status": r.get("state_name"),
                "trigger": trigger(r),
                "queued_at": ts(r.get("created")),
                "started_at": ts(r.get("start_time")),
                "ended_at": ts(r.get("end_time")),
                "duration_seconds": _duration(r),
                "url": flow_run_url(client, r["id"]),
            }
            for r in items
        ]


class UnifiedTaskRuns(ApiScan):
    """Rows for ``prefect.unified.task_runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(TASK_RUN_FIELDS)

    class Meta:
        """Table metadata."""

        name = "unified_task_runs_scan"
        description = "Scan behind the unified.task_runs table"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """run_id → flow_run_id, task_run_id → id, status → state types, raw_status → state names, ranges.

        ``task_name`` is matched by DuckDB only: Prefect filters on the run name, which carries a suffix.
        """
        native = _remap(pd, {"run_id": "flow_run_id", "task_run_id": "id", "raw_status": "state_name"})
        body = None if native is None else task_run_body(native)
        if body is None:
            return []
        if "flow_run_id" not in body.get("task_runs", {}):
            put(body, "task_runs.flow_run_id.is_null_", False)
        return [listing("/task_runs/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map task runs to unified rows (task runs outside a flow run are skipped)."""
        return [
            {
                "orchestrator": ORCHESTRATOR,
                "run_id": t["flow_run_id"],
                "task_run_id": t["id"],
                "task_name": task_name(t),
                "status": unified_status(t.get("state_type")),
                "raw_status": t.get("state_name"),
                "attempt": t.get("run_count"),
                "started_at": ts(t.get("start_time")),
                "ended_at": ts(t.get("end_time")),
                "duration_seconds": _duration(t),
                "url": task_run_url(client, t["id"]),
            }
            for t in items
            if t.get("flow_run_id")
        ]
