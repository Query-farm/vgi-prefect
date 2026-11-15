"""The ``main`` schema: Prefect REST resources as tables, with filter pushdown.

Each class below is the scan behind one catalog table. ``requests`` documents,
in code, exactly which SQL predicates reach the API and how they are spelled in
Prefect's JSON filter bodies; everything else is filtered by DuckDB
(``auto_apply_filters`` re-checks every pushed predicate).
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any, ClassVar

import pyarrow as pa

from .client import PrefectClient, quote
from .masking import mask_variable
from .meta import column
from .scan import (
    ApiScan,
    Listing,
    Pushdown,
    json_text,
    listing,
    push_any,
    push_range,
    push_tags,
    put,
    ts,
)

TSTZ = pa.timestamp("us", tz="UTC")
STR_LIST = pa.list_(pa.string())
JSON = pa.string()  # Advertised as JSON in descriptions; DuckDB's json functions read VARCHAR directly.

LEVELS = {10: "debug", 20: "info", 30: "warning", 40: "error", 50: "critical"}
LEVEL_NUMBERS = {v: k for k, v in LEVELS.items()}

_NAME_CACHE: dict[tuple[str, str, str], str] = {}
_NAME_LOCK = threading.Lock()


def level_name(level: Any) -> str | None:
    """Lower-case Python logging level name for a numeric level."""
    if level is None:
        return None
    return LEVELS.get(int(level), f"level {int(level)}")


def lookup_names(client: PrefectClient, collection: str, ids: set[str]) -> dict[str, str]:
    """Resolve flow / deployment ids to names with one filter call (cached per process)."""
    base = client.config.api_url
    out: dict[str, str] = {}
    missing: list[str] = []
    with _NAME_LOCK:
        for i in ids:
            hit = _NAME_CACHE.get((base, collection, i))
            if hit is None:
                missing.append(i)
            else:
                out[i] = hit
    for start in range(0, len(missing), 200):
        chunk = sorted(missing[start : start + 200])
        body = {collection: {"id": {"any_": chunk}}, "limit": 200, "offset": 0}
        for item in client.post(f"/{collection}/filter", body) or []:
            out[str(item["id"])] = str(item["name"])
            with _NAME_LOCK:
                _NAME_CACHE[(base, collection, str(item["id"]))] = str(item["name"])
    return out


def _state(item: dict[str, Any]) -> dict[str, Any]:
    state = item.get("state")
    return state if isinstance(state, dict) else {}


def _created_by(item: dict[str, Any], key: str) -> Any:
    cb = item.get("created_by")
    return cb.get(key) if isinstance(cb, dict) else None


def _json_value(value: Any, *, parse_strings: bool) -> str | None:
    """JSON text for artifact data: a table artifact's string is parsed, other strings become JSON strings."""
    if value is None:
        return None
    if isinstance(value, str) and parse_strings:
        try:
            return json.dumps(json.loads(value), sort_keys=True)
        except ValueError:
            pass
    return json.dumps(value, sort_keys=True)


class _Scan(ApiScan):
    """Common Meta flags; concrete tables set name and description."""

    class Meta:
        """Table metadata."""

        name = "_scan"
        description = "Base"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True


def _meta(name: str) -> type:
    class Meta:
        """Table metadata."""

        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True

    Meta.name = f"{name}_scan"  # type: ignore[attr-defined]
    Meta.description = f"Scan behind the {name} table"  # type: ignore[attr-defined]
    return Meta


# --------------------------------------------------------------------------- flows


class Flows(_Scan):
    """Rows for ``prefect.main.flows``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Flow id (UUID).", nullable=False),
            column("name", pa.string(), "Flow name, unique in the workspace.", nullable=False),
            column("tags", STR_LIST, "Flow tags."),
            column("labels", JSON, "Flow labels as a JSON object."),
            column("created", TSTZ, "When the flow was first registered."),
            column("updated", TSTZ, "When the flow was last updated."),
            column("url", pa.string(), "Link to the flow in the Prefect UI."),
        ]
    )
    Meta = _meta("flows")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """Id and name → ``flows.id/name.any_``; ``list_contains(tags, x)`` → ``flows.tags.all_``."""
        body: dict[str, Any] = {"sort": "NAME_ASC"}
        if not (push_any(body, pd, "id", "flows.id") and push_any(body, pd, "name", "flows.name")):
            return []
        push_tags(body, pd, "tags", "flows.tags")
        return [listing("/flows/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map flow objects."""
        return [
            {
                "id": f["id"],
                "name": f["name"],
                "tags": f.get("tags") or [],
                "labels": json_text(f.get("labels")),
                "created": ts(f.get("created")),
                "updated": ts(f.get("updated")),
                "url": client.ui(f"/flows/flow/{f['id']}"),
            }
            for f in items
        ]


# --------------------------------------------------------------------- deployments


def _deployment_body(pd: Pushdown, *, id_col: str = "id", name_col: str = "name") -> dict[str, Any] | None:
    body: dict[str, Any] = {"sort": "NAME_ASC"}
    ok = (
        push_any(body, pd, id_col, "deployments.id")
        and push_any(body, pd, name_col, "deployments.name")
        and push_any(body, pd, "flow_id", "flows.id")
        and push_any(body, pd, "flow_name", "flows.name")
        and push_any(body, pd, "work_pool_name", "work_pools.name")
        and push_any(body, pd, "work_queue_name", "deployments.work_queue_name")
    )
    if not ok:
        return None
    paused = pd.one("paused")
    if paused is not None:
        put(body, "deployments.paused.eq_", bool(paused))
    push_tags(body, pd, "tags", "deployments.tags")
    return body


class Deployments(_Scan):
    """Rows for ``prefect.main.deployments``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Deployment id (UUID).", nullable=False),
            column("name", pa.string(), "Deployment name (unique per flow).", nullable=False),
            column("flow_id", pa.string(), "Id of the deployed flow.", nullable=False),
            column("flow_name", pa.string(), "Name of the deployed flow."),
            column("description", pa.string(), "Deployment description."),
            column("version", pa.string(), "Deployment version label."),
            column("paused", pa.bool_(), "True when the deployment's schedules are paused."),
            column("status", pa.string(), "READY when a worker polled recently, else NOT_READY."),
            column("tags", STR_LIST, "Deployment tags (added to its runs)."),
            column("work_pool_name", pa.string(), "Work pool the deployment's runs go to."),
            column("work_queue_name", pa.string(), "Work queue within the pool."),
            column("entrypoint", pa.string(), "Flow entrypoint, e.g. 'flows.py:etl'."),
            column("path", pa.string(), "Working directory the entrypoint is relative to."),
            column("parameters", JSON, "Default flow parameters as JSON."),
            column("job_variables", JSON, "Infrastructure overrides as JSON."),
            column("concurrency_limit", pa.int64(), "Maximum concurrent runs of this deployment (count)."),
            column("enforce_parameter_schema", pa.bool_(), "True when run parameters are validated."),
            column("schedule_count", pa.int64(), "Number of schedules attached (see deployment_schedules)."),
            column("last_polled", TSTZ, "When a worker last polled for this deployment's runs."),
            column("created", TSTZ, "When the deployment was created."),
            column("updated", TSTZ, "When the deployment was last updated."),
            column("url", pa.string(), "Link to the deployment in the Prefect UI."),
        ]
    )
    Meta = _meta("deployments")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """Id/name/flow/work pool/queue → filter bodies; paused → ``eq_``; tags → ``all_``."""
        body = _deployment_body(pd)
        return [] if body is None else [listing("/deployments/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map deployment objects."""
        flows = lookup_names(client, "flows", {d["flow_id"] for d in items}) if "flow_name" in wanted else {}
        out = []
        for d in items:
            limit = d.get("concurrency_limit")
            if isinstance(d.get("global_concurrency_limit"), dict):
                limit = d["global_concurrency_limit"].get("limit", limit)
            out.append(
                {
                    "id": d["id"],
                    "name": d["name"],
                    "flow_id": d["flow_id"],
                    "flow_name": flows.get(d["flow_id"]),
                    "description": d.get("description"),
                    "version": d.get("version"),
                    "paused": d.get("paused"),
                    "status": d.get("status"),
                    "tags": d.get("tags") or [],
                    "work_pool_name": d.get("work_pool_name"),
                    "work_queue_name": d.get("work_queue_name"),
                    "entrypoint": d.get("entrypoint"),
                    "path": d.get("path"),
                    "parameters": json_text(d.get("parameters")),
                    "job_variables": json_text(d.get("job_variables")),
                    "concurrency_limit": limit,
                    "enforce_parameter_schema": d.get("enforce_parameter_schema"),
                    "schedule_count": len(d.get("schedules") or []),
                    "last_polled": ts(d.get("last_polled")),
                    "created": ts(d.get("created")),
                    "updated": ts(d.get("updated")),
                    "url": client.ui(f"/deployments/deployment/{d['id']}"),
                }
            )
        return out


class DeploymentSchedules(_Scan):
    """Rows for ``prefect.main.deployment_schedules`` (flattened from deployments)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("schedule_id", pa.string(), "Schedule id (UUID).", nullable=False),
            column("deployment_id", pa.string(), "Deployment the schedule belongs to.", nullable=False),
            column("deployment_name", pa.string(), "Name of that deployment."),
            column("active", pa.bool_(), "True when the schedule creates runs."),
            column("cron", pa.string(), "Cron expression, for cron schedules."),
            column("day_or", pa.bool_(), "Cron day-of-month/day-of-week combination: OR (true) or AND."),
            column("interval_seconds", pa.float64(), "Interval in seconds, for interval schedules."),
            column("anchor_date", TSTZ, "Anchor of an interval schedule."),
            column("rrule", pa.string(), "RFC 5545 recurrence rule, for RRule schedules."),
            column("timezone", pa.string(), "IANA timezone the schedule is evaluated in."),
            column("max_scheduled_runs", pa.int64(), "Cap on runs scheduled ahead (count)."),
            column("parameters", JSON, "Parameter overrides for runs of this schedule, as JSON."),
            column("slug", pa.string(), "Optional schedule slug."),
            column("created", TSTZ, "When the schedule was created."),
            column("updated", TSTZ, "When the schedule was last updated."),
        ]
    )
    Meta = _meta("deployment_schedules")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """deployment_id / deployment_name → ``deployments.id/name.any_``."""
        body: dict[str, Any] = {"sort": "NAME_ASC"}
        ok = push_any(body, pd, "deployment_id", "deployments.id") and push_any(
            body, pd, "deployment_name", "deployments.name"
        )
        return [listing("/deployments/filter", body)] if ok else []

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """One row per schedule of each deployment."""
        out = []
        for d in items:
            for s in d.get("schedules") or []:
                spec = s.get("schedule") or {}
                interval = spec.get("interval")
                out.append(
                    {
                        "schedule_id": s["id"],
                        "deployment_id": d["id"],
                        "deployment_name": d["name"],
                        "active": s.get("active"),
                        "cron": spec.get("cron"),
                        "day_or": spec.get("day_or"),
                        "interval_seconds": float(interval) if isinstance(interval, int | float) else None,
                        "anchor_date": ts(spec.get("anchor_date")),
                        "rrule": spec.get("rrule"),
                        "timezone": spec.get("timezone"),
                        "max_scheduled_runs": s.get("max_scheduled_runs"),
                        "parameters": json_text(s.get("parameters")),
                        "slug": s.get("slug"),
                        "created": ts(s.get("created")),
                        "updated": ts(s.get("updated")),
                    }
                )
        return out


# ------------------------------------------------------------------------ flow runs


def flow_run_body(pd: Pushdown) -> dict[str, Any] | None:
    """Filter body for ``/flow_runs/filter`` from native flow_runs columns (None = no rows can match)."""
    body: dict[str, Any] = {"sort": "ID_DESC"}
    ok = (
        push_any(body, pd, "id", "flow_runs.id")
        and push_any(body, pd, "name", "flow_runs.name")
        and push_any(body, pd, "flow_id", "flows.id")
        and push_any(body, pd, "flow_name", "flows.name")
        and push_any(body, pd, "deployment_id", "flow_runs.deployment_id")
        and push_any(body, pd, "deployment_name", "deployments.name")
        and push_any(body, pd, "work_pool_name", "work_pools.name")
        and push_any(body, pd, "work_queue_name", "flow_runs.work_queue_name")
        and push_any(body, pd, "state_type", "flow_runs.state.type", cast=lambda v: str(v).upper())
        and push_any(body, pd, "state_name", "flow_runs.state.name")
        and push_any(body, pd, "parent_task_run_id", "flow_runs.parent_task_run_id")
    )
    if not ok:
        return None
    push_tags(body, pd, "tags", "flow_runs.tags")
    push_range(body, pd, "start_time", "flow_runs.start_time")
    push_range(body, pd, "end_time", "flow_runs.end_time")
    push_range(body, pd, "expected_start_time", "flow_runs.expected_start_time")
    return body


def flow_run_url(client: PrefectClient, run_id: str) -> str:
    """UI link to a flow run."""
    return client.ui(f"/runs/flow-run/{run_id}")


def task_run_url(client: PrefectClient, task_run_id: str) -> str:
    """UI link to a task run."""
    return client.ui(f"/runs/task-run/{task_run_id}")


class FlowRuns(_Scan):
    """Rows for ``prefect.main.flow_runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Flow run id (UUID).", nullable=False),
            column("name", pa.string(), "Generated run name, e.g. 'flawless-mustang'.", nullable=False),
            column("flow_id", pa.string(), "Id of the flow.", nullable=False),
            column("flow_name", pa.string(), "Name of the flow."),
            column("deployment_id", pa.string(), "Deployment that created the run; NULL for direct calls."),
            column("deployment_name", pa.string(), "Name of that deployment."),
            column("work_pool_name", pa.string(), "Work pool the run was submitted to."),
            column("work_queue_name", pa.string(), "Work queue the run was submitted to."),
            column("state_type", pa.string(), "Prefect StateType: COMPLETED, FAILED, CRASHED, RUNNING, SCHEDULED, ..."),
            column("state_name", pa.string(), "State display name, e.g. 'Late', 'AwaitingRetry', 'Cached'."),
            column("state_message", pa.string(), "Message attached to the current state (often the error)."),
            column("state_timestamp", TSTZ, "When the run entered its current state."),
            column("tags", STR_LIST, "Run tags."),
            column("parameters", JSON, "Flow parameters of the run, as JSON."),
            column("run_count", pa.int64(), "Times the flow run has started (count)."),
            column("max_retries", pa.int64(), "Configured flow retries (count)."),
            column("expected_start_time", TSTZ, "When the run was scheduled to start."),
            column("next_scheduled_start_time", TSTZ, "Next scheduled start, while scheduled."),
            column("start_time", TSTZ, "When the run started."),
            column("end_time", TSTZ, "When the run finished."),
            column("total_run_time", pa.float64(), "Time spent running, in seconds."),
            column("estimated_run_time", pa.float64(), "Run time so far for a running run, in seconds."),
            column("estimated_start_time_delta", pa.float64(), "Start delay vs. expected_start_time, in seconds."),
            column("auto_scheduled", pa.bool_(), "True when a deployment schedule created the run."),
            column("parent_task_run_id", pa.string(), "For subflow runs: the parent's task run id."),
            column("idempotency_key", pa.string(), "Idempotency key the run was created with."),
            column("flow_version", pa.string(), "Flow version (source hash) the run executed."),
            column("infrastructure_pid", pa.string(), "Process / job id on the worker's infrastructure."),
            column("created_by_type", pa.string(), "What created the run: SCHEDULE, AUTOMATION, USER, ... or NULL."),
            column("created_by_name", pa.string(), "Display name of what created the run."),
            column("labels", JSON, "Run labels as a JSON object (flow, deployment, worker, pool ids)."),
            column("created", TSTZ, "When the run was created."),
            column("updated", TSTZ, "When the run was last updated."),
            column("url", pa.string(), "Link to the flow run in the Prefect UI."),
        ]
    )
    Meta = _meta("flow_runs")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """See ``flow_run_body``: ids, names, flow, deployment, pool, queue, state, tags, time ranges."""
        body = flow_run_body(pd)
        return [] if body is None else [listing("/flow_runs/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map flow-run objects; flow and deployment names are looked up only when selected."""
        flows = lookup_names(client, "flows", {r["flow_id"] for r in items}) if "flow_name" in wanted else {}
        deps = (
            lookup_names(client, "deployments", {r["deployment_id"] for r in items if r.get("deployment_id")})
            if "deployment_name" in wanted
            else {}
        )
        out = []
        for r in items:
            state = _state(r)
            out.append(
                {
                    "id": r["id"],
                    "name": r["name"],
                    "flow_id": r["flow_id"],
                    "flow_name": flows.get(r["flow_id"]),
                    "deployment_id": r.get("deployment_id"),
                    "deployment_name": deps.get(r.get("deployment_id") or ""),
                    "work_pool_name": r.get("work_pool_name"),
                    "work_queue_name": r.get("work_queue_name"),
                    "state_type": r.get("state_type"),
                    "state_name": r.get("state_name"),
                    "state_message": state.get("message") or None,
                    "state_timestamp": ts(state.get("timestamp")),
                    "tags": r.get("tags") or [],
                    "parameters": json_text(r.get("parameters")),
                    "run_count": r.get("run_count"),
                    "max_retries": (r.get("empirical_policy") or {}).get("retries"),
                    "expected_start_time": ts(r.get("expected_start_time")),
                    "next_scheduled_start_time": ts(r.get("next_scheduled_start_time")),
                    "start_time": ts(r.get("start_time")),
                    "end_time": ts(r.get("end_time")),
                    "total_run_time": r.get("total_run_time"),
                    "estimated_run_time": r.get("estimated_run_time"),
                    "estimated_start_time_delta": r.get("estimated_start_time_delta"),
                    "auto_scheduled": r.get("auto_scheduled"),
                    "parent_task_run_id": r.get("parent_task_run_id"),
                    "idempotency_key": r.get("idempotency_key"),
                    "flow_version": r.get("flow_version"),
                    "infrastructure_pid": r.get("infrastructure_pid"),
                    "created_by_type": _created_by(r, "type"),
                    "created_by_name": _created_by(r, "display_value"),
                    "labels": json_text(r.get("labels")),
                    "created": ts(r.get("created")),
                    "updated": ts(r.get("updated")),
                    "url": flow_run_url(client, r["id"]),
                }
            )
        return out


# ------------------------------------------------------------------------ task runs


def task_run_body(pd: Pushdown) -> dict[str, Any] | None:
    """Filter body for ``/task_runs/filter`` from native task_runs columns (None = no rows can match)."""
    body: dict[str, Any] = {"sort": "ID_DESC"}
    ok = (
        push_any(body, pd, "id", "task_runs.id")
        and push_any(body, pd, "name", "task_runs.name")
        and push_any(body, pd, "flow_run_id", "task_runs.flow_run_id")
        and push_any(body, pd, "state_type", "task_runs.state.type", cast=lambda v: str(v).upper())
        and push_any(body, pd, "state_name", "task_runs.state.name")
    )
    if not ok:
        return None
    push_tags(body, pd, "tags", "task_runs.tags")
    push_range(body, pd, "start_time", "task_runs.start_time")
    push_range(body, pd, "end_time", "task_runs.end_time")
    push_range(body, pd, "expected_start_time", "task_runs.expected_start_time")
    return body


class TaskRuns(_Scan):
    """Rows for ``prefect.main.task_runs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Task run id (UUID).", nullable=False),
            column("name", pa.string(), "Task run name, e.g. 'render-132'.", nullable=False),
            column("flow_run_id", pa.string(), "Flow run the task ran in; NULL for background tasks."),
            column("task_key", pa.string(), "Stable key of the task definition."),
            column("dynamic_key", pa.string(), "Key distinguishing repeated calls of one task in a run."),
            column("state_type", pa.string(), "Prefect StateType: COMPLETED, FAILED, CRASHED, RUNNING, PENDING, ..."),
            column("state_name", pa.string(), "State display name, e.g. 'Retrying', 'Cached', 'NotReady'."),
            column("state_message", pa.string(), "Message attached to the current state (often the error)."),
            column("tags", STR_LIST, "Task run tags (task tags plus tags active when it ran)."),
            column("run_count", pa.int64(), "Attempts made (count): 2 means one retry."),
            column("flow_run_run_count", pa.int64(), "The flow run's run_count when the task ran (count)."),
            column("max_retries", pa.int64(), "Configured task retries (count)."),
            column("expected_start_time", TSTZ, "When the task run was expected to start."),
            column("start_time", TSTZ, "When the task run started."),
            column("end_time", TSTZ, "When the task run finished."),
            column("total_run_time", pa.float64(), "Time spent running across attempts, in seconds."),
            column("estimated_run_time", pa.float64(), "Run time so far for a running task, in seconds."),
            column("cache_key", pa.string(), "Cache key, for cached tasks."),
            column("cache_expiration", TSTZ, "When the cached result expires."),
            column("task_version", pa.string(), "Task version label."),
            column("task_inputs", JSON, "Upstream inputs of each parameter, as JSON."),
            column("labels", JSON, "Task run labels as a JSON object."),
            column("created", TSTZ, "When the task run was created."),
            column("updated", TSTZ, "When the task run was last updated."),
            column("url", pa.string(), "Link to the task run in the Prefect UI."),
        ]
    )
    Meta = _meta("task_runs")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """See ``task_run_body``: ids, names, flow_run_id, state, tags, time ranges."""
        body = task_run_body(pd)
        return [] if body is None else [listing("/task_runs/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map task-run objects."""
        out = []
        for t in items:
            state = _state(t)
            out.append(
                {
                    "id": t["id"],
                    "name": t["name"],
                    "flow_run_id": t.get("flow_run_id"),
                    "task_key": t.get("task_key"),
                    "dynamic_key": t.get("dynamic_key"),
                    "state_type": t.get("state_type"),
                    "state_name": t.get("state_name"),
                    "state_message": state.get("message") or None,
                    "tags": t.get("tags") or [],
                    "run_count": t.get("run_count"),
                    "flow_run_run_count": t.get("flow_run_run_count"),
                    "max_retries": (t.get("empirical_policy") or {}).get("retries"),
                    "expected_start_time": ts(t.get("expected_start_time")),
                    "start_time": ts(t.get("start_time")),
                    "end_time": ts(t.get("end_time")),
                    "total_run_time": t.get("total_run_time"),
                    "estimated_run_time": t.get("estimated_run_time"),
                    "cache_key": t.get("cache_key"),
                    "cache_expiration": ts(t.get("cache_expiration")),
                    "task_version": t.get("task_version"),
                    "task_inputs": json_text(t.get("task_inputs")),
                    "labels": json_text(t.get("labels")),
                    "created": ts(t.get("created")),
                    "updated": ts(t.get("updated")),
                    "url": task_run_url(client, t["id"]),
                }
            )
        return out


# ----------------------------------------------------------------------------- logs


def _log_body(pd: Pushdown, *, task_level: bool) -> dict[str, Any] | None:
    body: dict[str, Any] = {"sort": "TIMESTAMP_ASC"}
    if not push_any(body, pd, "flow_run_id", "logs.flow_run_id"):
        return None
    if task_level:
        if not push_any(body, pd, "task_run_id", "logs.task_run_id"):
            return None
        if "task_run_id" not in body.get("logs", {}):
            put(body, "logs.task_run_id.is_null_", False)
    else:
        put(body, "logs.task_run_id.is_null_", True)
    # level (numeric bounds or =/IN) and level_name (=/IN) → logs.level.ge_ / le_.
    lo: int | None = None
    hi: int | None = None
    names = pd.values("level_name")
    if names is not None:
        nums = [LEVEL_NUMBERS[str(n)] for n in names if str(n) in LEVEL_NUMBERS]
        if not nums:
            return None
        lo, hi = min(nums), max(nums)
    levels = pd.values("level")
    if levels is not None:
        if not levels:
            return None
        lo = max(lo if lo is not None else min(levels), min(levels))
        hi = min(hi if hi is not None else max(levels), max(levels))
    else:
        b = pd.bounds("level")
        if b is not None:
            if isinstance(b[0], int):
                lo = max(lo or b[0], b[0])
            if isinstance(b[2], int):
                hi = b[2] if hi is None else min(hi, b[2])
    if lo is not None:
        put(body, "logs.level.ge_", int(lo))
    if hi is not None:
        put(body, "logs.level.le_", int(hi))
    if lo is not None and hi is not None and lo > hi:
        return None
    push_range(body, pd, "timestamp", "logs.timestamp")
    return body


def _log_fields(task_level: bool) -> list[pa.Field[Any]]:
    fields = [
        column("id", pa.string(), "Log line id (UUID).", nullable=False),
        column("flow_run_id", pa.string(), "Flow run that emitted the line."),
    ]
    if task_level:
        fields.append(column("task_run_id", pa.string(), "Task run that emitted the line.", nullable=False))
    return [
        *fields,
        column("timestamp", TSTZ, "When the line was logged.", nullable=False),
        column("level", pa.int32(), "Python logging level: 10 debug, 20 info, 30 warning, 40 error, 50 critical."),
        column("level_name", pa.string(), "Lower-case level name: debug, info, warning, error or critical."),
        column("logger", pa.string(), "Logger name, e.g. 'prefect.flow_runs' or 'prefect.task_runs'."),
        column("message", pa.string(), "The log message."),
        column("created", TSTZ, "When the API stored the line."),
    ]


def _log_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "id": g["id"],
            "flow_run_id": g.get("flow_run_id"),
            "task_run_id": g.get("task_run_id"),
            "timestamp": ts(g.get("timestamp")),
            "level": g.get("level"),
            "level_name": level_name(g.get("level")),
            "logger": g.get("name"),
            "message": g.get("message"),
            "created": ts(g.get("created")),
        }
        for g in items
    ]


class FlowRunLogs(_Scan):
    """Rows for ``prefect.main.flow_run_logs`` (lines with no task run)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(_log_fields(False))
    Meta = _meta("flow_run_logs")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """``task_run_id.is_null_``; flow_run_id → ``any_``; level / level_name → ``ge_``/``le_``; timestamp range."""
        body = _log_body(pd, task_level=False)
        return [] if body is None else [listing("/logs/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map log objects."""
        return _log_rows(items)


class TaskRunLogs(_Scan):
    """Rows for ``prefect.main.task_run_logs``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(_log_fields(True))
    Meta = _meta("task_run_logs")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """task_run_id → ``any_`` (else ``is_null_: false``); flow_run_id, level, timestamp as for flow_run_logs."""
        body = _log_body(pd, task_level=True)
        return [] if body is None else [listing("/logs/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map log objects."""
        return _log_rows(items)


# ------------------------------------------------------------------ infrastructure


class WorkPools(_Scan):
    """Rows for ``prefect.main.work_pools``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Work pool id (UUID).", nullable=False),
            column("name", pa.string(), "Work pool name.", nullable=False),
            column("type", pa.string(), "Infrastructure type: process, docker, kubernetes, ecs, ..."),
            column("description", pa.string(), "Work pool description."),
            column("is_paused", pa.bool_(), "True when the pool is paused."),
            column("status", pa.string(), "READY, NOT_READY or PAUSED."),
            column("concurrency_limit", pa.int64(), "Maximum concurrent runs in the pool (count)."),
            column("default_queue_id", pa.string(), "Id of the pool's default work queue."),
            column("base_job_template", JSON, "The pool's job template as JSON."),
            column("created", TSTZ, "When the pool was created."),
            column("updated", TSTZ, "When the pool was last updated."),
            column("url", pa.string(), "Link to the work pool in the Prefect UI."),
        ]
    )
    Meta = _meta("work_pools")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """Id, name and type → ``work_pools.*.any_``."""
        body: dict[str, Any] = {}
        ok = (
            push_any(body, pd, "id", "work_pools.id")
            and push_any(body, pd, "name", "work_pools.name")
            and push_any(body, pd, "type", "work_pools.type")
        )
        return [listing("/work_pools/filter", body)] if ok else []

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map work-pool objects."""
        return [
            {
                "id": p["id"],
                "name": p["name"],
                "type": p.get("type"),
                "description": p.get("description"),
                "is_paused": p.get("is_paused"),
                "status": p.get("status"),
                "concurrency_limit": p.get("concurrency_limit"),
                "default_queue_id": p.get("default_queue_id"),
                "base_job_template": json_text(p.get("base_job_template")),
                "created": ts(p.get("created")),
                "updated": ts(p.get("updated")),
                "url": client.ui(f"/work-pools/work-pool/{quote(p['name'])}"),
            }
            for p in items
        ]


class WorkQueues(_Scan):
    """Rows for ``prefect.main.work_queues``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Work queue id (UUID).", nullable=False),
            column("name", pa.string(), "Queue name (unique per pool).", nullable=False),
            column("work_pool_name", pa.string(), "Work pool the queue belongs to."),
            column("work_pool_id", pa.string(), "Id of that work pool."),
            column("description", pa.string(), "Queue description."),
            column("is_paused", pa.bool_(), "True when the queue is paused."),
            column("priority", pa.int64(), "Priority within the pool; lower is served first."),
            column("concurrency_limit", pa.int64(), "Maximum concurrent runs from the queue (count)."),
            column("status", pa.string(), "READY, NOT_READY or PAUSED."),
            column("last_polled", TSTZ, "When a worker last polled the queue."),
            column("created", TSTZ, "When the queue was created."),
            column("updated", TSTZ, "When the queue was last updated."),
        ]
    )
    Meta = _meta("work_queues")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """work_pool_name → ``/work_pools/<name>/queues/filter`` per pool; id and name → ``work_queues.*.any_``."""
        body: dict[str, Any] = {}
        if not (push_any(body, pd, "id", "work_queues.id") and push_any(body, pd, "name", "work_queues.name")):
            return []
        pools = pd.values("work_pool_name")
        if pools is not None:
            return [
                listing(f"/work_pools/{quote(str(p))}/queues/filter", body, missing_ok=True, work_pool_name=str(p))
                for p in sorted(set(pools))
            ]
        return [listing("/work_queues/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map work-queue objects."""
        ctx = dict(listing.context)
        return [
            {
                "id": q["id"],
                "name": q["name"],
                "work_pool_name": q.get("work_pool_name") or ctx.get("work_pool_name"),
                "work_pool_id": q.get("work_pool_id"),
                "description": q.get("description"),
                "is_paused": q.get("is_paused"),
                "priority": q.get("priority"),
                "concurrency_limit": q.get("concurrency_limit"),
                "status": q.get("status"),
                "last_polled": ts(q.get("last_polled")),
                "created": ts(q.get("created")),
                "updated": ts(q.get("updated")),
            }
            for q in items
        ]


class Workers(_Scan):
    """Rows for ``prefect.main.workers`` (one listing per work pool)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("work_pool_name", pa.string(), "Work pool the worker polls.", nullable=False),
            column("id", pa.string(), "Worker id (UUID).", nullable=False),
            column("name", pa.string(), "Worker name.", nullable=False),
            column("status", pa.string(), "ONLINE or OFFLINE."),
            column("last_heartbeat_time", TSTZ, "When the worker last sent a heartbeat."),
            column("heartbeat_interval_seconds", pa.int64(), "Expected seconds between heartbeats."),
            column("work_pool_id", pa.string(), "Id of the work pool."),
            column("created", TSTZ, "When the worker first registered."),
            column("updated", TSTZ, "When the worker record was last updated."),
        ]
    )
    Meta = _meta("workers")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """work_pool_name → which pools to read; status → ``workers.status.any_``; heartbeat range."""
        body: dict[str, Any] = {}
        if not push_any(body, pd, "status", "workers.status", cast=lambda v: str(v).upper()):
            return []
        push_range(body, pd, "last_heartbeat_time", "workers.last_heartbeat_time")
        pools = pd.values("work_pool_name")
        if pools is None:
            pools = [p["name"] for p in client.post("/work_pools/filter", {"limit": 200, "offset": 0}) or []]
        return [
            listing(f"/work_pools/{quote(str(p))}/workers/filter", body, missing_ok=True, work_pool_name=str(p))
            for p in sorted(set(pools))
        ]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map worker objects."""
        pool = dict(listing.context)["work_pool_name"]
        return [
            {
                "work_pool_name": pool,
                "id": w["id"],
                "name": w["name"],
                "status": w.get("status"),
                "last_heartbeat_time": ts(w.get("last_heartbeat_time")),
                "heartbeat_interval_seconds": w.get("heartbeat_interval_seconds"),
                "work_pool_id": w.get("work_pool_id"),
                "created": ts(w.get("created")),
                "updated": ts(w.get("updated")),
            }
            for w in items
        ]


class ConcurrencyLimits(_Scan):
    """Rows for ``prefect.main.concurrency_limits`` (the v2 API, tag limits included)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Concurrency limit id (UUID).", nullable=False),
            column("name", pa.string(), "Limit name; tag limits are named 'tag:<tag>'.", nullable=False),
            column("kind", pa.string(), "'tag' for a tag-based task limit, otherwise 'global'.", nullable=False),
            column("tag", pa.string(), "The tag of a tag limit; NULL for global limits."),
            column("limit", pa.int64(), "Maximum slots (count)."),
            column("active", pa.bool_(), "True when the limit is enforced."),
            column("active_slots", pa.int64(), "Slots currently held (count)."),
            column("denied_slots", pa.int64(), "Slot requests currently denied (count)."),
            column("slot_decay_per_second", pa.float64(), "Slots released per second (rate limits)."),
            column("created", TSTZ, "When the limit was created."),
            column("updated", TSTZ, "When the limit was last updated."),
        ]
    )
    Meta = _meta("concurrency_limits")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """The endpoint takes no filters: every limit is read."""
        return [listing("/v2/concurrency_limits/filter")]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map v2 concurrency-limit objects."""
        out = []
        for c in items:
            name = str(c["name"])
            is_tag = name.startswith("tag:")
            out.append(
                {
                    "id": c["id"],
                    "name": name,
                    "kind": "tag" if is_tag else "global",
                    "tag": name.removeprefix("tag:") if is_tag else None,
                    "limit": c.get("limit"),
                    "active": c.get("active"),
                    "active_slots": c.get("active_slots"),
                    "denied_slots": c.get("denied_slots"),
                    "slot_decay_per_second": c.get("slot_decay_per_second"),
                    "created": ts(c.get("created")),
                    "updated": ts(c.get("updated")),
                }
            )
        return out


# ----------------------------------------------------------------------- automation


class Automations(_Scan):
    """Rows for ``prefect.main.automations``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Automation id (UUID).", nullable=False),
            column("name", pa.string(), "Automation name.", nullable=False),
            column("description", pa.string(), "Automation description."),
            column("enabled", pa.bool_(), "True when the automation is active."),
            column("tags", STR_LIST, "Automation tags."),
            column("trigger_type", pa.string(), "Trigger type: event, compound, sequence or metric."),
            column("trigger", JSON, "The full trigger as JSON (match, expect, posture, threshold, within)."),
            column("actions", JSON, "Actions run when the trigger fires, as a JSON array."),
            column("actions_on_trigger", JSON, "Actions run when the trigger enters triggered state."),
            column("actions_on_resolve", JSON, "Actions run when the trigger resolves."),
            column("created", TSTZ, "When the automation was created."),
            column("updated", TSTZ, "When the automation was last updated."),
            column("url", pa.string(), "Link to the automation in the Prefect UI."),
        ]
    )
    Meta = _meta("automations")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """Id and name → ``automations.*.any_``; tags → ``automations.tags.all_``."""
        body: dict[str, Any] = {"sort": "NAME_ASC"}
        if not (push_any(body, pd, "id", "automations.id") and push_any(body, pd, "name", "automations.name")):
            return []
        push_tags(body, pd, "tags", "automations.tags")
        return [listing("/automations/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map automation objects."""
        return [
            {
                "id": a["id"],
                "name": a["name"],
                "description": a.get("description") or None,
                "enabled": a.get("enabled"),
                "tags": a.get("tags") or [],
                "trigger_type": (a.get("trigger") or {}).get("type"),
                "trigger": json_text(a.get("trigger")),
                "actions": json_text(a.get("actions")),
                "actions_on_trigger": json_text(a.get("actions_on_trigger")),
                "actions_on_resolve": json_text(a.get("actions_on_resolve")),
                "created": ts(a.get("created")),
                "updated": ts(a.get("updated")),
                "url": client.ui(f"/automations/automation/{a['id']}"),
            }
            for a in items
        ]


# Lower bound sent when a query has no occurred filter (Prefect otherwise reads only the last day).
EVENTS_EPOCH = "2000-01-01T00:00:00Z"


class Events(_Scan):
    """Rows for ``prefect.main.events``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Event id (UUID).", nullable=False),
            column("occurred", TSTZ, "When the event happened.", nullable=False),
            column("event", pa.string(), "Event name, e.g. 'prefect.flow-run.Failed'.", nullable=False),
            column("resource_id", pa.string(), "Primary resource, e.g. 'prefect.flow-run.<id>'.", nullable=False),
            column("resource_name", pa.string(), "The primary resource's prefect.resource.name label."),
            column("resource", JSON, "All labels of the primary resource, as a JSON object."),
            column("related", JSON, "Related resources (flow, deployment, tags, ...), as a JSON array."),
            column("payload", JSON, "Event payload as JSON (e.g. the validated state)."),
            column("received", TSTZ, "When Prefect received the event."),
            column("follows", pa.string(), "Id of the event this one follows, if ordered."),
        ]
    )
    Meta = _meta("events")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """occurred → ``occurred.since/until``; event =/IN → ``event.name``; resource_id → ``resource.id``."""
        flt: dict[str, Any] = {"order": "ASC"}
        occurred = pd.time_range("occurred")
        flt["occurred"] = {"since": occurred.get("after_", EVENTS_EPOCH)}
        if "before_" in occurred:
            flt["occurred"]["until"] = occurred["before_"]
        names = pd.values("event")
        if names is not None:
            if not names:
                return []
            flt["event"] = {"name": sorted({str(n) for n in names})}
        else:
            prefixes = pd.starts_with("event")
            if prefixes:
                flt["event"] = {"prefix": prefixes}
        resources = pd.values("resource_id")
        if resources is not None:
            if not resources:
                return []
            flt["resource"] = {"id": sorted({str(r) for r in resources})}
        return [listing("/events/filter", {"filter": flt}, paging="events")]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map event objects."""
        out = []
        for e in items:
            resource = e.get("resource") or {}
            out.append(
                {
                    "id": e["id"],
                    "occurred": ts(e.get("occurred")),
                    "event": e.get("event"),
                    "resource_id": resource.get("prefect.resource.id"),
                    "resource_name": resource.get("prefect.resource.name"),
                    "resource": json_text(resource),
                    "related": json_text(e.get("related") or []),
                    "payload": json_text(e.get("payload") or {}),
                    "received": ts(e.get("received")),
                    "follows": e.get("follows"),
                }
            )
        return out


# --------------------------------------------------------------------------- config


class Artifacts(_Scan):
    """Rows for ``prefect.main.artifacts``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Artifact id (UUID); one per version of a keyed artifact.", nullable=False),
            column("key", pa.string(), "Artifact key; NULL for unkeyed artifacts."),
            column("type", pa.string(), "markdown, table, link, progress or image."),
            column("description", pa.string(), "Artifact description (markdown)."),
            column("data", JSON, "Artifact data as JSON: table rows, a markdown string, a progress number, ..."),
            column("metadata", JSON, "Artifact metadata as a JSON object."),
            column("flow_run_id", pa.string(), "Flow run that created the artifact."),
            column("task_run_id", pa.string(), "Task run that created the artifact."),
            column("created", TSTZ, "When the artifact was created."),
            column("updated", TSTZ, "When the artifact was last updated."),
        ]
    )
    Meta = _meta("artifacts")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """Id, key, type, flow_run_id and task_run_id → ``artifacts.*.any_``."""
        body: dict[str, Any] = {"sort": "ID_DESC"}
        ok = all(
            push_any(body, pd, col, f"artifacts.{col}") for col in ("id", "key", "type", "flow_run_id", "task_run_id")
        )
        return [listing("/artifacts/filter", body)] if ok else []

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map artifact objects; table artifacts' JSON text is parsed."""
        return [
            {
                "id": a["id"],
                "key": a.get("key"),
                "type": a.get("type"),
                "description": a.get("description"),
                "data": _json_value(a.get("data"), parse_strings=a.get("type") == "table"),
                "metadata": json_text(a.get("metadata_")),
                "flow_run_id": a.get("flow_run_id"),
                "task_run_id": a.get("task_run_id"),
                "created": ts(a.get("created")),
                "updated": ts(a.get("updated")),
            }
            for a in items
        ]


class BlockTypes(_Scan):
    """Rows for ``prefect.main.block_types``."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Block type id (UUID).", nullable=False),
            column("name", pa.string(), "Block type name, e.g. 'AWS Credentials'.", nullable=False),
            column("slug", pa.string(), "Block type slug, e.g. 'aws-credentials'.", nullable=False),
            column("description", pa.string(), "What the block type is for (markdown)."),
            column("documentation_url", pa.string(), "Link to the block type's documentation."),
            column("logo_url", pa.string(), "Link to the block type's logo."),
            column("is_protected", pa.bool_(), "True for Prefect's built-in protected types."),
            column("created", TSTZ, "When the type was registered."),
            column("updated", TSTZ, "When the type was last updated."),
        ]
    )
    Meta = _meta("block_types")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """slug → ``block_types.slug.any_``."""
        body: dict[str, Any] = {}
        return [listing("/block_types/filter", body)] if push_any(body, pd, "slug", "block_types.slug") else []

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map block-type objects."""
        return [
            {
                "id": b["id"],
                "name": b["name"],
                "slug": b["slug"],
                "description": b.get("description"),
                "documentation_url": b.get("documentation_url"),
                "logo_url": b.get("logo_url"),
                "is_protected": b.get("is_protected"),
                "created": ts(b.get("created")),
                "updated": ts(b.get("updated")),
            }
            for b in items
        ]


class BlockDocuments(_Scan):
    """Rows for ``prefect.main.block_documents`` (names and types only)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(
        [
            column("id", pa.string(), "Block document id (UUID).", nullable=False),
            column("name", pa.string(), "Block name, used in Block.load('<name>')."),
            column("block_type_slug", pa.string(), "Slug of the block's type, e.g. 'secret'."),
            column("block_type_name", pa.string(), "Name of the block's type."),
            column("block_type_id", pa.string(), "Id of the block's type."),
            column("block_schema_id", pa.string(), "Id of the schema version the document uses."),
            column("is_anonymous", pa.bool_(), "True for unnamed blocks Prefect created implicitly."),
            column("secret_fields", STR_LIST, "Names of the fields Prefect treats as secret (never their values)."),
            column("created", TSTZ, "When the block was saved."),
            column("updated", TSTZ, "When the block was last updated."),
        ]
    )
    Meta = _meta("block_documents")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """Id, name, block_type_slug → ``any_``; is_anonymous → ``eq_``. Always ``include_secrets: false``."""
        body: dict[str, Any] = {"include_secrets": False, "sort": "NAME_ASC"}
        ok = (
            push_any(body, pd, "id", "block_documents.id")
            and push_any(body, pd, "name", "block_documents.name")
            and push_any(body, pd, "block_type_slug", "block_types.slug")
        )
        if not ok:
            return []
        anon = pd.one("is_anonymous")
        if anon is not None:
            put(body, "block_documents.is_anonymous.eq_", bool(anon))
        return [listing("/block_documents/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map block documents, dropping ``data`` entirely."""
        out = []
        for b in items:
            btype = b.get("block_type") or {}
            fields = (b.get("block_schema") or {}).get("fields") or {}
            out.append(
                {
                    "id": b["id"],
                    "name": b.get("name"),
                    "block_type_slug": btype.get("slug"),
                    "block_type_name": b.get("block_type_name") or btype.get("name"),
                    "block_type_id": b.get("block_type_id"),
                    "block_schema_id": b.get("block_schema_id"),
                    "is_anonymous": b.get("is_anonymous"),
                    "secret_fields": [str(f) for f in fields.get("secret_fields") or []],
                    "created": ts(b.get("created")),
                    "updated": ts(b.get("updated")),
                }
            )
        return out


VARIABLE_FIELDS = [
    column("id", pa.string(), "Variable id (UUID).", nullable=False),
    column("name", pa.string(), "Variable name.", nullable=False),
    column("value", pa.string(), "'***' unless revealed: then strings as is, other values as JSON text."),
    column("is_masked", pa.bool_(), "True when value is the '***' mask.", nullable=False),
    column("value_type", pa.string(), "JSON type of the stored value: string, number, boolean, array, object, null."),
    column("tags", STR_LIST, "Variable tags."),
    column("created", TSTZ, "When the variable was created."),
    column("updated", TSTZ, "When the variable was last updated."),
]


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int | float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    return "object"


def variable_rows(items: list[dict[str, Any]], *, reveal: bool) -> list[dict[str, Any]]:
    """Map variables, masking values (see ``masking.mask_variable``)."""
    out = []
    for v in items:
        value, masked = mask_variable(str(v["name"]), v.get("value"), reveal=reveal)
        out.append(
            {
                "id": v["id"],
                "name": v["name"],
                "value": value,
                "is_masked": masked,
                "value_type": _json_type(v.get("value")),
                "tags": v.get("tags") or [],
                "created": ts(v.get("created")),
                "updated": ts(v.get("updated")),
            }
        )
    return out


def variable_body(pd: Pushdown) -> dict[str, Any] | None:
    """Filter body for ``/variables/filter``."""
    body: dict[str, Any] = {"sort": "NAME_ASC"}
    if not (push_any(body, pd, "id", "variables.id") and push_any(body, pd, "name", "variables.name")):
        return None
    push_tags(body, pd, "tags", "variables.tags")
    return body


class Variables(_Scan):
    """Rows for ``prefect.main.variables`` (values always masked)."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(VARIABLE_FIELDS)
    Meta = _meta("variables")

    @classmethod
    def requests(cls, client: PrefectClient, pd: Pushdown) -> list[Listing]:
        """Id and name → ``variables.*.any_``; tags → ``variables.tags.all_``."""
        body = variable_body(pd)
        return [] if body is None else [listing("/variables/filter", body)]

    @classmethod
    def rows(
        cls, client: PrefectClient, items: list[dict[str, Any]], wanted: set[str], listing: Listing
    ) -> list[dict[str, Any]]:
        """Map variables with every value masked."""
        return variable_rows(items, reveal=False)


log = logging.getLogger(__name__)
