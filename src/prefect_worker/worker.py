"""Assemble the ``prefect`` catalog and run the worker.

Run it:

    vgi-prefect-worker                      # stdio: what DuckDB launches by default
    vgi-prefect-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB, then give it credentials:

    ATTACH 'prefect' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-prefect-worker');
    CREATE SECRET (TYPE prefect, API_URL 'http://localhost:4200/api');
"""

from __future__ import annotations

import json
from typing import Any, cast

import pyarrow as pa
from vgi import Worker
from vgi.catalog import Catalog, Schema, Table
from vgi.catalog.constraints import ReferencedKey, constraint_record
from vgi.metadata import FunctionExample

from .client import PREFECT_SECRET
from .functions import VariableValues
from .meta import (
    CATALOG_DESCRIPTION,
    CATALOG_NAME,
    CATALOG_TAGS,
    MAIN_CATEGORIES,
    MAIN_DOC_LLM,
    MAIN_DOC_MD,
    SOURCE_URL,
    TABLE_DOCS,
    UNIFIED_CATEGORIES,
    UNIFIED_DOC_LLM,
    UNIFIED_DOC_MD,
    keywords_array,
    object_tags,
)
from .status import UnifiedStatus
from .tables import (
    Artifacts,
    Automations,
    BlockDocuments,
    BlockTypes,
    ConcurrencyLimits,
    Deployments,
    DeploymentSchedules,
    Events,
    FlowRunLogs,
    FlowRuns,
    Flows,
    TaskRunLogs,
    TaskRuns,
    Variables,
    Workers,
    WorkPools,
    WorkQueues,
)
from .unified import UnifiedRuns, UnifiedTaskRuns

Q = f"{CATALOG_NAME}.main"
U = f"{CATALOG_NAME}.unified"

EXAMPLES: dict[str, list[tuple[str, str]]] = {
    "flows": [
        (
            "Flows with a given tag",
            f"SELECT name, created FROM {Q}.flows WHERE list_contains(tags, 'etl') ORDER BY name",
        ),
        (
            "Flow-run outcomes per flow",
            f"SELECT f.name, r.state_type, count(*) AS runs FROM {Q}.flows f JOIN {Q}.flow_runs r ON r.flow_id = f.id "
            "GROUP BY ALL ORDER BY f.name, r.state_type",
        ),
    ],
    "deployments": [
        (
            "Deployments per work pool and whether they are paused",
            f"SELECT work_pool_name, flow_name, name, paused, status FROM {Q}.deployments "
            "ORDER BY work_pool_name, flow_name, name",
        ),
    ],
    "deployment_schedules": [
        (
            "Active cron schedules with their deployment",
            f"SELECT deployment_name, cron, timezone FROM {Q}.deployment_schedules WHERE active AND cron IS NOT NULL "
            "ORDER BY deployment_name",
        ),
    ],
    "flow_runs": [
        (
            "Failed or crashed runs of one flow in November 2026 (flow, state types and the range reach the API)",
            f"SELECT name, state_name, start_time, state_message FROM {Q}.flow_runs WHERE flow_name = 'broken-import' "
            "AND state_type IN ('FAILED', 'CRASHED') AND start_time >= TIMESTAMPTZ '2026-11-01' ORDER BY start_time",
        ),
        (
            "Run count and median run time per flow and state",
            f"SELECT flow_name, state_type, count(*) AS runs, median(total_run_time) AS median_s FROM {Q}.flow_runs "
            "GROUP BY ALL ORDER BY flow_name, state_type",
        ),
    ],
    "task_runs": [
        (
            "Task runs that needed a retry",
            f"SELECT name, run_count, max_retries, state_name FROM {Q}.task_runs WHERE run_count > 1 ORDER BY name",
        ),
        (
            "Slowest completed tasks",
            f"SELECT name, total_run_time FROM {Q}.task_runs WHERE state_type = 'COMPLETED' "
            "ORDER BY total_run_time DESC LIMIT 10",
        ),
    ],
    "flow_run_logs": [
        (
            "Error lines logged by flow runs (level is sent to the API)",
            f"SELECT flow_run_id, timestamp, message FROM {Q}.flow_run_logs WHERE level_name = 'error' "
            "ORDER BY timestamp",
        ),
    ],
    "task_run_logs": [
        (
            "Why each failed task run failed",
            f"SELECT t.name, l.timestamp, l.message FROM {Q}.task_runs t "
            f"JOIN {Q}.task_run_logs l ON l.task_run_id = t.id "
            "WHERE l.level >= 40 ORDER BY l.timestamp",
        ),
    ],
    "work_pools": [
        ("Work pools by type and status", f"SELECT name, type, status, is_paused FROM {Q}.work_pools ORDER BY name"),
    ],
    "work_queues": [
        (
            "Queues per pool in priority order",
            f"SELECT work_pool_name, name, priority, status, last_polled FROM {Q}.work_queues "
            "ORDER BY work_pool_name, priority",
        ),
    ],
    "workers": [
        (
            "Workers and their last heartbeat",
            f"SELECT work_pool_name, name, status, last_heartbeat_time FROM {Q}.workers ORDER BY work_pool_name, name",
        ),
    ],
    "concurrency_limits": [
        (
            "Concurrency limits and their slot usage",
            f'SELECT name, kind, "limit", active_slots FROM {Q}.concurrency_limits ORDER BY name',
        ),
    ],
    "automations": [
        (
            "Enabled automations and the events they expect",
            "SELECT name, trigger_type, trigger_definition->>'$.posture' AS posture, "
            f"trigger_definition->'$.expect' AS expects "
            f"FROM {Q}.automations WHERE enabled ORDER BY name",
        ),
    ],
    "events": [
        (
            "Flow-run state changes in a window (occurred and the prefix reach the API)",
            f"SELECT event, count(*) AS n FROM {Q}.events WHERE starts_with(event, 'prefect.flow-run.') "
            "AND occurred >= TIMESTAMPTZ '2026-11-15' AND occurred < TIMESTAMPTZ '2026-11-16' "
            "GROUP BY event ORDER BY n DESC",
        ),
    ],
    "artifacts": [
        (
            "Latest rows of a table artifact",
            f"SELECT created, data FROM {Q}.artifacts WHERE key = 'orders-summary' ORDER BY created DESC LIMIT 1",
        ),
    ],
    "block_types": [
        (
            "Block types by slug",
            f"SELECT slug, name FROM {Q}.block_types WHERE NOT is_protected ORDER BY slug LIMIT 20",
        ),
    ],
    "block_documents": [
        (
            "Named blocks per type (values are never read)",
            f"SELECT block_type_slug, list(name ORDER BY name) AS blocks FROM {Q}.block_documents "
            "GROUP BY block_type_slug ORDER BY block_type_slug",
        ),
    ],
    "variables": [
        (
            "Variable names, types and tags (values masked)",
            f"SELECT name, value_type, tags FROM {Q}.variables ORDER BY name",
        ),
    ],
    "runs": [
        ("Runs per status", f"SELECT status, count(*) AS runs FROM {U}.runs GROUP BY status ORDER BY status"),
        (
            "Failure rate per pipeline since November",
            f"SELECT pipeline, avg((status = 'failed')::INT) AS failure_rate FROM {U}.runs "
            "WHERE started_at >= TIMESTAMPTZ '2026-11-01' GROUP BY pipeline ORDER BY failure_rate DESC",
        ),
    ],
    "task_runs_unified": [
        (
            "Task outcomes joined to their runs",
            f"SELECT r.pipeline, t.status, count(*) AS tasks FROM {U}.task_runs t JOIN {U}.runs r USING (run_id) "
            "GROUP BY ALL ORDER BY r.pipeline, t.status",
        )
    ],
}


def duckdb_type(t: pa.DataType) -> str:
    """Spell an Arrow type the way DuckDB names it (for ``vgi.result_columns_schema``)."""
    if getattr(t, "extension_name", None) == "arrow.json":
        return "JSON"
    if pa.types.is_timestamp(t):
        return "TIMESTAMPTZ" if t.tz else "TIMESTAMP"
    if pa.types.is_list(t):
        return f"{duckdb_type(t.value_type)}[]"
    if pa.types.is_struct(t):
        return "STRUCT(" + ", ".join(f"{f.name} {duckdb_type(f.type)}" for f in t) + ")"
    simple = {pa.string(): "VARCHAR", pa.int64(): "BIGINT", pa.int32(): "INTEGER", pa.bool_(): "BOOLEAN"}
    return simple.get(t, "DOUBLE" if pa.types.is_floating(t) else str(t).upper())


def _result_columns(schema: pa.Schema) -> str:
    return json.dumps(
        [
            {"name": f.name, "type": duckdb_type(f.type), "description": (f.metadata or {})[b"comment"].decode()}
            for f in schema
        ]
    )


def _examples_json(name: str) -> str:
    return json.dumps([{"description": d, "sql": s} for d, s in EXAMPLES[name]])


def _table(
    name: str,
    scan: type[Any],
    schema: list[str],
    key: list[str],
    fks: tuple[tuple[list[str], str, list[str]], ...] = (),
    *,
    examples_key: str | None = None,
) -> Table:
    """Build a catalog table over ``scan`` with NOT NULL, primary key and foreign key records.

    Each foreign key is ``(columns, referenced table, referenced primary key)`` in the
    same schema. They mirror the foreign keys of Prefect's database (deleting a flow deletes
    its runs and deployments), so every row satisfies them.
    """
    doc = TABLE_DOCS[examples_key or name]
    not_null = [f.name for f in scan.FIXED_SCHEMA if not f.nullable]
    constraints = [
        constraint_record(
            schema, name, f"{name}_{c}_not_null", "NOT_NULL", [[c]], enforcement="ENFORCED", validation="ALL_ROWS"
        )
        for c in not_null
    ]
    if key:
        constraints.append(
            constraint_record(
                schema,
                name,
                f"{name}_pk",
                "PRIMARY_KEY",
                [[c] for c in key],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
            )
        )
    for cols, ref_table, ref_cols in fks:
        constraints.append(
            constraint_record(
                schema,
                name,
                f"{name}_{ref_table}_fk",
                "FOREIGN_KEY",
                [[c] for c in cols],
                enforcement="ENFORCED",
                validation="ALL_ROWS",
                references=ReferencedKey(
                    schema_path=schema,
                    table=ref_table,
                    columns=[[c] for c in ref_cols],
                    constraint_id=f"{ref_table}_pk",
                ),
            )
        )
    return Table(
        name=name,
        function=scan,
        comment=doc["comment"],
        constraints=tuple(constraints),
        column_comments={f.name: (f.metadata or {})[b"comment"].decode() for f in scan.FIXED_SCHEMA},
        tags={
            **object_tags(
                title=doc["title"],
                category=doc["category"],
                keywords=doc["keywords"],
                doc_llm=doc["doc_llm"],
                doc_md=doc["doc_md"],
            ),
            "domain": "orchestration",
            "vgi.example_queries": _examples_json(examples_key or name),
        },
    )


MAIN_TABLES = [
    _table("flows", Flows, ["main"], ["id"]),
    _table("deployments", Deployments, ["main"], ["id"], ((["flow_id"], "flows", ["id"]),)),
    _table(
        "deployment_schedules",
        DeploymentSchedules,
        ["main"],
        ["schedule_id"],
        ((["deployment_id"], "deployments", ["id"]),),
    ),
    _table(
        "flow_runs",
        FlowRuns,
        ["main"],
        ["id"],
        ((["flow_id"], "flows", ["id"]), (["deployment_id"], "deployments", ["id"])),
    ),
    _table("task_runs", TaskRuns, ["main"], ["id"], ((["flow_run_id"], "flow_runs", ["id"]),)),
    _table("flow_run_logs", FlowRunLogs, ["main"], ["id"]),
    _table("task_run_logs", TaskRunLogs, ["main"], ["id"]),
    _table("work_pools", WorkPools, ["main"], ["id"]),
    _table("work_queues", WorkQueues, ["main"], ["id"], ((["work_pool_id"], "work_pools", ["id"]),)),
    _table("workers", Workers, ["main"], ["id"], ((["work_pool_id"], "work_pools", ["id"]),)),
    _table("concurrency_limits", ConcurrencyLimits, ["main"], ["id"]),
    _table("automations", Automations, ["main"], ["id"]),
    _table("events", Events, ["main"], ["id"]),
    _table("artifacts", Artifacts, ["main"], ["id"]),
    _table("block_types", BlockTypes, ["main"], ["id"]),
    _table("block_documents", BlockDocuments, ["main"], ["id"], ((["block_type_id"], "block_types", ["id"]),)),
    _table("variables", Variables, ["main"], ["name"]),
]
UNIFIED_TABLES = [
    _table("runs", UnifiedRuns, ["unified"], ["run_id"]),
    _table(
        "task_runs",
        UnifiedTaskRuns,
        ["unified"],
        ["task_run_id"],
        ((["run_id"], "runs", ["run_id"]),),
        examples_key="task_runs_unified",
    ),
]

VariableValues.Meta.tags = {
    **object_tags(
        title="Variable Values",
        category="config",
        keywords="variable values, reveal, configuration values, settings",
        doc_llm=(
            "variable_values(reveal := false) lists Prefect variables: id, name, value, is_masked, value_type, tags, "
            "created, updated. By default every value is '***'. With reveal := true a value is shown (strings as is, "
            "other JSON values as JSON text) only when its name does not look secret (password, token, key, secret, "
            "auth, dsn, ...) and the value does not look like a credential (JWT, API-key prefixes, private keys, URLs "
            "with passwords)."
        ),
        doc_md=(
            "## variable_values(reveal := false)\n\n"
            "Variables with opt-in revealing of values that do not look secret. Secret-looking names stay masked."
        ),
    ),
    "domain": "orchestration",
    "vgi.result_columns_schema": _result_columns(VariableValues.FIXED_SCHEMA),
}
VariableValues.Meta.examples = [
    FunctionExample(
        sql=f"SELECT name, value FROM {Q}.variable_values(reveal := true) WHERE NOT is_masked ORDER BY name",
        description="Non-secret variable values",
    ),
]

FUNCTIONS: list[type[Any]] = [VariableValues]
for fn in [*FUNCTIONS, cast("type[Any]", UnifiedStatus)]:
    fn.Meta.tags = {
        **fn.Meta.tags,
        "vgi.example_queries": json.dumps([{"description": e.description, "sql": e.sql} for e in fn.Meta.examples]),
    }


def _schema_examples(keys: list[str], extra: list[type[Any]]) -> str:
    out = [{"description": d, "sql": s} for n in keys for d, s in EXAMPLES[n][:1]]
    out += [{"description": e.description, "sql": e.sql} for fn in extra for e in fn.Meta.examples[:1]]
    return json.dumps(out)


class PrefectWorker(Worker):
    """The worker process: the ``prefect`` catalog with ``main`` and ``unified`` schemas."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_DESCRIPTION,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment=(
                    "Prefect's native REST collections as tables, with pushdown of ids, states, tags and time ranges."
                ),
                tables=MAIN_TABLES,
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "Prefect Resources",
                    "domain": "orchestration",
                    "vgi.keywords": keywords_array(
                        "flows, flow runs, task runs, deployments, logs, work pools, events"
                    ),
                    "vgi.doc_llm": MAIN_DOC_LLM,
                    "vgi.doc_md": MAIN_DOC_MD,
                    "vgi.categories": json.dumps(MAIN_CATEGORIES),
                    "vgi.example_queries": _schema_examples(
                        ["flow_runs", "task_runs", "deployments", "events", "flow_run_logs"], FUNCTIONS
                    ),
                },
            ),
            Schema(
                path=["unified"],
                comment="Runs and task runs in the cross-orchestrator shape shared by Query.Farm workers.",
                tables=UNIFIED_TABLES,
                functions=[UnifiedStatus],
                tags={
                    "vgi.title": "Unified Orchestrator Runs",
                    "domain": "orchestration",
                    "vgi.keywords": keywords_array("unified, runs, task runs, cross orchestrator, union all"),
                    "vgi.doc_llm": UNIFIED_DOC_LLM,
                    "vgi.doc_md": UNIFIED_DOC_MD,
                    "vgi.categories": json.dumps(UNIFIED_CATEGORIES),
                    "vgi.example_queries": _schema_examples(["runs", "task_runs_unified"], [UnifiedStatus]),
                },
            ),
        ],
    )

    secret_types = [PREFECT_SECRET]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    PrefectWorker.main()


if __name__ == "__main__":
    main()
