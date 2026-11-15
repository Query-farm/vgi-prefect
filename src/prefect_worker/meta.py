"""Discovery metadata: catalog, schemas, tables and functions.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Every table and function carries ``object_tags(...)``: a title, a
category from its schema's category list, keywords, and LLM- and human-facing
docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "prefect"

SOURCE_URL = "https://github.com/Query-farm/vgi-prefect"
PREFECT_API_DOCS = "https://docs.prefect.io/v3/api-ref/rest-api"

MAIN_CATEGORIES = [
    {"name": "flows", "title": "Flows and deployments", "description": "Flows, deployments and their schedules."},
    {"name": "runs", "title": "Flow and task runs", "description": "Flow runs, task runs and their states."},
    {"name": "logs", "title": "Run logs", "description": "Log lines emitted by flow runs and task runs."},
    {
        "name": "infrastructure",
        "title": "Work pools and workers",
        "description": "Work pools, work queues, workers and global concurrency limits.",
    },
    {"name": "automation", "title": "Automations and events", "description": "Automations and the event feed."},
    {
        "name": "config",
        "title": "Artifacts, blocks and variables",
        "description": "Run artifacts, block types and documents (names only) and variables (values masked).",
    },
]
UNIFIED_CATEGORIES = [
    {
        "name": "unified_runs",
        "title": "Cross-orchestrator runs",
        "description": "Runs and task runs in a shared shape.",
    },
]
_ALL_CATEGORIES = {c["name"] for c in MAIN_CATEGORIES + UNIFIED_CATEGORIES}


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in _ALL_CATEGORIES:
        raise ValueError(f"unknown category {category!r}")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


CREDENTIALS_MD = (
    "Credentials come from a DuckDB secret of type `prefect`, created **after** `ATTACH`:\n\n"
    "```sql\n"
    "-- Prefect Cloud: the workspace's API URL and an API key\n"
    "CREATE SECRET pf (TYPE prefect,\n"
    "  API_URL 'https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>',\n"
    "  API_KEY 'pnu_...');\n"
    "-- Self-hosted Prefect server (AUTH_STRING only if the server sets PREFECT_SERVER_API_AUTH_STRING)\n"
    "CREATE SECRET pf (TYPE prefect, API_URL 'http://localhost:4200/api', AUTH_STRING 'admin:...');\n"
    "```"
)

CATALOG_DESCRIPTION = (
    "Prefect 3's REST API (Prefect Cloud or a self-hosted server) as SQL tables: flows, deployments and "
    "schedules, flow runs, task runs, run logs, work pools, queues and workers, concurrency limits, "
    "automations, events, artifacts, blocks and variables, plus cross-orchestrator unified runs."
)

CATALOG_DOC_LLM = (
    "Query one Prefect 3 workspace (Prefect Cloud) or self-hosted Prefect server through its REST API. Requires a "
    "DuckDB secret created after ATTACH: CREATE SECRET (TYPE prefect, API_URL '<PREFECT_API_URL>', API_KEY '...') "
    "for Cloud, or (TYPE prefect, API_URL 'http://host:4200/api' [, AUTH_STRING 'user:pass']) for a server. "
    "Schema `main`: flows, deployments, deployment_schedules, flow_runs, task_runs, flow_run_logs (lines logged by "
    "the flow itself), task_run_logs (lines logged inside tasks), work_pools, work_queues, workers, "
    "concurrency_limits, automations, events, artifacts, block_types, block_documents (names only, never values), "
    "variables (values always masked) and the table function variable_values(reveal := false). Schema `unified`: "
    "runs and task_runs in the shape shared with the Airflow, Dagster and other orchestrator workers. Filters on "
    "ids, names, state_type/state_name, tags (list_contains(tags, 'x')), deployment, work pool, timestamp ranges, "
    "log level and event name/resource are translated into Prefect's JSON filter bodies; always filter by flow "
    "and a time range on large workspaces. Timestamps are `TIMESTAMPTZ` in UTC."
)

CATALOG_DOC_MD = (
    "# Prefect\n\n"
    "Query a Prefect 3 workspace's flows, deployments, runs, logs, work pools, automations, events and "
    "artifacts with SQL. Works with Prefect Cloud and self-hosted Prefect servers.\n\n"
    f"{CREDENTIALS_MD}\n\n"
    "Filters on names, ids, states, tags and time ranges become Prefect's own `POST /…/filter` bodies, so a "
    "query for one flow's failed runs last week reads only those runs."
)

MAIN_DOC_LLM = (
    "Native Prefect resources, one table per REST collection. Join keys: flows.id = flow_runs.flow_id = "
    "deployments.flow_id; flow_runs.id = task_runs.flow_run_id = flow_run_logs.flow_run_id = "
    "task_run_logs.flow_run_id = artifacts.flow_run_id; task_runs.id = task_run_logs.task_run_id; "
    "deployments.id = flow_runs.deployment_id = deployment_schedules.deployment_id. state_type is Prefect's "
    "upper-case StateType (SCHEDULED, PENDING, RUNNING, PAUSED, CANCELLING, COMPLETED, FAILED, CRASHED, "
    "CANCELLED); state_name is the display name (Late, AwaitingRetry, Retrying, Cached, ...). Pushed down to the "
    "API: id/name/flow_id/flow_name/deployment_id/deployment_name/work_pool_name/work_queue_name (= or IN), "
    "state_type and state_name (= or IN), list_contains(tags, 'x'), start_time/end_time/expected_start_time "
    "ranges, log level/level_name and timestamp ranges, event name, resource_id and occurred ranges."
)

MAIN_DOC_MD = (
    "## main\n\n"
    "Prefect's REST resources as tables. A flow has deployments and flow runs, a flow run has task runs, "
    "and both emit logs and artifacts.\n\n"
    "Filter by flow, state and a time range wherever you can: those predicates become Prefect filter bodies, "
    "so the worker reads only the matching runs instead of the whole history.\n\n"
    "Secrets stay in Prefect. Variable values are masked unless you ask for non-secret ones, and block "
    "documents are listed by name and type only."
)

UNIFIED_DOC_LLM = (
    "Cross-orchestrator tables with columns identical across the Airflow, Dagster, Prefect, Temporal, "
    "Databricks, dbt and GitHub workers, for UNION ALL. runs(orchestrator, run_id, pipeline, status, raw_status, "
    "trigger, queued_at, started_at, ended_at, duration_seconds, url) and task_runs(orchestrator, run_id, "
    "task_run_id, task_name, status, raw_status, attempt, started_at, ended_at, duration_seconds, url). status is "
    "one of queued, running, success, failed, cancelled, skipped, derived from Prefect's state type (CRASHED counts "
    "as failed; SCHEDULED and PENDING as queued; PAUSED and CANCELLING as running). raw_status is the state name. "
    "runs.run_id is the flow-run id and pipeline the flow name; task_runs.run_id joins runs.run_id. Filters on "
    "run_id, pipeline, status, raw_status and started_at/ended_at ranges are pushed to the API."
)

UNIFIED_DOC_MD = (
    "## unified\n\n"
    "Flow runs and task runs in the shape every Query.Farm orchestrator worker shares, so you can "
    "`UNION ALL` Prefect with Airflow, Dagster, Temporal and others.\n\n"
    "`status` comes from Prefect's state type (a crashed run counts as `failed`); `raw_status` keeps the state "
    "name such as `Late` or `AwaitingRetry`."
)


def _doc(comment: str, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    return {
        "comment": comment,
        "title": title,
        "category": category,
        "keywords": keywords,
        "doc_llm": doc_llm,
        "doc_md": doc_md,
    }


TABLE_DOCS: dict[str, dict[str, str]] = {
    "flows": _doc(
        "Prefect flows: name, tags and labels.",
        "Prefect Flows",
        "flows",
        "flow, flows, pipeline, workflow, tags",
        "One row per flow, keyed by id. Pushed down: id and name (= or IN) and list_contains(tags, 'x'). "
        "Join flow_runs ON flow_runs.flow_id = flows.id.",
        "Every flow the workspace has seen, with its tags and labels.",
    ),
    "deployments": _doc(
        "Prefect deployments: flow, work pool and queue, paused flag, parameters and status.",
        "Prefect Deployments",
        "flows",
        "deployment, deployments, work pool, work queue, paused, parameters, entrypoint",
        "One row per deployment, keyed by id. flow_name is looked up from flows. Pushed down: id, name, flow_id, "
        "flow_name, work_pool_name, work_queue_name (= or IN), paused (=) and list_contains(tags, 'x'). schedules "
        "are in deployment_schedules. Join flow_runs ON flow_runs.deployment_id = deployments.id.",
        "Deployments with their flow, work pool, queue, parameters and readiness status.",
    ),
    "deployment_schedules": _doc(
        "Schedules attached to Prefect deployments: cron, interval or rrule, timezone and active flag.",
        "Prefect Deployment Schedules",
        "flows",
        "schedule, cron, interval, rrule, timezone, deployment schedule",
        "One row per deployment schedule, keyed by schedule_id. Exactly one of cron, interval_seconds or rrule is "
        "set. Pushed down: deployment_id and deployment_name (= or IN). Join deployments ON "
        "deployment_schedules.deployment_id = deployments.id.",
        "When each deployment runs: cron, interval or RRule schedules with their timezone.",
    ),
    "flow_runs": _doc(
        "Prefect flow runs: state, timings, tags, parameters, deployment and work pool.",
        "Prefect Flow Run History",
        "runs",
        "flow run, runs, state, failed runs, run history, duration, late runs, retries",
        "One row per flow run, keyed by id. state_type is COMPLETED, FAILED, CRASHED, CANCELLED, RUNNING, "
        "SCHEDULED, PENDING, PAUSED or CANCELLING; state_name the display name (Late, AwaitingRetry, ...). "
        "flow_name and deployment_name are looked up. Pushed down: id, name, flow_id, flow_name, deployment_id, "
        "deployment_name, work_pool_name, work_queue_name, state_type, state_name (= or IN), "
        "list_contains(tags, 'x'), and start_time, end_time, expected_start_time ranges. total_run_time is in "
        "seconds. parameters is JSON.",
        "Run history for every flow. Filter by `flow_name`, `state_type` and a time range to keep reads small.",
    ),
    "task_runs": _doc(
        "Prefect task runs: state, run count, retries and timings, keyed to their flow run.",
        "Prefect Task Run History",
        "runs",
        "task run, tasks, task state, retries, failed tasks, duration, cache",
        "One row per task run, keyed by id. run_count counts attempts (2 = one retry); max_retries is the "
        "configured retry budget. Pushed down: id, name, flow_run_id (= or IN), state_type, state_name, "
        "list_contains(tags, 'x'), start_time, end_time and expected_start_time ranges. Join flow_runs ON "
        "task_runs.flow_run_id = flow_runs.id.",
        "Every task run's state, attempt count and duration, per flow run.",
    ),
    "flow_run_logs": _doc(
        "Log lines emitted by Prefect flow runs themselves (not from inside a task).",
        "Prefect Flow Run Logs",
        "logs",
        "logs, flow run logs, log lines, errors, messages, debugging",
        "One row per log line whose task_run_id is NULL. level is the Python logging level (10 debug, 20 info, "
        "30 warning, 40 error, 50 critical); level_name its lower-case name. Pushed down: flow_run_id (= or IN), "
        "level and level_name (= or ranges) and timestamp ranges. Lines logged inside tasks are in task_run_logs; "
        "UNION ALL both for a run's full log.",
        "What each flow run logged, by level and time. Task-level lines are in `task_run_logs`.",
    ),
    "task_run_logs": _doc(
        "Log lines emitted inside Prefect task runs.",
        "Prefect Task Run Logs",
        "logs",
        "logs, task logs, log lines, errors, traceback, retries",
        "One row per log line with a task_run_id. Pushed down: flow_run_id and task_run_id (= or IN), level and "
        "level_name, timestamp ranges. Filter level_name = 'error' to find why a task failed. Join task_runs ON "
        "task_run_logs.task_run_id = task_runs.id.",
        "What each task run logged, with the task and flow run it belongs to.",
    ),
    "work_pools": _doc(
        "Prefect work pools: type, status, paused flag and concurrency limit.",
        "Prefect Work Pools",
        "infrastructure",
        "work pool, infrastructure, process, kubernetes, docker, pool status",
        "One row per work pool, keyed by name. Pushed down: id, name and type (= or IN). base_job_template is "
        "JSON. Join work_queues and workers ON work_pool_name.",
        "Work pools with their type, status and concurrency limit.",
    ),
    "work_queues": _doc(
        "Prefect work queues: pool, priority, paused flag, status and last poll.",
        "Prefect Work Queues",
        "infrastructure",
        "work queue, queue, priority, paused, last polled",
        "One row per work queue, keyed by id. Pushed down: id and name (= or IN). Lower priority numbers are "
        "served first. status is READY, NOT_READY or PAUSED.",
        "Work queues per pool, with priority, status and when a worker last polled them.",
    ),
    "workers": _doc(
        "Prefect workers polling each work pool, with heartbeat and status.",
        "Prefect Workers",
        "infrastructure",
        "worker, workers, heartbeat, online, offline",
        "One row per worker per work pool. Pushed down: work_pool_name (= or IN, otherwise every pool is read), "
        "status (ONLINE / OFFLINE) and last_heartbeat_time ranges.",
        "Which workers are polling which pools, and when they last sent a heartbeat.",
    ),
    "concurrency_limits": _doc(
        "Prefect global and tag-based concurrency limits with their slot usage.",
        "Prefect Concurrency Limits",
        "infrastructure",
        "concurrency, limits, slots, throttling, tag limit",
        "One row per concurrency limit (v2 API, which includes tag limits named 'tag:<tag>'). kind is 'global' or "
        "'tag'; tag is the tag of a tag limit. active_slots is the number of slots currently held.",
        "Global and tag concurrency limits and how many slots are in use.",
    ),
    "automations": _doc(
        "Prefect automations: trigger, actions and enabled flag.",
        "Prefect Automations",
        "automation",
        "automation, trigger, actions, alerts, notifications",
        "One row per automation, keyed by id. trigger and actions are JSON; trigger_type is the trigger's type "
        "(event, compound, sequence, metric). Pushed down: id and name (= or IN), list_contains(tags, 'x').",
        "Automations with their triggers and actions.",
    ),
    "events": _doc(
        "Prefect events: what happened to which resource, and when.",
        "Prefect Event Feed",
        "automation",
        "events, event feed, audit, state changes, resource",
        "One row per event, keyed by id. event is the name, e.g. 'prefect.flow-run.Failed'. resource_id is the "
        "primary resource (e.g. 'prefect.flow-run.<id>'); resource and related are JSON. Pushed down: occurred "
        "ranges (since/until), event (= or IN), starts_with(event, 'prefix') and resource_id (= or IN). Without an "
        "occurred filter every retained event is read: filter occurred on busy workspaces.",
        "The workspace's event feed. Always filter `occurred` to a window.",
    ),
    "artifacts": _doc(
        "Prefect artifacts: key, type, description and data published by runs.",
        "Prefect Artifacts",
        "config",
        "artifact, artifacts, markdown, table, link, report, run output",
        "One row per artifact version, keyed by id. type is markdown, table, link, progress or image; data is "
        "JSON (a table artifact's rows, a markdown string, ...). Pushed down: id, key, type, flow_run_id, "
        "task_run_id (= or IN).",
        "Artifacts that runs published: reports, tables and links.",
    ),
    "block_types": _doc(
        "Prefect block types: name, slug, description and documentation link.",
        "Prefect Block Types",
        "config",
        "block type, blocks, integrations, credentials types",
        "One row per block type, keyed by id. Pushed down: slug (= or IN).",
        "The block types registered in the workspace.",
    ),
    "block_documents": _doc(
        "Prefect block documents: names and types only, never their values.",
        "Prefect Block Documents",
        "config",
        "block, blocks, block document, credentials, configuration",
        "One row per named block document, keyed by id: name, block_type_slug, block_type_name, is_anonymous and "
        "secret_fields (names of the fields Prefect treats as secret). Block data is never read or returned. "
        "Pushed down: id, name, block_type_slug (= or IN) and is_anonymous.",
        "Which blocks exist and of what type. Values are never read.",
    ),
    "variables": _doc(
        "Prefect variables: name and tags; values are always masked.",
        "Prefect Variables",
        "config",
        "variable, variables, configuration, settings",
        "One row per variable, keyed by name. value is always '***' here; use "
        "prefect.main.variable_values(reveal := true) for values that do not look secret. value_type is the JSON "
        "type of the stored value. Pushed down: id and name (= or IN), list_contains(tags, 'x').",
        "Variable names, tags and value types. Values are masked.",
    ),
    "runs": _doc(
        "Prefect flow runs in the cross-orchestrator unified.runs shape.",
        "Unified Runs From Prefect",
        "unified_runs",
        "unified runs, cross orchestrator, union all, run status, pipelines",
        "One row per flow run. run_id is the flow-run id; pipeline the flow name; status queued/running/success/"
        "failed/cancelled (Prefect has no skipped run); raw_status the state name; trigger schedule (auto-scheduled), "
        "event (automation), api (deployment run), manual (flow called directly) or other (subflow). queued_at is "
        "when the run was created. Pushed down: run_id, pipeline, status, raw_status (= or IN), started_at and "
        "ended_at ranges.",
        "Flow runs ready to `UNION ALL` with other orchestrators' runs.",
    ),
    "task_runs_unified": _doc(
        "Prefect task runs in the cross-orchestrator unified.task_runs shape.",
        "Unified Task Runs From Prefect",
        "unified_runs",
        "unified task runs, cross orchestrator, union all, task status",
        "One row per task run. run_id is the flow-run id (joins unified.runs.run_id); task_run_id the task-run id; "
        "attempt the run count (attempts made so far). Pushed "
        "down: run_id, task_run_id, task_name, status, raw_status (= or IN), started_at and ended_at ranges.",
        "Task runs ready to `UNION ALL` with other orchestrators' task runs.",
    ),
}

AGENT_TEST_TASKS = [
    {
        "name": "failed_runs",
        "prompt": "Using the prefect catalog, which flow runs failed or crashed? Give the flow name and run name.",
    },
    {
        "name": "retried_task",
        "prompt": "Using the prefect catalog, which task run needed more than one attempt, and how many attempts?",
    },
    {
        "name": "runs_per_status",
        "prompt": "Using the prefect catalog's unified schema, how many runs are there per unified status?",
    },
    {
        "name": "etl_runs",
        "prompt": "Using the prefect catalog, how many flow runs are tagged 'etl'?",
    },
    {
        "name": "validation_error",
        "prompt": (
            "Using the prefect catalog, what error message did the 'validate' task log in the failed "
            "'broken-import' flow run?"
        ),
    },
    {
        "name": "deployment_schedule",
        "prompt": "Using the prefect catalog, what cron expression and timezone schedule the 'nightly' deployment?",
    },
    {
        "name": "scheduled_runs",
        "prompt": (
            "Using the prefect catalog, how many flow runs of the 'nightly' deployment are scheduled and waiting?"
        ),
    },
    {
        "name": "artifact_keys",
        "prompt": "Using the prefect catalog, how many versions of each artifact key exist?",
    },
    {
        "name": "safe_variables",
        "prompt": "Using the prefect catalog, list the variables whose values are safe to reveal, with their values.",
    },
    {
        "name": "failure_events",
        "prompt": "Using the prefect catalog's event feed, how many 'prefect.flow-run.Failed' events were recorded?",
    },
    {
        "name": "normalize_state",
        "prompt": "What unified status does the prefect worker assign to the Prefect state type 'CRASHED'?",
    },
]

CATALOG_TAGS = {
    "vgi.title": "Prefect",
    "vgi.subtitle": "Prefect 3 flows, runs, logs, deployments, work pools and events as SQL tables",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "prefect, prefect cloud, orchestration, workflow, flow run, task run, deployment, work pool, rest api"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
