<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Prefect Flows, Runs and Logs in DuckDB

> **vgi-prefect** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-prefect/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-prefect/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

The Prefect 3 REST API as SQL tables, for **Prefect Cloud** workspaces and **self-hosted Prefect
servers**. You can query flows, deployments and their schedules, flow runs, task runs, run logs,
work pools, queues and workers, concurrency limits, automations, the event feed, artifacts, blocks
(names only) and variables (masked). It also provides `unified.runs` and `unified.task_runs`,
which have the same shape as the Airflow, Dagster and other Query.Farm orchestrator workers.

```sql
-- Which flows failed or crashed this month, and how long did those runs take?
SELECT flow_name, count(*) AS failures, round(avg(total_run_time), 1) AS avg_s
FROM prefect.main.flow_runs
WHERE state_type IN ('FAILED', 'CRASHED') AND start_time >= TIMESTAMPTZ '2026-11-01'
GROUP BY flow_name ORDER BY failures DESC;
```

The state filter and the time range are sent to Prefect inside the `POST /flow_runs/filter`
body, so the worker fetches only the failed November runs.

## Quick start

**DuckDB 2.0:**

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;

ATTACH 'prefect' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-prefect@v0.1.0 vgi-prefect-worker');

-- Credentials go in a DuckDB secret. Create it after ATTACH, which registers the secret type.
CREATE SECRET pf (TYPE prefect,
  API_URL 'https://api.prefect.cloud/api/accounts/<account_id>/workspaces/<workspace_id>',
  API_KEY 'pnu_...');

SELECT name, state_name, start_time FROM prefect.main.flow_runs ORDER BY start_time DESC LIMIT 20;
```

**Haybarn 2.0** (`uvx haybarn-cli`): use `INSTALL vgi FROM community; LOAD vgi;` and the same
`ATTACH`.

**From a checkout:** run `make build`, then use
`LOCATION '/abs/path/to/vgi-prefect/.venv/bin/vgi-prefect-worker'`.

## Credentials

Every table reads the session's `prefect` secret:

| Field | Meaning |
|---|---|
| `API_URL` (required) | The value `PREFECT_API_URL` would have. For Prefect Cloud: `https://api.prefect.cloud/api/accounts/<a>/workspaces/<w>`. For a server: `http://host:4200/api`. |
| `API_KEY` | Prefect Cloud API key (user or service account), sent as a bearer token. |
| `AUTH_STRING` | For a server started with `PREFECT_SERVER_API_AUTH_STRING`: the same `user:password`, sent as HTTP basic auth. |
| `UI_URL` | Base URL for the `url` columns. The default is derived from `API_URL`: `https://app.prefect.cloud/account/<a>/workspace/<w>` for Cloud, or `API_URL` without `/api` for a server. |
| `PAGE_SIZE` | Objects requested per page, from 1 to 200 (default 200, Prefect's maximum). Events always use 50 or fewer. |
| `TIMEOUT` | Per-request timeout in seconds (default 30). |

```sql
-- Self-hosted server, no auth
CREATE SECRET pf (TYPE prefect, API_URL 'http://localhost:4200/api');
-- Self-hosted server with basic auth
CREATE SECRET pf (TYPE prefect, API_URL 'https://prefect.internal/api', AUTH_STRING 'admin:...');
```

`API_KEY` and `AUTH_STRING` are marked `redact`, so `duckdb_secrets()` hides them. They are never
written to the scan cursor or to logs.

## Tables and functions

Schema `prefect.main`:

| Table | One row per | Pushed down to the API |
|---|---|---|
| `flows` | flow | `id`, `name`, `list_contains(tags, …)` |
| `deployments` | deployment | `id`, `name`, `flow_id`, `flow_name`, `work_pool_name`, `work_queue_name`, `paused`, tags |
| `deployment_schedules` | deployment schedule (cron / interval / rrule) | `deployment_id`, `deployment_name` |
| `flow_runs` | flow run | `id`, `name`, `flow_id`, `flow_name`, `deployment_id`, `deployment_name`, `work_pool_name`, `work_queue_name`, `state_type`, `state_name`, `parent_task_run_id`, tags, `start_time` / `end_time` / `expected_start_time` ranges |
| `task_runs` | task run | `id`, `name`, `flow_run_id`, `state_type`, `state_name`, tags, `start_time` / `end_time` / `expected_start_time` ranges |
| `flow_run_logs` | log line logged by a flow run itself (`task_run_id IS NULL`) | `flow_run_id`, `level` / `level_name`, `timestamp` range |
| `task_run_logs` | log line logged inside a task run | `flow_run_id`, `task_run_id`, `level` / `level_name`, `timestamp` range |
| `work_pools` | work pool | `id`, `name`, `type` |
| `work_queues` | work queue | `id`, `name`; `work_pool_name` selects the per-pool endpoint |
| `workers` | worker per pool | `work_pool_name` (which pools to read), `status`, `last_heartbeat_time` range |
| `concurrency_limits` | global or tag concurrency limit (v2 API) | (none) |
| `automations` | automation | `id`, `name`, tags |
| `events` | event | `occurred` range (`since` / `until`), `event` (= / IN, `starts_with` / `LIKE 'p%'` as a prefix), `resource_id` |
| `artifacts` | artifact version | `id`, `key`, `type`, `flow_run_id`, `task_run_id` |
| `block_types` | block type | `slug` |
| `block_documents` | named block: name, type and the *names* of its secret fields | `id`, `name`, `block_type_slug`, `is_anonymous` |
| `variables` | variable; `value` is always `***` | `id`, `name`, tags |
| `variable_values(reveal := false)` | variable; with `reveal := true`, values that don't look secret | |

Schema `prefect.unified` has the cross-orchestrator shape that every Query.Farm orchestrator worker
shares:

- `runs(orchestrator, run_id, pipeline, status, raw_status, trigger, queued_at, started_at, ended_at, duration_seconds, url)`:
  one row per flow run. `pipeline` is the flow name. `trigger` is `schedule`, `event` (automation),
  `api` (deployment run), `manual` (flow called directly) or `other` (subflow). `queued_at` is the
  run's creation time.
- `task_runs(orchestrator, run_id, task_run_id, task_name, status, raw_status, attempt, started_at, ended_at, duration_seconds, url)`:
  one row per task run inside a flow run. `run_id` joins `runs.run_id`, and `attempt` is Prefect's
  `run_count`.
- `unified_status(state_type)`: the status mapping as a scalar. COMPLETED → success, FAILED and
  CRASHED → failed, CANCELLED → cancelled, RUNNING, PAUSED and CANCELLING → running, SCHEDULED and
  PENDING → queued. `raw_status` keeps Prefect's state name (`Late`, `AwaitingRetry`, `Cached`, ...).

```sql
SELECT orchestrator, pipeline, status, count(*) FROM prefect.unified.runs GROUP BY ALL
UNION ALL
SELECT orchestrator, pipeline, status, count(*) FROM airflow.unified.runs GROUP BY ALL;
```

### How pushdown and paging work

Prefect's list endpoints are `POST /<collection>/filter` with a JSON filter body. Each table
translates the predicates DuckDB pushes into that body:

- `=` and `IN` become `any_`.
- Timestamp ranges become `after_` / `before_`.
- `list_contains(tags, 'x')` becomes `tags.all_`.
- Flow, deployment and work-pool names go into the `flows` / `deployments` / `work_pools` sections.
- Log levels become `level.ge_` / `le_`.
- Events use `occurred.since` / `until`, `event.name`, `event.prefix` and `resource.id`.

DuckDB re-applies every predicate to the returned rows, so a translation only has to narrow the
result, never match it exactly. Prefect's `before_` / `after_` are inclusive, and its
`start_time` filter also matches runs that haven't started yet by their expected start.

Paging uses `limit` / `offset` (events use the `/events/filter/next` page token). The cursor holds
only the listing index, the offset and the page token, so a scan can resume on any worker
process over the HTTP transport. HTTP 429 and 502/503/504 are retried with backoff that honours
`Retry-After`.

Offset paging reads a moving target. If runs are created while a long scan is in progress, a row
can appear twice or be skipped. Filter by a time range to keep scans short.

`events` with no `occurred` filter reads every event Prefect still retains; Prefect's own default is
the last day. On a busy workspace, filter `occurred`.

### Secrets never leak

- `variables.value` is always `***`. `variable_values(reveal := true)` shows a value only when its
  name doesn't look secret (password, token, key, secret, auth, dsn, ...) and the value doesn't look
  like a credential (JWT, API-key prefixes, private keys, URLs with an embedded password).
- `block_documents` never reads block data. The worker always sends `include_secrets: false`,
  drops `data`, and lists only the names of the fields Prefect treats as secret.

## Testing

```bash
make check      # ruff, mypy, pydoclint, pytest, SQL tests over stdio/launch/http, vgi-lint
```

The SQL suite (`test/sql/*.test`) runs through the real `vgi` extension in Haybarn 2.0, against
`test/mock_api/server.py`. That server replays `test/fixtures/prefect3.json`, which was recorded
from a real **Prefect 3.9.3** server (`prefect server start`, SQLite, no Docker). The server was
seeded with the flows in `test/prefect/flows.py`:

- an ETL flow run with tags,
- a report whose `render` task fails once and succeeds on retry,
- a flow whose `validate` task fails,
- a deployment with a cron schedule, worker-executed and cancelled runs,
- logs, markdown / table / link artifacts, variables, blocks, concurrency limits and an automation.

The replay serves the data on two mounts: `/api` with basic auth, like a self-hosted server, and
`/api/accounts/<a>/workspaces/<w>` with a bearer API key, like Prefect Cloud. It also:

- **validates every filter body against the OpenAPI grammar recorded from the server.** An unknown
  key at any depth is a 422 `extra_forbidden`, as in Prefect, so a mistranslated pushdown fails the
  tests.
- implements the filter semantics.
- logs each request at `/__requests`. `test/sql/pushdown.test` reads that log to assert the exact
  bodies.
- answers 429 to the first request on every path, so every run exercises the retry path.

To re-record:

```bash
uv venv /tmp/prefect-env && uv pip install -p /tmp/prefect-env prefect==3.9.3
export PREFECT_HOME=/tmp/prefect-home PREFECT_SERVER_API_AUTH_STRING=admin:prefect-fixture-pw
/tmp/prefect-env/bin/prefect server start --port 4299 &     # note the PID; stop it by PID afterwards
export PREFECT_API_URL=http://127.0.0.1:4299/api PREFECT_API_AUTH_STRING=admin:prefect-fixture-pw
/tmp/prefect-env/bin/python test/prefect/seed.py configure
/tmp/prefect-env/bin/python test/prefect/seed.py run
/tmp/prefect-env/bin/prefect worker start --pool local-pool --name fixture-worker --run-once
/tmp/prefect-env/bin/python test/prefect/seed.py cancel
/tmp/prefect-env/bin/python test/prefect/record.py test/fixtures/prefect3.json
PREFECT_LIVE=http://127.0.0.1:4299 test/prefect/run-live.sh   # the same queries against real Prefect
```

`test/prefect/run-live.sh` runs `prefect.test` and every data query of `pushdown.test` against the
live server. It shows that real Prefect accepts each filter body the worker sends and returns the
same results as the replay.

The metadata check (`make lint-explain`) scores 100/100 with zero findings at `fail_on = "info"`.
The tables need a live Prefect API and credentials, so CI runs the lint with `execute: "false"`.
The SQL suite covers the tables against the replay instead.

## Deploying

The same entry point serves stdio, where DuckDB starts it, or HTTP
(`vgi-prefect-worker --http --port 8000`, or the Dockerfile). Over HTTP, credentials still come
from the caller's DuckDB secret, never from the server's environment.

## Support

Open an issue on [Query-farm/vgi-prefect](https://github.com/Query-farm/vgi-prefect/issues).
For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the [BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm

Prefect is a trademark of Prefect Technologies, Inc. This project is not affiliated with or
endorsed by Prefect Technologies.
