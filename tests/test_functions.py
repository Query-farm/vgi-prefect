"""Unit tests for the client, pushdown translation, masking and status mapping.

The SQL-level tests (through DuckDB and the real vgi extension) live in test/sql.
"""

from __future__ import annotations

import datetime as dt
import json
from typing import Any

import pyarrow as pa
import pytest
from vgi.table_filter_pushdown import ComparisonOp, ConstantFilter, InFilter, PushdownFilters

from prefect_worker.client import PrefectClient, PrefectConfig, PrefectError, retry_delay
from prefect_worker.masking import mask_variable
from prefect_worker.scan import Pushdown, ScanState, iso, listing
from prefect_worker.tables import (
    Events,
    FlowRunLogs,
    FlowRuns,
    TaskRunLogs,
    Workers,
    _json_value,
    _range_prefix,
    flow_run_body,
    variable_rows,
)
from prefect_worker.unified import UnifiedRuns, UnifiedTaskRuns, task_name, trigger, unified_status
from prefect_worker.worker import PrefectWorker

UTC = dt.UTC
TS = pa.timestamp("us", tz="UTC")


def _pd(*filters: Any) -> Pushdown:
    return Pushdown(PushdownFilters(filters=tuple(filters)))


def _eq(col: str, value: Any, typ: pa.DataType | None = None) -> ConstantFilter:
    return ConstantFilter(column_name=col, column_index=0, op=ComparisonOp.EQ, value=pa.scalar(value, typ))


def _cmp(col: str, op: ComparisonOp, value: Any, typ: pa.DataType = TS) -> ConstantFilter:
    return ConstantFilter(column_name=col, column_index=0, op=op, value=pa.scalar(value, typ))


def _in(col: str, values: list[Any]) -> InFilter:
    return InFilter(column_name=col, column_index=0, values=pa.array(values))


def _client(**fields: Any) -> PrefectClient:
    return PrefectClient(PrefectConfig.from_secret({"api_url": "http://prefect.invalid/api", **fields}))


# ------------------------------------------------------------------ configuration


def test_config_requires_secret() -> None:
    with pytest.raises(PrefectError, match="no Prefect credentials"):
        PrefectConfig.from_secret(None)


def test_config_validates_fields() -> None:
    with pytest.raises(PrefectError, match="API_URL"):
        PrefectConfig.from_secret({"api_url": "localhost:4200"})
    with pytest.raises(PrefectError, match="AUTH_STRING"):
        PrefectConfig.from_secret({"api_url": "http://h/api", "auth_string": "nocolon"})
    with pytest.raises(PrefectError, match="PAGE_SIZE"):
        PrefectConfig.from_secret({"api_url": "http://h/api", "page_size": 201})


def test_config_derives_ui_url() -> None:
    cloud = PrefectConfig.from_secret(
        {"api_url": pa.scalar("https://api.prefect.cloud/api/accounts/a1/workspaces/w1/"), "api_key": "pnu_x"}
    )
    assert cloud.api_url == "https://api.prefect.cloud/api/accounts/a1/workspaces/w1"
    assert cloud.ui_url == "https://app.prefect.cloud/account/a1/workspace/w1"
    server = PrefectConfig.from_secret({"api_url": "http://localhost:4200/api"})
    assert server.ui_url == "http://localhost:4200"
    custom = PrefectConfig.from_secret({"api_url": "http://h/api", "ui_url": "https://ui.example/"})
    assert custom.ui_url == "https://ui.example"


def test_config_repr_hides_secrets() -> None:
    cfg = PrefectConfig.from_secret({"api_url": "http://h/api", "api_key": "pnu_secret", "auth_string": "a:hunter2"})
    assert "pnu_secret" not in repr(cfg) and "hunter2" not in repr(cfg)


def test_auth_headers() -> None:
    assert _client(api_key="pnu_k")._headers()["Authorization"] == "Bearer pnu_k"
    assert _client(auth_string="admin:pw")._headers()["Authorization"] == "Basic YWRtaW46cHc="
    assert "Authorization" not in _client()._headers()


def test_retry_delay_honours_retry_after() -> None:
    assert retry_delay("3", 1) == 3.0
    assert retry_delay("9999", 1) == 60.0
    assert retry_delay(None, 2) == 2.0


def test_client_retries_429_then_succeeds(mock_api: str) -> None:
    client = PrefectClient(
        PrefectConfig.from_secret({"api_url": f"{mock_api}/api", "auth_string": "admin:prefect-fixture-pw"})
    )
    # The replay answers the first request to each path with 429 + Retry-After: 0.
    flows = client.post("/flows/filter", {"limit": 10})
    assert sorted(f["name"] for f in flows) == ["broken-import", "etl-orders", "flaky-report"]


def test_client_reports_422_detail(mock_api: str) -> None:
    client = PrefectClient(
        PrefectConfig.from_secret({"api_url": f"{mock_api}/api", "auth_string": "admin:prefect-fixture-pw"})
    )
    client.post("/variables/filter", {})  # absorb the replay's first-request 429
    with pytest.raises(PrefectError, match=r"HTTP 422.*extra_forbidden"):
        client.post("/variables/filter", {"variables": {"bogus": {"any_": ["x"]}}})


# ------------------------------------------------------------------ pushdown


def test_iso_formats_utc_z() -> None:
    assert iso(dt.datetime(2026, 11, 15, 7, 30, tzinfo=UTC)) == "2026-11-15T07:30:00Z"
    assert iso(dt.date(2026, 11, 15)) == "2026-11-15T00:00:00Z"


def test_flow_run_body_translates_predicates() -> None:
    pd = _pd(
        _eq("flow_name", "etl"),
        _in("state_type", ["failed", "CRASHED"]),
        _cmp("start_time", ComparisonOp.GE, dt.datetime(2026, 11, 1, tzinfo=UTC)),
        _cmp("start_time", ComparisonOp.LT, dt.datetime(2026, 11, 2, tzinfo=UTC)),
        _eq("work_pool_name", "pool"),
    )
    body = flow_run_body(pd)
    assert body == {
        "sort": "ID_DESC",
        "flows": {"name": {"any_": ["etl"]}},
        "flow_runs": {
            "state": {"type": {"any_": ["CRASHED", "FAILED"]}},
            "start_time": {"after_": "2026-11-01T00:00:00Z", "before_": "2026-11-02T00:00:00Z"},
        },
        "work_pools": {"name": {"any_": ["pool"]}},
    }


def test_empty_in_list_reads_nothing() -> None:
    assert FlowRuns.requests(_client(), _pd(_in("state_type", [None]))) == []


def test_log_levels() -> None:
    body = FlowRunLogs.requests(_client(), _pd(_in("level_name", ["warning", "error"])))[0].body
    assert body["logs"] == {"task_run_id": {"is_null_": True}, "level": {"ge_": 30, "le_": 40}}
    body = TaskRunLogs.requests(_client(), _pd(_cmp("level", ComparisonOp.GE, 40, pa.int32())))[0].body
    assert body["logs"] == {"task_run_id": {"is_null_": False}, "level": {"ge_": 40}}
    assert FlowRunLogs.requests(_client(), _pd(_eq("level_name", "loud"))) == []


def test_event_prefix_from_range() -> None:
    assert _range_prefix(("prefect.worker.", True, "prefect.worker/", False)) == ["prefect.worker."]
    assert _range_prefix(("prefect.a", True, "prefect.c", True)) == ["prefect."]
    assert _range_prefix(("a", True, "b", True)) == []
    assert _range_prefix(None) == []
    flt = Events.requests(_client(), _pd())[0].body["filter"]
    assert flt["occurred"] == {"since": "2000-01-01T00:00:00Z"}


def test_workers_fan_out_per_pool() -> None:
    lst = Workers.requests(_client(), _pd(_in("work_pool_name", ["b pool", "a"]), _eq("status", "online")))
    assert [x.path for x in lst] == ["/work_pools/a/workers/filter", "/work_pools/b%20pool/workers/filter"]
    assert lst[0].body == {"workers": {"status": {"any_": ["ONLINE"]}}}


def test_unified_pushdown() -> None:
    lst = UnifiedRuns.requests(_client(), _pd(_eq("status", "failed"), _eq("pipeline", "etl")))
    assert lst[0].body["flow_runs"]["state"]["type"]["any_"] == ["CRASHED", "FAILED"]
    assert lst[0].body["flows"]["name"]["any_"] == ["etl"]
    assert UnifiedRuns.requests(_client(), _pd(_eq("status", "skipped"))) == []
    task = UnifiedTaskRuns.requests(_client(), _pd())[0].body
    assert task["task_runs"]["flow_run_id"] == {"is_null_": False}


def test_listing_and_state_are_credential_free() -> None:
    lst = listing("/flows/filter", {"a": 1}, work_pool_name="p")
    assert lst.body == {"a": 1} and dict(lst.context) == {"work_pool_name": "p"}
    assert set(ScanState().__dataclass_fields__) == {"listing", "offset", "token"}


# ------------------------------------------------------------------ mapping


def test_unified_status_and_trigger() -> None:
    assert [unified_status(s) for s in ("COMPLETED", "crashed", "CANCELLING", "PAUSED", None, "WAT")] == [
        "success",
        "failed",
        "running",
        "running",
        "queued",
        "queued",
    ]
    assert trigger({"auto_scheduled": True}) == "schedule"
    assert trigger({"created_by": {"type": "AUTOMATION"}, "deployment_id": "d"}) == "event"
    assert trigger({"parent_task_run_id": "t"}) == "other"
    assert trigger({"created_by": {"type": "USER"}, "deployment_id": "d"}) == "manual"
    assert trigger({"deployment_id": "d"}) == "api"
    assert trigger({}) == "manual"


def test_task_name_strips_dynamic_suffix() -> None:
    assert task_name({"name": "render-132", "dynamic_key": "132b71f3"}) == "render"
    assert task_name({"name": "custom", "dynamic_key": "0"}) == "custom"


def test_variable_masking() -> None:
    assert mask_variable("report_owner", "data-team", reveal=True) == ("data-team", False)
    assert mask_variable("report_owner", "data-team", reveal=False) == ("***", True)
    assert mask_variable("warehouse_password", "x", reveal=True) == ("***", True)
    assert mask_variable("hook", "https://u:pw@h/x", reveal=True) == ("***", True)
    rows = variable_rows([{"id": "1", "name": "n", "value": 100, "tags": None}], reveal=True)
    assert rows[0]["value"] == "100" and rows[0]["value_type"] == "number" and rows[0]["tags"] == []


def test_artifact_data_json() -> None:
    assert json.loads(_json_value('[{"a": 1}]', parse_strings=True) or "") == [{"a": 1}]
    assert json.loads(_json_value("# md", parse_strings=False) or "") == "# md"
    assert _json_value(None, parse_strings=True) is None


def test_catalog_shape() -> None:
    schemas = {tuple(s.path): s for s in PrefectWorker.catalog.schemas}
    main = {t.name for t in schemas[("main",)].tables}
    assert {t.name for t in schemas[("unified",)].tables} == {"runs", "task_runs"}
    assert {"flows", "flow_runs", "task_runs", "flow_run_logs", "task_run_logs", "events", "variables"} <= main
    assert [f.name for f in UnifiedRuns.FIXED_SCHEMA] == [
        "orchestrator",
        "run_id",
        "pipeline",
        "status",
        "raw_status",
        "trigger",
        "queued_at",
        "started_at",
        "ended_at",
        "duration_seconds",
        "url",
    ]
    assert [f.name for f in UnifiedTaskRuns.FIXED_SCHEMA] == [
        "orchestrator",
        "run_id",
        "task_run_id",
        "task_name",
        "status",
        "raw_status",
        "attempt",
        "started_at",
        "ended_at",
        "duration_seconds",
        "url",
    ]
