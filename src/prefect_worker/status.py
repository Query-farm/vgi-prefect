"""Scalar ``unified_status(state_type)``: Prefect state type → cross-orchestrator status.

The same mapping the ``unified`` tables use, exposed so you can normalize
states you already have (an exported flow_run table, event payloads, a
webhook feed) without calling the API.
"""

from __future__ import annotations

import json
from typing import Annotated, ClassVar

import pyarrow as pa
import pyarrow.compute as pc
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample, NullHandling
from vgi.scalar_function import ScalarFunction

from .meta import object_tags
from .unified import unified_status

_STATE_DOC = (
    "A Prefect StateType such as COMPLETED, FAILED, CRASHED, RUNNING or SCHEDULED (case-insensitive). "
    "NULL or an unknown type maps to queued."
)


class UnifiedStatus(ScalarFunction):
    """``unified_status(state_type)`` for every row."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "unified_status"
        # NULL is a state too (no state yet): it maps to queued rather than propagating.
        null_handling = NullHandling.SPECIAL
        description = "Map a Prefect state type onto queued/running/success/failed/cancelled"
        tags: ClassVar[dict[str, str]] = {
            **object_tags(
                title="Unified Status From Prefect State",
                category="unified_runs",
                keywords="status mapping, normalize state, state type, crashed, cross orchestrator, unified status",
                doc_llm=(
                    "unified_status(state_type) maps a Prefect StateType to the shared orchestrator vocabulary "
                    "used by the unified schema: COMPLETED→success, FAILED and CRASHED→failed, CANCELLED→cancelled, "
                    "RUNNING, PAUSED and CANCELLING→running, SCHEDULED and PENDING→queued. Case-insensitive; NULL "
                    "and unknown types map to queued. Needs no credentials and makes no API calls."
                ),
                doc_md=(
                    "## unified_status(state_type)\n\n"
                    "Normalize a Prefect state type to `queued`, `running`, `success`, `failed` or `cancelled`, "
                    "exactly as `unified.runs` and `unified.task_runs` do. A crashed run counts as `failed`."
                ),
            ),
            "domain": "orchestration",
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Normalize every Prefect state type",
                        "sql": (
                            "SELECT s AS state_type, prefect.unified.unified_status(s) AS status FROM (VALUES "
                            "('COMPLETED'), ('CRASHED'), ('CANCELLING'), ('SCHEDULED'), ('cancelled')) t(s) "
                            "ORDER BY s"
                        ),
                        "expected_result": [
                            {"state_type": "CANCELLING", "status": "running"},
                            {"state_type": "COMPLETED", "status": "success"},
                            {"state_type": "CRASHED", "status": "failed"},
                            {"state_type": "SCHEDULED", "status": "queued"},
                            {"state_type": "cancelled", "status": "cancelled"},
                        ],
                    },
                    {
                        "description": "A missing state counts as queued",
                        "sql": "SELECT prefect.unified.unified_status(NULL::VARCHAR) AS status",
                        "expected_result": [{"status": "queued"}],
                    },
                ]
            ),
        }
        examples: ClassVar[list[FunctionExample]] = [
            FunctionExample(
                sql=(
                    "SELECT prefect.unified.unified_status(state_type) AS status, count(*) AS task_runs "
                    "FROM prefect.main.task_runs GROUP BY ALL ORDER BY status"
                ),
                description="Task runs per unified status",
            ),
        ]

    @classmethod
    def compute(
        cls,
        state_type: Annotated[pa.StringArray, Param(doc=_STATE_DOC)],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Map each state type; NULL maps to queued."""
        # Map the handful of distinct values once, then gather: no per-row Python.
        distinct = pc.unique(state_type)
        mapped = pa.array([unified_status(s) for s in distinct.to_pylist()], type=pa.string())
        return pc.take(mapped, pc.index_in(state_type, value_set=distinct, skip_nulls=False))
