"""Table function ``variable_values(reveal := false)``: variables with opt-in revealing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

import pyarrow as pa
from vgi.arguments import Arg, Secret
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import PREFECT_SECRET_TYPE
from .scan import client_from
from .tables import VARIABLE_FIELDS, variable_rows


@dataclass(slots=True, frozen=True, kw_only=True)
class VariableArgs:
    """Arguments for ``variable_values(reveal := false)``."""

    reveal: Annotated[
        bool,
        Arg(
            "reveal",
            default=False,
            doc="Show values whose name and value do not look secret. Default false: every value is '***'.",
        ),
    ]


@dataclass(kw_only=True)
class OffsetState(ArrowSerializableDataclass):
    """Cursor: next offset, or -1 when done."""

    offset: int = 0


@init_single_worker
class VariableValues(TableFunctionGenerator[VariableArgs, OffsetState]):
    """Prefect variables with opt-in revealing of non-secret values."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = pa.schema(VARIABLE_FIELDS)

    class Meta:
        """Function metadata; tags are filled in by ``worker.py``."""

        name = "variable_values"
        tags: ClassVar[dict[str, str]] = {}
        examples: ClassVar[list[FunctionExample]] = []
        description = "Prefect variables; reveal := true shows values whose name and value do not look secret"

    @classmethod
    def on_bind(
        cls,
        params: BindParams[VariableArgs],
        *,
        prefect: Annotated[dict[str, pa.Scalar[Any]] | None, Secret(PREFECT_SECRET_TYPE)] = None,
    ) -> BindResponse:
        """Return the fixed schema and ask DuckDB for the ``prefect`` secret."""
        return BindResponse(output_schema=pa.schema(VARIABLE_FIELDS))

    @classmethod
    def initial_state(cls, params: ProcessParams[VariableArgs]) -> OffsetState:
        """Start at offset 0."""
        return OffsetState()

    @classmethod
    def process(cls, params: ProcessParams[VariableArgs], state: OffsetState, out: OutputCollector) -> None:
        """Emit one page of variables per call."""
        if state.offset < 0:
            out.finish()
            return
        client = client_from(params)
        limit = client.config.page_size
        items = list(
            client.post("/variables/filter", {"sort": "NAME_ASC", "limit": limit, "offset": state.offset}) or []
        )
        state.offset = -1 if len(items) < limit else state.offset + len(items)
        if not items:
            out.finish()
            return
        rows = variable_rows(items, reveal=params.args.reveal)
        schema = params.output_schema
        out.emit(pa.RecordBatch.from_pydict({f.name: [r[f.name] for r in rows] for f in schema}, schema=schema))
