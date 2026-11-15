"""Shared fixtures: the replay of the recorded Prefect 3 API."""

from __future__ import annotations

import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def mock_api() -> Iterator[str]:
    """Base URL (no /api) of the replay, rate-limiting the first request to every path."""
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "test" / "mock_api" / "server.py")],
        stdout=subprocess.PIPE,
        text=True,
        env={"MOCK_FLAKY": "1", "PATH": ""},
    )
    assert proc.stdout is not None
    line = proc.stdout.readline().strip()
    assert line.startswith("PORT:"), line
    yield f"http://127.0.0.1:{line.removeprefix('PORT:')}"
    proc.terminate()
    proc.wait(timeout=10)
