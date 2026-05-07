"""
Pytest plugin exposing the local debug runner as a fixture.

Usage in user tests:

    def test_my_dag(airflow_local_runner):
        result = airflow_local_runner.run_dag(
            "/abs/path/to/my_dag.py",
            dag_id="my_dag",
            task_mocks=[...],
        )
        assert result.ok

The plugin is registered automatically via the `pytest11` entry point in
`pyproject.toml`; no `conftest.py` glue is required for downstream projects.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest

from airflow_local_debug.mocks import TaskMockRule
from airflow_local_debug.models import RunResult
from airflow_local_debug.plugins import AirflowDebugPlugin
from airflow_local_debug.runner import run_full_dag, run_full_dag_from_file


class AirflowLocalRunner:
    """Helper exposed by the `airflow_local_runner` pytest fixture.

    `run_dag` dispatches to the file-based or in-process runner based on the
    type of `target`. A `str` / `pathlib.Path` is treated as a DAG file path;
    anything else is assumed to be an already-imported Airflow DAG object.
    """

    def run_dag(
        self,
        target: Any,
        *,
        dag_id: str | None = None,
        config_path: str | None = None,
        logical_date: str | date | datetime | None = None,
        conf: dict[str, Any] | None = None,
        extra_env: dict[str, str] | None = None,
        trace: bool = False,
        fail_fast: bool = True,
        plugins: Iterable[AirflowDebugPlugin] | None = None,
        task_mocks: Iterable[TaskMockRule] | None = None,
        task_ids: Iterable[str] | None = None,
        start_task_ids: Iterable[str] | None = None,
        task_group_ids: Iterable[str] | None = None,
        collect_xcoms: bool = True,
    ) -> RunResult:
        kwargs: dict[str, Any] = {
            "config_path": config_path,
            "logical_date": logical_date,
            "conf": conf,
            "extra_env": extra_env,
            "trace": trace,
            "fail_fast": fail_fast,
            "plugins": plugins,
            "task_mocks": task_mocks,
            "task_ids": task_ids,
            "start_task_ids": start_task_ids,
            "task_group_ids": task_group_ids,
            "collect_xcoms": collect_xcoms,
        }
        if isinstance(target, (str, Path)):
            return run_full_dag_from_file(str(target), dag_id=dag_id, **kwargs)
        if dag_id is not None:
            raise TypeError(
                "dag_id is only meaningful when target is a DAG file path; "
                "remove it when passing a DAG object directly."
            )
        return run_full_dag(target, **kwargs)


@pytest.fixture
def airflow_local_runner() -> AirflowLocalRunner:
    """Return an `AirflowLocalRunner` for executing DAGs inside a test."""
    return AirflowLocalRunner()
