from __future__ import annotations

from airflow_local_debug.models import RunResult, TaskRunInfo
from airflow_local_debug.report import format_run_report


def test_format_run_report_hides_duplicate_live_exception() -> None:
    result = RunResult(
        dag_id="demo",
        state="failed",
        tasks=[TaskRunInfo(task_id="a", state="failed")],
        exception="boom",
        exception_was_logged=True,
    )

    rendered = format_run_report(result)

    assert "Tasks:" in rendered
    assert "Exception:" not in rendered


def test_format_run_report_prints_exception_when_not_logged() -> None:
    result = RunResult(
        dag_id="demo",
        state="failed",
        exception="boom",
        exception_was_logged=False,
    )

    rendered = format_run_report(result)

    assert "Exception:" in rendered
    assert "boom" in rendered
