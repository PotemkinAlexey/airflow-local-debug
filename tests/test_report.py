from __future__ import annotations

import json

from airflow_local_debug.models import RunResult, TaskRunInfo
from airflow_local_debug.report import _format_duration, format_run_report, write_run_artifacts


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


def test_format_run_report_prints_graph_svg_path() -> None:
    result = RunResult(
        dag_id="demo",
        state="success",
        graph_svg_path="/tmp/graph.svg",
    )

    rendered = format_run_report(result)

    assert "Graph SVG: /tmp/graph.svg" in rendered


def test_format_run_report_prints_task_summary_and_durations() -> None:
    result = RunResult(
        dag_id="demo",
        state="failed",
        tasks=[
            TaskRunInfo(task_id="extract", state="success", duration_seconds=1.25),
            TaskRunInfo(task_id="load", state="failed", duration_seconds=62.4),
            TaskRunInfo(task_id="cleanup", state=None, duration_seconds=0.123),
        ],
    )

    rendered = format_run_report(result)

    assert "Task summary: failed=1, success=1, unknown=1" in rendered
    assert "- extract: success (1.25s)" in rendered
    assert "- load: failed (1m 2s)" in rendered
    assert "- cleanup: unknown (123ms)" in rendered


def test_format_duration_handles_ranges() -> None:
    assert _format_duration(None) is None
    assert _format_duration(-1) is None
    assert _format_duration(0.004) == "4ms"
    assert _format_duration(1.0) == "1s"
    assert _format_duration(1.234) == "1.23s"
    assert _format_duration(3661) == "1h 1m 1s"


def test_write_run_artifacts_persists_snapshot_files(tmp_path) -> None:
    result = RunResult(
        dag_id="demo",
        run_id="manual__2026-05-06",
        state="failed",
        logical_date="2026-05-06T12:00:00+00:00",
        backend="dag.test.strict",
        airflow_version="3.2.1",
        config_path="/tmp/airflow_defaults.py",
        graph_ascii="demo\n  first -> second",
        graph_svg_path="/tmp/graph.svg",
        tasks=[TaskRunInfo(task_id="first", state="failed", duration_seconds=2.5)],
        notes=["note"],
        exception="pretty boom",
        exception_raw="Traceback\nboom",
    )

    artifacts = write_run_artifacts(result, tmp_path / "report", include_graph=False)

    assert set(artifacts) == {"result", "report", "exception", "graph"}
    payload = json.loads(artifacts["result"].read_text(encoding="utf-8"))
    assert payload["dag_id"] == "demo"
    assert payload["graph_svg_path"] == "/tmp/graph.svg"
    assert payload["tasks"] == [
        {
            "end_date": None,
            "duration_seconds": 2.5,
            "map_index": None,
            "start_date": None,
            "state": "failed",
            "task_id": "first",
            "try_number": None,
        }
    ]
    assert "DAG: demo" in artifacts["report"].read_text(encoding="utf-8")
    assert "first -> second" not in artifacts["report"].read_text(encoding="utf-8")
    assert artifacts["exception"].read_text(encoding="utf-8") == "Traceback\nboom\n"
    assert artifacts["graph"].read_text(encoding="utf-8") == "demo\n  first -> second\n"
