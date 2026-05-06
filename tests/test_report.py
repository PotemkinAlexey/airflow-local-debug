from __future__ import annotations

import json

from airflow_local_debug.models import RunResult, TaskRunInfo
from airflow_local_debug.report import format_run_report, write_run_artifacts


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
        tasks=[TaskRunInfo(task_id="first", state="failed")],
        notes=["note"],
        exception="pretty boom",
        exception_raw="Traceback\nboom",
    )

    artifacts = write_run_artifacts(result, tmp_path / "report", include_graph=False)

    assert set(artifacts) == {"result", "report", "exception", "graph"}
    payload = json.loads(artifacts["result"].read_text(encoding="utf-8"))
    assert payload["dag_id"] == "demo"
    assert payload["tasks"] == [
        {
            "end_date": None,
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
