from __future__ import annotations

import json
from xml.etree import ElementTree

from airflow_local_debug.models import DeferrableTaskInfo, RunResult, TaskMockInfo, TaskRunInfo
from airflow_local_debug.report import _format_duration, format_run_gantt, format_run_report, write_run_artifacts


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


def test_format_run_report_prints_selected_tasks() -> None:
    result = RunResult(
        dag_id="demo",
        state="success",
        selected_tasks=["load", "notify"],
    )

    rendered = format_run_report(result)

    assert "Selected tasks: load, notify" in rendered


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


def test_format_run_report_prints_mocked_tasks() -> None:
    result = RunResult(
        dag_id="demo",
        state="success",
        tasks=[TaskRunInfo(task_id="load", state="success", mocked=True)],
        mocks=[TaskMockInfo(task_id="load", mode="success", rule_name="local load", xcom_keys=["return_value"])],
    )

    rendered = format_run_report(result)

    assert "Mocked tasks:" in rendered
    assert "- load: success via local load xcom=return_value" in rendered
    assert "- load: success [mocked]" in rendered


def test_format_run_report_prints_deferrable_tasks() -> None:
    result = RunResult(
        dag_id="demo",
        state="success",
        deferrables=[
            DeferrableTaskInfo(
                task_id="wait",
                operator="ExternalTaskSensor",
                trigger="airflow.triggers.temporal.DateTimeTrigger",
                local_mode="inline-trigger",
            )
        ],
    )

    rendered = format_run_report(result)

    assert "Deferrable tasks:" in rendered
    assert "- wait: ExternalTaskSensor trigger=airflow.triggers.temporal.DateTimeTrigger mode=inline-trigger" in rendered


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
        tasks=[
            TaskRunInfo(task_id="first", state="failed", duration_seconds=2.5),
            TaskRunInfo(task_id="second", state="skipped", duration_seconds=0),
        ],
        deferrables=[DeferrableTaskInfo(task_id="first", operator="ExternalTaskSensor", local_mode="inline-trigger")],
        xcoms={"first": {"return_value": {"rows": 2}}},
        notes=["note"],
        exception="pretty boom",
        exception_raw="Traceback\nboom",
    )

    artifacts = write_run_artifacts(result, tmp_path / "report", include_graph=False)

    assert set(artifacts) == {"result", "report", "exception", "graph", "tasks", "junit", "xcom"}
    payload = json.loads(artifacts["result"].read_text(encoding="utf-8"))
    assert payload["dag_id"] == "demo"
    assert payload["deferrables"][0]["task_id"] == "first"
    assert payload["graph_svg_path"] == "/tmp/graph.svg"
    assert payload["tasks"][0] == {
        "end_date": None,
        "duration_seconds": 2.5,
        "map_index": None,
        "mocked": False,
        "start_date": None,
        "state": "failed",
        "task_id": "first",
        "try_number": None,
    }
    assert "DAG: demo" in artifacts["report"].read_text(encoding="utf-8")
    assert "first -> second" not in artifacts["report"].read_text(encoding="utf-8")
    assert artifacts["exception"].read_text(encoding="utf-8") == "Traceback\nboom\n"
    assert artifacts["graph"].read_text(encoding="utf-8") == "demo\n  first -> second\n"
    assert artifacts["tasks"].read_text(encoding="utf-8").splitlines() == [
        "task_id,map_index,state,try_number,start_date,end_date,duration_seconds,mocked",
        "first,,failed,,,,2.5,false",
        "second,,skipped,,,,0,false",
    ]
    assert json.loads(artifacts["xcom"].read_text(encoding="utf-8")) == {"first": {"return_value": {"rows": 2}}}

    suite = ElementTree.parse(artifacts["junit"]).getroot()
    assert suite.tag == "testsuite"
    assert suite.attrib["name"] == "demo"
    assert suite.attrib["tests"] == "2"
    assert suite.attrib["failures"] == "1"
    assert suite.attrib["skipped"] == "1"
    cases = suite.findall("testcase")
    assert [case.attrib["name"] for case in cases] == ["first", "second"]
    assert cases[0].find("failure") is not None
    assert cases[1].find("skipped") is not None


def test_format_run_gantt_returns_none_without_timing_data() -> None:
    result = RunResult(
        dag_id="demo",
        tasks=[TaskRunInfo(task_id="a", state="success")],
    )
    assert format_run_gantt(result) is None


def test_format_run_gantt_renders_offsets_and_durations() -> None:
    result = RunResult(
        dag_id="demo",
        tasks=[
            TaskRunInfo(
                task_id="extract",
                state="success",
                start_date="2026-05-07T10:00:00+00:00",
                end_date="2026-05-07T10:00:01+00:00",
                duration_seconds=1.0,
            ),
            TaskRunInfo(
                task_id="transform",
                state="success",
                start_date="2026-05-07T10:00:01+00:00",
                end_date="2026-05-07T10:00:04+00:00",
                duration_seconds=3.0,
            ),
            TaskRunInfo(
                task_id="load",
                state="success",
                start_date="2026-05-07T10:00:04+00:00",
                end_date="2026-05-07T10:00:10+00:00",
                duration_seconds=6.0,
            ),
        ],
    )

    rendered = format_run_gantt(result, width=40)
    assert rendered is not None
    lines = rendered.splitlines()
    assert lines[0].startswith("Timing (total ")
    assert any(line.startswith("  extract") for line in lines)
    assert any(line.startswith("  transform") for line in lines)
    assert any(line.startswith("  load") for line in lines)
    assert "█" in rendered


def test_format_run_gantt_skips_tasks_with_partial_data() -> None:
    result = RunResult(
        dag_id="demo",
        tasks=[
            TaskRunInfo(
                task_id="ok",
                state="success",
                start_date="2026-05-07T10:00:00+00:00",
                duration_seconds=1.0,
            ),
            TaskRunInfo(task_id="missing_start", state="success", duration_seconds=1.0),
            TaskRunInfo(task_id="missing_duration", state="success", start_date="2026-05-07T10:00:01+00:00"),
        ],
    )
    rendered = format_run_gantt(result)
    assert rendered is not None
    assert "ok" in rendered
    assert "missing_start" not in rendered
    assert "missing_duration" not in rendered


def test_format_run_gantt_returns_none_when_total_span_is_zero() -> None:
    same_instant = "2026-05-07T10:00:00+00:00"
    result = RunResult(
        dag_id="demo",
        tasks=[
            TaskRunInfo(task_id="a", state="success", start_date=same_instant, duration_seconds=0.0),
        ],
    )
    assert format_run_gantt(result) is None


def test_format_run_report_includes_gantt_when_timing_available() -> None:
    result = RunResult(
        dag_id="demo",
        state="success",
        tasks=[
            TaskRunInfo(
                task_id="only",
                state="success",
                start_date="2026-05-07T10:00:00+00:00",
                end_date="2026-05-07T10:00:02+00:00",
                duration_seconds=2.0,
            ),
        ],
    )
    rendered = format_run_report(result)
    assert "Timing (total" in rendered
    assert "only" in rendered
