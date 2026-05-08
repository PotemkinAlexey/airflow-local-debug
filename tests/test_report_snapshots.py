from __future__ import annotations

from pathlib import Path

from airflow_local_debug.models import RunResult, TaskMockInfo, TaskRunInfo
from airflow_local_debug.reporting.report import write_run_artifacts

SNAPSHOT_DIR = Path(__file__).parent / "snapshots" / "report_artifacts"


def _snapshot_result() -> RunResult:
    return RunResult(
        dag_id="orders_success",
        run_id="manual__2026-05-07T00:00:00+00:00",
        state="success",
        logical_date="2026-05-07T00:00:00+00:00",
        backend="dag.test.strict",
        airflow_version="3.2.1",
        config_path="/repo/examples/airflow_defaults.py",
        graph_ascii="orders_success\n  extract_orders -> transform_orders\n  transform_orders -> load_to_warehouse",
        graph_svg_path="/repo/.tmp/orders-success-report/graph.svg",
        selected_tasks=["extract_orders", "transform_orders", "load_to_warehouse"],
        tasks=[
            TaskRunInfo(task_id="extract_orders", state="success", try_number=1, duration_seconds=0.25),
            TaskRunInfo(task_id="transform_orders", state="success", try_number=1, duration_seconds=0.5),
            TaskRunInfo(task_id="load_to_warehouse", state="success", try_number=1, duration_seconds=0.75, mocked=True),
        ],
        mocks=[
            TaskMockInfo(
                task_id="load_to_warehouse",
                mode="success",
                rule_name="local warehouse load",
                xcom_keys=["return_value"],
            )
        ],
        xcoms={"load_to_warehouse": {"return_value": {"rows_loaded": 2, "table": "analytics.orders"}}},
        notes=["Loaded local config from /repo/examples/airflow_defaults.py"],
    )


def _normalized_text(path: Path) -> str:
    return path.read_text(encoding="utf-8").replace("\r\n", "\n").rstrip("\n")


def test_write_run_artifacts_matches_golden_snapshots(tmp_path: Path) -> None:
    artifacts = write_run_artifacts(_snapshot_result(), tmp_path, include_graph=True)

    assert set(artifacts) == {"graph", "junit", "report", "result", "tasks", "xcom"}
    for snapshot_path in sorted(SNAPSHOT_DIR.iterdir()):
        actual_path = tmp_path / snapshot_path.name
        assert actual_path.exists(), snapshot_path.name
        assert _normalized_text(actual_path) == _normalized_text(snapshot_path)
