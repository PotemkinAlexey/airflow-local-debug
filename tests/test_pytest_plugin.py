from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from airflow_local_debug import pytest_plugin
from airflow_local_debug.models import RunResult
from airflow_local_debug.pytest_plugin import AirflowLocalRunner


def test_pytest_plugin_registers_fixture() -> None:
    assert hasattr(pytest_plugin, "airflow_local_runner")


def test_run_dag_with_dag_object_dispatches_to_run_full_dag(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run_full_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured["dag"] = dag
        captured["kwargs"] = kwargs
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(pytest_plugin, "run_full_dag", fake_run_full_dag)

    class _Dag:
        dag_id = "demo"

    runner = AirflowLocalRunner()
    result = runner.run_dag(_Dag(), config_path="/tmp/cfg.py", task_ids=["a", "b"])

    assert isinstance(captured["dag"], _Dag)
    assert captured["kwargs"]["config_path"] == "/tmp/cfg.py"
    assert list(captured["kwargs"]["task_ids"]) == ["a", "b"]
    assert result.ok


def test_run_dag_with_artifacts_writes_outputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, Any] = {}

    def fake_run_full_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured["dag"] = dag
        captured["kwargs"] = kwargs
        return RunResult(
            dag_id="demo",
            state="success",
            graph_ascii="demo\n  task",
            graph_svg_path=str(kwargs["graph_svg_path"]),
            xcoms={"task": {"return_value": {"rows": 3}}},
        )

    monkeypatch.setattr(pytest_plugin, "run_full_dag", fake_run_full_dag)

    class _Dag:
        dag_id = "demo"

    runner = AirflowLocalRunner()
    result = runner.run_dag(
        _Dag(),
        collect_xcoms=False,
        report_dir=tmp_path / "artifacts",
        xcom_json_path=tmp_path / "xcom.json",
        include_graph_in_report=True,
    )

    assert result.ok
    assert captured["kwargs"]["collect_xcoms"] is True
    assert captured["kwargs"]["graph_svg_path"] == tmp_path / "artifacts" / "graph.svg"
    assert (tmp_path / "xcom.json").exists()
    assert (tmp_path / "artifacts" / "result.json").exists()
    assert (tmp_path / "artifacts" / "report.md").exists()
    assert "Graph SVG:" in (tmp_path / "artifacts" / "report.md").read_text(encoding="utf-8")


def test_run_dag_with_string_path_dispatches_to_file_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_run_full_dag_from_file(dag_file: str, **kwargs: Any) -> RunResult:
        captured["dag_file"] = dag_file
        captured["kwargs"] = kwargs
        return RunResult(dag_id=kwargs.get("dag_id") or "<unknown>", state="success")

    monkeypatch.setattr(pytest_plugin, "run_full_dag_from_file", fake_run_full_dag_from_file)

    runner = AirflowLocalRunner()
    result = runner.run_dag("/abs/path/to/my_dag.py", dag_id="my_dag", start_task_ids=["mid"])

    assert captured["dag_file"] == "/abs/path/to/my_dag.py"
    assert captured["kwargs"]["dag_id"] == "my_dag"
    assert list(captured["kwargs"]["start_task_ids"]) == ["mid"]
    assert result.ok


def test_run_dag_with_path_object_dispatches_to_file_runner(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: dict[str, Any] = {}

    def fake_run_full_dag_from_file(dag_file: str, **kwargs: Any) -> RunResult:
        captured["dag_file"] = dag_file
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(pytest_plugin, "run_full_dag_from_file", fake_run_full_dag_from_file)

    dag_file = tmp_path / "demo_dag.py"
    dag_file.write_text("# placeholder")

    runner = AirflowLocalRunner()
    runner.run_dag(dag_file)

    assert captured["dag_file"] == str(dag_file)


def test_run_dag_rejects_dag_id_with_dag_object() -> None:
    class _Dag:
        dag_id = "demo"

    runner = AirflowLocalRunner()
    with pytest.raises(TypeError, match="dag_id is only meaningful"):
        runner.run_dag(_Dag(), dag_id="demo")


def test_airflow_local_runner_fixture_is_usable(airflow_local_runner: AirflowLocalRunner) -> None:
    assert isinstance(airflow_local_runner, AirflowLocalRunner)
