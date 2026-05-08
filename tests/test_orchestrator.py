from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pytest

from airflow_local_debug.execution import orchestrator
from airflow_local_debug.execution.mocks import TaskMockRule
from airflow_local_debug.models import LocalConfig, RunResult


@dataclass
class FakeTask:
    task_id: str
    retries: int = 3
    retry_delay: object = field(default_factory=object)
    retry_exponential_backoff: bool = True
    max_retry_delay: object = field(default_factory=object)


class FakeDagWithTest:
    dag_id = "demo"

    def __init__(self) -> None:
        self.task_dict = {"task": FakeTask("task")}
        self.test_kwargs: dict[str, Any] | None = None

    def test(self, **kwargs: Any) -> object:
        self.test_kwargs = kwargs
        return object()


class FakeDagWithRun:
    dag_id = "legacy"

    def __init__(self) -> None:
        self.task_dict = {"task": FakeTask("task")}
        self.clear_kwargs: dict[str, Any] | None = None
        self.run_kwargs: dict[str, Any] | None = None

    def clear(self, **kwargs: Any) -> None:
        self.clear_kwargs = kwargs

    def run(self, **kwargs: Any) -> None:
        self.run_kwargs = kwargs


class FakePluginManager:
    def __init__(self) -> None:
        self.before_context: dict[str, Any] | None = None
        self.after_result: RunResult | None = None

    def before_run(self, dag: Any, context: dict[str, Any]) -> None:
        self.before_context = context

    def after_run(self, dag: Any, context: dict[str, Any], result: RunResult) -> None:
        self.after_result = result


def _install_fast_runtime(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    captured: dict[str, Any] = {"plugin_manager": FakePluginManager()}

    @contextmanager
    def fake_bootstrap_airflow_env(**kwargs: Any) -> Any:
        captured["bootstrap_kwargs"] = kwargs
        yield

    @contextmanager
    def fake_local_task_mocks(dag: Any, rules: list[TaskMockRule], *, notes: list[str]) -> Any:
        captured["mock_rules"] = rules
        yield "mock-registry"

    @contextmanager
    def fake_live_task_trace(dag: Any, **kwargs: Any) -> Any:
        captured["trace_kwargs"] = kwargs
        yield "trace-session"

    def fake_result_from_dagrun(dag: Any, dagrun: Any, **kwargs: Any) -> RunResult:
        captured["result_kwargs"] = kwargs
        return RunResult(
            dag_id=getattr(dag, "dag_id", "demo"),
            state="success",
            backend=kwargs.get("backend"),
            config_path=kwargs.get("config_path"),
            notes=kwargs.get("notes", []),
            graph_ascii=kwargs.get("graph_ascii"),
            exception=kwargs.get("exception"),
            exception_raw=kwargs.get("exception_raw"),
            selected_tasks=kwargs.get("selected_tasks") or [],
            deferrables=kwargs.get("deferrables") or [],
        )

    monkeypatch.setattr(orchestrator, "bootstrap_airflow_env", fake_bootstrap_airflow_env)
    monkeypatch.setattr(orchestrator, "local_task_mocks", fake_local_task_mocks)
    monkeypatch.setattr(orchestrator, "live_task_trace", fake_live_task_trace)
    monkeypatch.setattr(orchestrator, "print_run_preamble", lambda *args, **kwargs: None)
    monkeypatch.setattr(orchestrator, "bootstrap_pools", lambda *args, **kwargs: None)
    monkeypatch.setattr(orchestrator, "result_from_dagrun", fake_result_from_dagrun)
    monkeypatch.setattr(orchestrator, "build_plugin_manager", lambda **kwargs: captured["plugin_manager"])
    monkeypatch.setattr(orchestrator, "attach_graph_svg", lambda *args, **kwargs: None)
    monkeypatch.setattr(orchestrator, "detect_deferrable_tasks", lambda *args, **kwargs: [])
    monkeypatch.setattr(orchestrator, "format_deferrable_note", lambda deferrables: None)
    monkeypatch.setattr(orchestrator, "format_dag_graph", lambda dag: f"graph:{dag.dag_id}")
    monkeypatch.setattr(orchestrator, "get_airflow_version", lambda: "test")
    return captured


def _execute(dag: Any, **kwargs: Any) -> RunResult:
    return orchestrator.execute_full_dag(
        dag,
        local_config=LocalConfig(),
        config_path="/tmp/config.py",
        logical_date=kwargs.pop("logical_date", "2026-01-02T00:00:00+00:00"),
        conf=kwargs.pop("conf", {"dataset": "daily"}),
        extra_env=kwargs.pop("extra_env", {"FEATURE_FLAG": "local"}),
        trace=kwargs.pop("trace", True),
        fail_fast=kwargs.pop("fail_fast", True),
        plugins=kwargs.pop("plugins", None),
        task_mocks=kwargs.pop("task_mocks", None),
        collect_xcoms=kwargs.pop("collect_xcoms", True),
        notes=kwargs.pop("notes", []),
        **kwargs,
    )


def test_execute_full_dag_uses_strict_dag_test_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _install_fast_runtime(monkeypatch)
    strict_calls: list[dict[str, Any]] = []

    def fake_strict_dag_test(dag: Any, **kwargs: Any) -> object:
        strict_calls.append(kwargs)
        return object()

    monkeypatch.setattr(orchestrator, "strict_dag_test", fake_strict_dag_test)
    dag = FakeDagWithTest()

    result = _execute(dag, fail_fast=True)

    assert result.backend == "dag.test.strict"
    assert strict_calls == [
        {
            "execution_date": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "run_conf": {"dataset": "daily"},
            "trace_session": "trace-session",
        }
    ]
    assert captured["trace_kwargs"]["wrap_task_methods"] is False
    assert captured["result_kwargs"]["task_mock_registry"] == "mock-registry"
    assert "Using strict local dag.test loop" in "\n".join(result.notes)


def test_execute_full_dag_uses_non_strict_dag_test_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _install_fast_runtime(monkeypatch)
    dag = FakeDagWithTest()

    result = _execute(dag, fail_fast=False)

    assert result.backend == "dag.test"
    assert dag.test_kwargs == {
        "logical_date": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "conf": {"dataset": "daily"},
    }
    assert captured["trace_kwargs"]["wrap_task_methods"] is True
    assert "strict local dag.test loop" not in "\n".join(result.notes)


def test_execute_full_dag_uses_legacy_dag_run_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _install_fast_runtime(monkeypatch)
    dag = FakeDagWithRun()
    last_dagrun = object()
    monkeypatch.setattr(orchestrator, "best_effort_last_dagrun", lambda dag: last_dagrun)
    monkeypatch.setattr(
        orchestrator,
        "build_legacy_dag_run_kwargs",
        lambda dag, logical_date, conf: {"start_date": logical_date, "end_date": logical_date},
    )

    result = _execute(dag, fail_fast=True)

    assert result.backend == "dag.run"
    assert dag.clear_kwargs == {
        "start_date": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 2, tzinfo=timezone.utc),
    }
    assert dag.run_kwargs == {
        "start_date": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "end_date": datetime(2026, 1, 2, tzinfo=timezone.utc),
    }
    assert captured["result_kwargs"]["backend"] == "dag.run"
    assert "Falling back to legacy dag.run()" in "\n".join(result.notes)


def test_execute_full_dag_returns_unsupported_result(monkeypatch: pytest.MonkeyPatch) -> None:
    _install_fast_runtime(monkeypatch)

    class UnsupportedDag:
        dag_id = "unsupported"
        task_dict: dict[str, FakeTask] = {}

    result = _execute(UnsupportedDag())

    assert result.backend == "unsupported"
    assert result.exception == "This Airflow runtime exposes neither dag.test() nor dag.run()."


def test_execute_full_dag_plumbs_partial_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = _install_fast_runtime(monkeypatch)
    base_dag = FakeDagWithTest()
    partial_dag = FakeDagWithTest()
    partial_dag.dag_id = "partial"
    rule = TaskMockRule(task_id="upstream", xcom={"return_value": 1})

    monkeypatch.setattr(orchestrator, "resolve_partial_task_ids", lambda *args, **kwargs: ["selected"])
    monkeypatch.setattr(orchestrator, "build_partial_selection_note", lambda dag, selected: "Partial run: selected")
    monkeypatch.setattr(orchestrator, "detect_external_upstreams", lambda dag, selected: {"selected": {"upstream"}})
    monkeypatch.setattr(orchestrator, "partial_dag_for_selected_tasks", lambda dag, selected: partial_dag)
    monkeypatch.setattr(orchestrator, "strict_dag_test", lambda *args, **kwargs: object())

    result = _execute(base_dag, task_mocks=[rule])

    assert result.dag_id == "partial"
    assert result.selected_tasks == ["selected"]
    assert captured["mock_rules"] == [rule]
    assert "Partial run: selected" in result.notes
    assert "Partial run upstream XCom mocks active for: upstream." in result.notes
