from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from airflow_local_debug.plugins import AirflowDebugPlugin, DebugPluginManager
from airflow_local_debug.reporting.live_trace import live_task_trace


class _FakeTask:
    def __init__(self, task_id: str) -> None:
        self.task_id = task_id

    def pre_execute(self, context: dict[str, Any]) -> None:
        return None

    def execute(self, context: dict[str, Any]) -> str:
        return f"result-of-{self.task_id}"


class _FakeDag:
    def __init__(self, tasks: list[_FakeTask]) -> None:
        self.task_dict = {task.task_id: task for task in tasks}


class _RecordingPlugin(AirflowDebugPlugin):
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []

    def before_task(self, task: Any, context: Mapping[str, Any]) -> None:
        self.events.append(("before", task.task_id))

    def after_task(self, task: Any, context: Mapping[str, Any], result: Any) -> None:
        self.events.append(("after", task.task_id))


def test_live_task_trace_wraps_and_restores_methods() -> None:
    task = _FakeTask("a")
    dag = _FakeDag([task])
    original_execute = task.execute

    plugin = _RecordingPlugin()
    manager = DebugPluginManager([plugin])
    with live_task_trace(dag, plugin_manager=manager) as session:
        assert session is not None
        assert task.execute is not original_execute  # patched
        result = task.execute({})
        assert result == "result-of-a"

    # restored after context exit
    assert task.execute.__func__ is _FakeTask.execute  # type: ignore[attr-defined]
    assert plugin.events == [("before", "a"), ("after", "a")]


def test_live_task_trace_is_idempotent_for_repeated_wrap() -> None:
    task = _FakeTask("a")
    dag = _FakeDag([task])
    manager = DebugPluginManager([_RecordingPlugin()])

    with live_task_trace(dag, plugin_manager=manager) as session:
        wrapped_execute = task.execute
        session._wrap_task(task)  # second wrap must be a no-op
        assert task.execute is wrapped_execute

    # Original method must be restored even after the duplicate wrap call.
    assert task.execute.__func__ is _FakeTask.execute  # type: ignore[attr-defined]


def test_live_task_trace_rejects_conflicting_inputs() -> None:
    import pytest

    with pytest.raises(ValueError):
        live_task_trace(_FakeDag([]), plugins=[], plugin_manager=DebugPluginManager())
