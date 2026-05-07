from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from airflow_local_debug.plugins import AirflowDebugPlugin, DebugPluginManager


class _RecordingPlugin(AirflowDebugPlugin):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def before_task(self, task: Any, context: Mapping[str, Any]) -> None:
        self.calls.append("before_task")

    def after_task(self, task: Any, context: Mapping[str, Any], result: Any) -> None:
        self.calls.append("after_task")


class _ExplodingPlugin(AirflowDebugPlugin):
    def before_task(self, task: Any, context: Mapping[str, Any]) -> None:
        raise RuntimeError("boom")


def test_plugin_manager_dispatches_in_order() -> None:
    plugin = _RecordingPlugin()
    manager = DebugPluginManager([plugin])

    manager.before_task(task=object(), context={})
    manager.after_task(task=object(), context={}, result=None)

    assert plugin.calls == ["before_task", "after_task"]


def test_plugin_manager_isolates_plugin_errors() -> None:
    notes: list[str] = []
    good = _RecordingPlugin()
    bad = _ExplodingPlugin()
    manager = DebugPluginManager([bad, good], notes=notes)

    manager.before_task(task=object(), context={})

    assert good.calls == ["before_task"]
    assert any("ExplodingPlugin.before_task" in note for note in notes)


def test_plugin_manager_drops_none_plugins() -> None:
    manager = DebugPluginManager([None, _RecordingPlugin()])  # type: ignore[list-item]
    assert len(manager.plugins) == 1
