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


# --- ProblemLogPlugin: addHandler patch must always be restored -----------


def test_problem_log_plugin_restores_add_handler_after_run() -> None:
    import logging

    from airflow_local_debug.plugins import ProblemLogPlugin

    original = logging.Logger.addHandler
    plugin = ProblemLogPlugin()
    plugin.before_run(dag=None, context={})
    assert logging.Logger.addHandler is not original  # patched
    plugin.after_run(dag=None, context={}, result=None)
    assert logging.Logger.addHandler is original  # restored


def test_problem_log_plugin_restores_add_handler_even_when_post_restore_step_raises() -> None:
    import logging

    from airflow_local_debug.plugins import ProblemLogPlugin

    original = logging.Logger.addHandler
    plugin = ProblemLogPlugin()
    plugin.before_run(dag=None, context={})

    # Force a step AFTER the addHandler restore to raise; the addHandler
    # patch must already be undone before this failure surfaces.
    assert plugin._pretty_handler is not None

    def boom_close() -> None:
        raise RuntimeError("forced cleanup failure")

    plugin._pretty_handler.close = boom_close  # type: ignore[method-assign]

    try:
        plugin.after_run(dag=None, context={}, result=None)
    except RuntimeError:
        pass

    assert logging.Logger.addHandler is original
