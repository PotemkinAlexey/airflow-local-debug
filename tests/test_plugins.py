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


def test_problem_log_plugin_full_cleanup_runs_even_when_pretty_handler_close_raises() -> None:
    """P2 regression: every cleanup step must complete even if one raises.

    Previously, a failure inside `_pretty_handler.close()` would skip the
    suppress-filter removal, the logger-list reset, and the
    `_HAS_PRIMARY_TASK_ERROR` token reset. All four must complete now.
    """
    import logging

    from airflow_local_debug.plugins import _HAS_PRIMARY_TASK_ERROR, ProblemLogPlugin

    original = logging.Logger.addHandler
    plugin = ProblemLogPlugin()
    plugin.before_run(dag=None, context={})

    assert plugin._pretty_handler is not None
    assert plugin._suppress_filter is not None
    assert plugin._attached_loggers  # something was attached
    assert plugin._failure_token is not None

    # Snapshot the handlers that got the suppress filter so we can verify
    # they are clean afterwards.
    filtered_pairs = list(plugin._filtered_handlers)

    # Force the pretty handler's close() to raise.
    def boom_close() -> None:
        raise RuntimeError("forced close failure")

    plugin._pretty_handler.close = boom_close  # type: ignore[method-assign]

    plugin.after_run(dag=None, context={}, result=None)  # must not raise

    # 1. addHandler patch undone.
    assert logging.Logger.addHandler is original
    # 2. pretty handler reference cleared.
    assert plugin._pretty_handler is None
    # 3. suppress filters removed from every handler we touched.
    for handler, suppress_filter in filtered_pairs:
        assert suppress_filter not in getattr(handler, "filters", ())
    # 4. internal lists reset.
    assert plugin._attached_loggers == []
    assert plugin._filtered_handlers == []
    assert plugin._suppress_filter is None
    # 5. context-var token reset (i.e. no longer signalling primary error).
    assert plugin._failure_token is None
    assert _HAS_PRIMARY_TASK_ERROR.get() is False


# --- helpers --------------------------------------------------------------


class _FakeTask:
    def __init__(self, task_id: str, task_type: str = "PythonOperator", **attrs: Any) -> None:
        self.task_id = task_id
        self.task_type = task_type
        for name, value in attrs.items():
            setattr(self, name, value)


class _FakeTI:
    def __init__(
        self,
        task_id: str,
        *,
        run_id: str | None = "run-1",
        map_index: int | None = -1,
    ) -> None:
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index


# --- _display_task_id / _operator_label / _trace_key ----------------------


def test_display_task_id_uses_map_index_when_present() -> None:
    from airflow_local_debug.plugins import _display_task_id

    task = _FakeTask("transform")
    ti = _FakeTI("transform", map_index=2)
    assert _display_task_id(task, {"ti": ti}) == "transform[2]"


def test_display_task_id_skips_map_index_for_unmapped_task() -> None:
    from airflow_local_debug.plugins import _display_task_id

    task = _FakeTask("transform")
    ti = _FakeTI("transform", map_index=-1)
    assert _display_task_id(task, {"ti": ti}) == "transform"


def test_operator_label_aliases_python_operator_to_python_callable() -> None:
    from airflow_local_debug.plugins import _operator_label

    assert _operator_label(_FakeTask("t", task_type="PythonOperator")) == "PythonCallable"
    assert _operator_label(_FakeTask("t", task_type="DecoratedOperator")) == "PythonCallable"
    assert _operator_label(_FakeTask("t", task_type="BashOperator")) == "BashOperator"


def test_collect_resolved_inputs_prefers_op_args_kwargs() -> None:
    from airflow_local_debug.plugins import _collect_resolved_inputs

    task = _FakeTask("t", op_args=(1, 2), op_kwargs={"x": 3})
    payload = _collect_resolved_inputs(task)
    assert payload == {"op_args": (1, 2), "op_kwargs": {"x": 3}}


def test_collect_resolved_inputs_falls_back_to_template_fields() -> None:
    from airflow_local_debug.plugins import _collect_resolved_inputs

    task = _FakeTask("t", template_fields=("sql", "bash_command"), sql="SELECT 1", bash_command="")
    payload = _collect_resolved_inputs(task)
    assert payload == {"sql": "SELECT 1"}


def test_trace_key_combines_task_run_and_map_index() -> None:
    from airflow_local_debug.plugins import _trace_key

    task = _FakeTask("transform")
    ti = _FakeTI("transform", run_id="run-x", map_index=5)
    assert _trace_key(task, {"ti": ti}) == ("transform", "run-x", 5)


# --- ConsoleTracePlugin ---------------------------------------------------


def test_console_trace_plugin_runs_full_lifecycle_without_error() -> None:
    """Smoke-test: plugin can begin, complete, and re-handle a task without raising."""
    from airflow_local_debug.plugins import ConsoleTracePlugin

    plugin = ConsoleTracePlugin()
    task = _FakeTask("extract")
    ti = _FakeTI("extract")
    context = {"ti": ti}

    plugin.before_task(task, context)
    plugin.after_task(task, context, result={"rows": 1})

    # Idempotent: a second after_task without a matching before_task should not raise.
    plugin.after_task(task, context, result=None)


def test_console_trace_plugin_handles_task_error() -> None:
    from airflow_local_debug.plugins import ConsoleTracePlugin

    plugin = ConsoleTracePlugin()
    task = _FakeTask("extract")
    ti = _FakeTI("extract")
    context = {"ti": ti}

    plugin.before_task(task, context)
    err = RuntimeError("boom")
    plugin.on_task_error(task, context, err)

    # Subsequent after_task should not error even though the tracer was already exited.
    plugin.after_task(task, context, result=None)


def test_console_trace_plugin_skips_repeat_before_task_for_same_key() -> None:
    from airflow_local_debug.plugins import ConsoleTracePlugin

    plugin = ConsoleTracePlugin()
    task = _FakeTask("t")
    ti = _FakeTI("t")
    context = {"ti": ti}

    plugin.before_task(task, context)
    # Same key — should be a no-op (otherwise the second tracer would leak).
    plugin.before_task(task, context)
    plugin.after_task(task, context, result=None)


# --- TaskContextPlugin ----------------------------------------------------


def test_task_context_plugin_publishes_current_task_label_in_contextvar() -> None:
    from airflow_local_debug.plugins import _CURRENT_TASK_LABEL, TaskContextPlugin

    plugin = TaskContextPlugin()
    task = _FakeTask("transform")
    ti = _FakeTI("transform", map_index=0)
    context = {"ti": ti}

    assert _CURRENT_TASK_LABEL.get() is None
    plugin.before_task(task, context)
    assert _CURRENT_TASK_LABEL.get() == "transform[0]"
    plugin.after_task(task, context, result=None)
    assert _CURRENT_TASK_LABEL.get() is None


def test_task_context_plugin_resets_label_on_task_error() -> None:
    from airflow_local_debug.plugins import _CURRENT_TASK_LABEL, TaskContextPlugin

    plugin = TaskContextPlugin()
    task = _FakeTask("oops")
    ti = _FakeTI("oops")
    context = {"ti": ti}

    plugin.before_task(task, context)
    assert _CURRENT_TASK_LABEL.get() == "oops"
    plugin.on_task_error(task, context, RuntimeError("boom"))
    assert _CURRENT_TASK_LABEL.get() is None


# --- DebugPluginManager full hook surface ---------------------------------


class _FullPlugin(AirflowDebugPlugin):
    def __init__(self) -> None:
        self.events: list[str] = []

    def before_run(self, dag: Any, context: Mapping[str, Any]) -> None:
        self.events.append("before_run")

    def after_run(self, dag: Any, context: Mapping[str, Any], result: Any) -> None:
        self.events.append("after_run")

    def before_task_callback(self, task: Any, context: Mapping[str, Any], callback_name: str) -> None:
        self.events.append(f"before_callback:{callback_name}")

    def after_task_callback(
        self,
        task: Any,
        context: Mapping[str, Any],
        callback_name: str,
        result: Any,
    ) -> None:
        self.events.append(f"after_callback:{callback_name}")

    def on_task_error(self, task: Any, context: Mapping[str, Any], error: BaseException) -> None:
        self.events.append("on_task_error")

    def on_task_callback_error(
        self,
        task: Any,
        context: Mapping[str, Any],
        callback_name: str,
        error: BaseException,
    ) -> None:
        self.events.append(f"on_callback_error:{callback_name}")


def test_plugin_manager_dispatches_run_and_callback_hooks() -> None:
    plugin = _FullPlugin()
    manager = DebugPluginManager([plugin])

    manager.before_run(dag=object(), context={})
    manager.before_task_callback(task=object(), context={}, callback_name="on_success")
    manager.after_task_callback(task=object(), context={}, callback_name="on_success", result=None)
    manager.on_task_error(task=object(), context={}, error=RuntimeError("x"))
    manager.on_task_callback_error(task=object(), context={}, callback_name="on_failure", error=RuntimeError("y"))
    manager.after_run(dag=object(), context={}, result=None)

    assert plugin.events == [
        "before_run",
        "before_callback:on_success",
        "after_callback:on_success",
        "on_task_error",
        "on_callback_error:on_failure",
        "after_run",
    ]


def test_plugin_manager_skips_plugins_without_the_hook() -> None:
    """Plugins that don't implement a hook should be silently skipped (default impl is empty)."""
    plugin = _RecordingPlugin()  # only implements before_task / after_task
    manager = DebugPluginManager([plugin])

    # Should not raise.
    manager.before_run(dag=object(), context={})
    manager.after_run(dag=object(), context={}, result=None)
    assert plugin.calls == []
