from __future__ import annotations

from contextlib import AbstractContextManager
from types import MethodType
from typing import Any

from airflow_local_debug.plugins import (
    AirflowDebugPlugin,
    ConsoleTracePlugin,
    DebugPluginManager,
    task_label_scope,
)

_MISSING = object()
_CALLBACK_ATTRS = (
    "on_execute_callback",
    "on_success_callback",
    "on_failure_callback",
    "on_retry_callback",
)


class LiveTaskTraceSession(AbstractContextManager):
    def __init__(
        self,
        dag: Any,
        plugin_manager: DebugPluginManager,
        *,
        wrap_task_methods: bool = True,
    ) -> None:
        self.dag = dag
        self.plugin_manager = plugin_manager
        self.wrap_task_methods = wrap_task_methods
        self._originals: dict[int, dict[str, Any]] = {}
        self._started: set[tuple[str, str | None, int | None]] = set()
        self._results: dict[tuple[str, str | None, int | None], Any] = {}

    def __enter__(self) -> "LiveTaskTraceSession":
        if not self.wrap_task_methods:
            return self
        for task in list(getattr(self.dag, "task_dict", {}).values()):
            self._wrap_task(task)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self.wrap_task_methods:
            self._started.clear()
            self._results.clear()
            self._originals.clear()
            return None
        for task in list(getattr(self.dag, "task_dict", {}).values()):
            original = self._originals.get(id(task))
            if not original:
                continue
            for attr_name in ("pre_execute", "execute", "post_execute", "render_template_fields", *_CALLBACK_ATTRS):
                original_value = original[attr_name]
                if original_value is _MISSING:
                    task.__dict__.pop(attr_name, None)
                else:
                    setattr(task, attr_name, original_value)
        self._started.clear()
        self._results.clear()
        self._originals.clear()
        return None

    def _task_run_key(self, task: Any, context: dict[str, Any]) -> tuple[str, str | None, int | None]:
        ti = context.get("ti") or context.get("task_instance")
        map_index = getattr(ti, "map_index", None)
        if map_index == -1:
            map_index = None
        run_id = getattr(ti, "run_id", None) or context.get("run_id")
        return (getattr(task, "task_id", "<unknown>"), run_id, map_index)

    def _ensure_before_task(self, task: Any, context: dict[str, Any]) -> tuple[str, str | None, int | None]:
        key = self._task_run_key(task, context)
        if key not in self._started:
            self.plugin_manager.before_task(task, context)
            self._started.add(key)
        return key

    def _finish_task(self, key: tuple[str, str | None, int | None]) -> None:
        self._started.discard(key)
        self._results.pop(key, None)

    def begin_task(self, task: Any, context: dict[str, Any]) -> None:
        self._ensure_before_task(task, context)

    def complete_task(self, task: Any, context: dict[str, Any], result: Any) -> None:
        key = self._task_run_key(task, context)
        if key not in self._started:
            return
        self.plugin_manager.after_task(task, context, result)
        self._finish_task(key)

    def fail_task(self, task: Any, context: dict[str, Any], error: BaseException) -> None:
        key = self._task_run_key(task, context)
        if key not in self._started:
            return
        self.plugin_manager.on_task_error(task, context, error)
        self._finish_task(key)

    def _wrap_task(self, task: Any) -> None:
        original_pre = task.__dict__.get("pre_execute", _MISSING)
        original_execute = task.__dict__.get("execute", _MISSING)
        original_post = task.__dict__.get("post_execute", _MISSING)
        original_render = task.__dict__.get("render_template_fields", _MISSING)
        bound_pre = getattr(task, "pre_execute", None)
        bound_execute = getattr(task, "execute", None)
        bound_post = getattr(task, "post_execute", None)
        bound_render = getattr(task, "render_template_fields", None)

        if not callable(bound_pre) or not callable(bound_execute):
            return

        self._originals[id(task)] = {
            "pre_execute": original_pre,
            "execute": original_execute,
            "post_execute": original_post,
            "render_template_fields": original_render,
        }
        for callback_attr in _CALLBACK_ATTRS:
            self._originals[id(task)][callback_attr] = task.__dict__.get(callback_attr, _MISSING)

        def traced_pre_execute(bound_self: Any, *args: Any, **kwargs: Any) -> Any:
            context = _extract_context(args, kwargs)
            key = self._ensure_before_task(bound_self, context)
            try:
                return bound_pre(*args, **kwargs)
            except BaseException as exc:
                self.plugin_manager.on_task_error(bound_self, context, exc)
                self._finish_task(key)
                raise

        def traced_execute(bound_self: Any, *args: Any, **kwargs: Any) -> Any:
            context = _extract_context(args, kwargs)
            key = self._ensure_before_task(bound_self, context)
            try:
                result = bound_execute(*args, **kwargs)
            except BaseException as exc:
                self.plugin_manager.on_task_error(bound_self, context, exc)
                self._finish_task(key)
                raise
            if callable(bound_post):
                self._results[key] = result
            else:
                self.plugin_manager.after_task(bound_self, context, result)
                self._finish_task(key)
            return result

        def traced_post_execute(bound_self: Any, *args: Any, **kwargs: Any) -> Any:
            context = _extract_context(args, kwargs)
            key = self._ensure_before_task(bound_self, context)
            try:
                post_result = bound_post(*args, **kwargs) if callable(bound_post) else None
            except BaseException as exc:
                self.plugin_manager.on_task_error(bound_self, context, exc)
                self._finish_task(key)
                raise
            result = _extract_post_result(args, kwargs, fallback=self._results.get(key))
            self.plugin_manager.after_task(bound_self, context, result)
            self._finish_task(key)
            return post_result

        def traced_render_template_fields(bound_self: Any, *args: Any, **kwargs: Any) -> Any:
            result = bound_render(*args, **kwargs) if callable(bound_render) else None
            context = _extract_context(args, kwargs)
            ti = context.get("ti") or context.get("task_instance")
            mapped_clone = getattr(ti, "task", None)
            if mapped_clone is not None and mapped_clone is not bound_self:
                self._wrap_task(mapped_clone)
            return result

        task.pre_execute = MethodType(traced_pre_execute, task)
        task.execute = MethodType(traced_execute, task)
        if callable(bound_post):
            task.post_execute = MethodType(traced_post_execute, task)
        if callable(bound_render):
            task.render_template_fields = MethodType(traced_render_template_fields, task)
        for callback_attr in _CALLBACK_ATTRS:
            original_callback = getattr(task, callback_attr, None)
            wrapped_callback = self._wrap_callback(task, callback_attr, original_callback)
            if wrapped_callback is not None:
                setattr(task, callback_attr, wrapped_callback)

    def _wrap_callback(self, task: Any, callback_name: str, callback_value: Any) -> Any:
        callbacks = _normalize_callbacks(callback_value)
        if not callbacks:
            return None

        def traced_callback(*args: Any, **kwargs: Any) -> Any:
            context = _extract_context(args, kwargs)
            with task_label_scope(task, context):
                self.plugin_manager.before_task_callback(task, context, callback_name)
                try:
                    result = None
                    for callback in callbacks:
                        result = callback(*args, **kwargs)
                except BaseException as exc:
                    self.plugin_manager.on_task_callback_error(task, context, callback_name, exc)
                    raise
                self.plugin_manager.after_task_callback(task, context, callback_name, result)
                return result

        return traced_callback


def _extract_context(args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    if args and isinstance(args[0], dict):
        return args[0]
    context = kwargs.get("context")
    return context if isinstance(context, dict) else {}


def _extract_post_result(args: tuple[Any, ...], kwargs: dict[str, Any], *, fallback: Any) -> Any:
    if "result" in kwargs:
        return kwargs["result"]
    if len(args) >= 2:
        return args[1]
    return fallback


def _normalize_callbacks(callback_value: Any) -> list[Any]:
    if callback_value is None:
        return []
    if isinstance(callback_value, (list, tuple)):
        return [callback for callback in callback_value if callable(callback)]
    if callable(callback_value):
        return [callback_value]
    return []


def live_task_trace(
    dag: Any,
    *,
    plugins: list[AirflowDebugPlugin] | None = None,
    plugin_manager: DebugPluginManager | None = None,
    wrap_task_methods: bool = True,
) -> LiveTaskTraceSession:
    manager = plugin_manager or DebugPluginManager(plugins if plugins is not None else [ConsoleTracePlugin()])
    return LiveTaskTraceSession(dag, manager, wrap_task_methods=wrap_task_methods)
