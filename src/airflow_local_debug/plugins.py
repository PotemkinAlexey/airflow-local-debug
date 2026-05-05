"""
Per-run plugin system for local DAG execution.

These plugins are intentionally scoped to a single local run. They are not
stored on DAG objects and do not participate in Airflow serialization.

The most important hook points are:
- `before_run`
- `before_task`
- `before_task_callback`
- `after_task`
- `after_task_callback`
- `on_task_error`
- `on_task_callback_error`
- `after_run`

Custom plugins should subclass `AirflowDebugPlugin` and be passed into
`run_full_dag(..., plugins=[...])` or `debug_dag(..., plugins=[...])`.
"""

from __future__ import annotations

import contextvars
import logging
import sys
from contextlib import contextmanager
from typing import Any, Iterable, Mapping

from airflow_local_debug.traceback_utils import (
    StepTracer,
    StepTracerOptions,
    format_pretty_log_record,
    shrink,
)

log = logging.getLogger(__name__)
_CURRENT_TASK_LABEL: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "airflow_debug_current_task_label",
    default=None,
)
_HAS_PRIMARY_TASK_ERROR: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "airflow_debug_has_primary_task_error",
    default=False,
)


def _ti_from_context(context: Mapping[str, Any]) -> Any | None:
    return context.get("ti") or context.get("task_instance")


def _map_index_from_context(context: Mapping[str, Any]) -> int | None:
    ti = _ti_from_context(context)
    map_index = getattr(ti, "map_index", None)
    return None if map_index in (None, -1) else map_index


def _run_id_from_context(context: Mapping[str, Any]) -> str | None:
    ti = _ti_from_context(context)
    return getattr(ti, "run_id", None) or context.get("run_id")


def _display_task_id(task: Any, context: Mapping[str, Any]) -> str:
    map_index = _map_index_from_context(context)
    task_id = getattr(task, "task_id", "<unknown>")
    if map_index is None:
        return task_id
    return f"{task_id}[{map_index}]"


def _operator_label(task: Any) -> str:
    name = getattr(task, "task_type", None) or task.__class__.__name__
    if name in {"PythonOperator", "_PythonDecoratedOperator", "DecoratedOperator"}:
        return "PythonCallable"
    return str(name)


def _collect_resolved_inputs(task: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {}

    op_args = getattr(task, "op_args", None)
    if op_args not in (None, (), []):
        payload["op_args"] = op_args

    op_kwargs = getattr(task, "op_kwargs", None)
    if op_kwargs not in (None, {}, []):
        payload["op_kwargs"] = op_kwargs

    if payload:
        return payload

    template_fields = list(getattr(task, "template_fields", ()) or ())
    for field_name in template_fields[:6]:
        try:
            value = getattr(task, field_name)
        except Exception:
            continue
        if value in (None, "", (), [], {}):
            continue
        payload[field_name] = value

    return payload


def _trace_key(task: Any, context: Mapping[str, Any]) -> tuple[str, str | None, int | None]:
    return (
        getattr(task, "task_id", "<unknown>"),
        _run_id_from_context(context),
        _map_index_from_context(context),
    )


@contextmanager
def task_label_scope(task: Any, context: Mapping[str, Any]):
    token = _CURRENT_TASK_LABEL.set(_display_task_id(task, context))
    try:
        yield
    finally:
        _CURRENT_TASK_LABEL.reset(token)


class AirflowDebugPlugin:
    """
    Lifecycle hooks for a single local DAG run.

    Example:

        class MyPlugin(AirflowDebugPlugin):
            def before_task(self, task, context):
                print(f"START {task.task_id}")
    """

    def before_run(self, dag: Any, context: Mapping[str, Any]) -> None:
        pass

    def after_run(self, dag: Any, context: Mapping[str, Any], result: Any) -> None:
        pass

    def before_task(self, task: Any, context: Mapping[str, Any]) -> None:
        pass

    def after_task(self, task: Any, context: Mapping[str, Any], result: Any) -> None:
        pass

    def before_task_callback(self, task: Any, context: Mapping[str, Any], callback_name: str) -> None:
        pass

    def after_task_callback(
        self,
        task: Any,
        context: Mapping[str, Any],
        callback_name: str,
        result: Any,
    ) -> None:
        pass

    def on_task_error(self, task: Any, context: Mapping[str, Any], error: BaseException) -> None:
        pass

    def on_task_callback_error(
        self,
        task: Any,
        context: Mapping[str, Any],
        callback_name: str,
        error: BaseException,
    ) -> None:
        pass


class _ProblemLogSuppressFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno < logging.WARNING or bool(getattr(record, "_airflow_debug_passthrough", False))


class _ProblemLogPrettyHandler(logging.Handler):
    def __init__(
        self,
        *,
        stream: Any,
        enable_colors: bool = True,
        warning_loop_threshold: int = 3,
    ) -> None:
        super().__init__(level=logging.WARNING)
        self.stream = stream
        self.enable_colors = enable_colors
        self.warning_loop_threshold = max(int(warning_loop_threshold), 2)
        self._warning_counts: dict[tuple[str, str, str], int] = {}

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno < logging.WARNING:
            return
        _maybe_mark_primary_problem_record(record)
        if _should_skip_pretty_problem_record(record):
            return
        object_label = _CURRENT_TASK_LABEL.get()
        decision = self._handle_repeated_warning(record, object_label=object_label)
        if decision == "skip":
            return
        record._airflow_debug_problem_log = True
        text = format_pretty_log_record(
            record,
            object_label=object_label,
            enable_colors=self.enable_colors,
        )
        self.stream.write(text + "\n")
        self.stream.flush()

    def _handle_repeated_warning(self, record: logging.LogRecord, *, object_label: str | None) -> str:
        if record.levelno != logging.WARNING:
            return "show"

        signature = _problem_warning_signature(record, object_label=object_label)
        if signature is None:
            return "show"

        count = self._warning_counts.get(signature, 0) + 1
        self._warning_counts[signature] = count
        if count == 1:
            return "show"
        if count < self.warning_loop_threshold:
            return "skip"

        object_name, logger_name, headline = signature
        raise RepeatedProblemWarningError(
            f"Repeated warning loop detected for {object_name} via {logger_name}: "
            f"{headline} (seen {count} times)"
        )


class RepeatedProblemWarningError(RuntimeError):
    """Raised when the same warning repeats inside one task during local debug."""


class DebugPluginManager:
    """
    Per-run plugin dispatcher.

    Plugin failures are isolated and recorded into `RunResult.notes` instead of
    breaking the DAG execution itself.
    """

    def __init__(self, plugins: Iterable[AirflowDebugPlugin] | None = None, *, notes: list[str] | None = None) -> None:
        self.plugins = [plugin for plugin in (plugins or []) if plugin is not None]
        self.notes = notes

    def _record_plugin_error(self, plugin: AirflowDebugPlugin, hook_name: str, exc: BaseException) -> None:
        message = f"Plugin {plugin.__class__.__name__}.{hook_name} failed: {exc}"
        log.debug(message, exc_info=True)
        if self.notes is not None:
            self.notes.append(message)

    def _dispatch(self, hook_name: str, *args: Any) -> None:
        for plugin in self.plugins:
            hook = getattr(plugin, hook_name, None)
            if not callable(hook):
                continue
            try:
                hook(*args)
            except Exception as exc:
                self._record_plugin_error(plugin, hook_name, exc)

    def before_run(self, dag: Any, context: Mapping[str, Any]) -> None:
        self._dispatch("before_run", dag, context)

    def after_run(self, dag: Any, context: Mapping[str, Any], result: Any) -> None:
        self._dispatch("after_run", dag, context, result)

    def before_task(self, task: Any, context: Mapping[str, Any]) -> None:
        self._dispatch("before_task", task, context)

    def after_task(self, task: Any, context: Mapping[str, Any], result: Any) -> None:
        self._dispatch("after_task", task, context, result)

    def before_task_callback(self, task: Any, context: Mapping[str, Any], callback_name: str) -> None:
        self._dispatch("before_task_callback", task, context, callback_name)

    def after_task_callback(self, task: Any, context: Mapping[str, Any], callback_name: str, result: Any) -> None:
        self._dispatch("after_task_callback", task, context, callback_name, result)

    def on_task_error(self, task: Any, context: Mapping[str, Any], error: BaseException) -> None:
        self._dispatch("on_task_error", task, context, error)

    def on_task_callback_error(
        self,
        task: Any,
        context: Mapping[str, Any],
        callback_name: str,
        error: BaseException,
    ) -> None:
        self._dispatch("on_task_callback_error", task, context, callback_name, error)


class ConsoleTracePlugin(AirflowDebugPlugin):
    """Default per-task console tracer used when `trace=True`."""

    def __init__(self) -> None:
        self._active: dict[tuple[str, str | None, int | None], StepTracer] = {}

    def before_task(self, task: Any, context: Mapping[str, Any]) -> None:
        key = _trace_key(task, context)
        if key in self._active:
            return

        tracer = StepTracer(
            _display_task_id(task, context),
            _operator_label(task),
            _run_id_from_context(context),
            _map_index_from_context(context),
            options=StepTracerOptions(stream=sys.stdout),
        )
        tracer.__enter__()
        tracer.event("resolved_kwargs", shrink(_collect_resolved_inputs(task)))
        self._active[key] = tracer

    def after_task(self, task: Any, context: Mapping[str, Any], result: Any) -> None:
        key = _trace_key(task, context)
        tracer = self._active.pop(key, None)
        if tracer is None:
            return
        tracer.__exit__(None, None, None)
        tracer.event("result", shrink(result))

    def on_task_error(self, task: Any, context: Mapping[str, Any], error: BaseException) -> None:
        key = _trace_key(task, context)
        _mark_primary_task_error(error)
        try:
            setattr(error, "_airflow_debug_live_logged", True)
        except Exception:
            pass
        tracer = self._active.pop(key, None)
        if tracer is None:
            return
        tracer.__exit__(error.__class__, error, error.__traceback__)


class TaskContextPlugin(AirflowDebugPlugin):
    """
    Expose the currently running task id to other local debug plugins.

    This is mainly used by `ProblemLogPlugin` so that warning/error log records
    can be rendered under the currently executing task label.
    """

    def __init__(self) -> None:
        self._tokens: dict[tuple[str, str | None, int | None], contextvars.Token[str | None]] = {}

    def before_task(self, task: Any, context: Mapping[str, Any]) -> None:
        key = _trace_key(task, context)
        label = _display_task_id(task, context)
        self._tokens[key] = _CURRENT_TASK_LABEL.set(label)

    def after_task(self, task: Any, context: Mapping[str, Any], result: Any) -> None:
        self._reset(task, context)

    def on_task_error(self, task: Any, context: Mapping[str, Any], error: BaseException) -> None:
        self._reset(task, context)

    def _reset(self, task: Any, context: Mapping[str, Any]) -> None:
        key = _trace_key(task, context)
        token = self._tokens.pop(key, None)
        if token is not None:
            _CURRENT_TASK_LABEL.reset(token)


class ProblemLogPlugin(AirflowDebugPlugin):
    """
    Pretty-print warning/error log records by problem category during local runs.

    The classification is generic and based on exception/log content rather than
    vendor-specific hardcoding. Categories include auth, network, timeout,
    permission, I/O, JSON, HTTP, Airflow, key, and value problems.
    """

    def __init__(self, *, stream: Any | None = None, enable_colors: bool = True) -> None:
        self.stream = stream or sys.stdout
        self.enable_colors = enable_colors
        self._pretty_handler: _ProblemLogPrettyHandler | None = None
        self._attached_loggers: list[logging.Logger] = []
        self._filtered_handlers: list[tuple[logging.Handler, logging.Filter]] = []
        self._suppress_filter: logging.Filter | None = None
        self._original_add_handler: Any | None = None
        self._failure_token: contextvars.Token[bool] | None = None

    def before_run(self, dag: Any, context: Mapping[str, Any]) -> None:
        self._pretty_handler = _ProblemLogPrettyHandler(stream=self.stream, enable_colors=self.enable_colors)
        self._failure_token = _HAS_PRIMARY_TASK_ERROR.set(False)

        root = logging.getLogger()
        loggers: list[logging.Logger] = [root]
        for value in logging.root.manager.loggerDict.values():
            if isinstance(value, logging.Logger) and value.handlers and not value.propagate:
                loggers.append(value)

        suppress_filter = _ProblemLogSuppressFilter()
        self._suppress_filter = suppress_filter
        for logger in loggers:
            logger.addHandler(self._pretty_handler)
            self._attached_loggers.append(logger)
            for handler in list(logger.handlers):
                if handler is self._pretty_handler:
                    continue
                self._attach_filter(handler, suppress_filter)

        self._patch_add_handler()

    def after_run(self, dag: Any, context: Mapping[str, Any], result: Any) -> None:
        if self._original_add_handler is not None:
            logging.Logger.addHandler = self._original_add_handler
            self._original_add_handler = None

        if self._pretty_handler is not None:
            for logger in self._attached_loggers:
                try:
                    logger.removeHandler(self._pretty_handler)
                except Exception:
                    continue
            self._pretty_handler.close()

        for handler, suppress_filter in self._filtered_handlers:
            try:
                handler.removeFilter(suppress_filter)
            except Exception:
                continue

        self._pretty_handler = None
        self._attached_loggers.clear()
        self._filtered_handlers.clear()
        self._suppress_filter = None
        if self._failure_token is not None:
            _HAS_PRIMARY_TASK_ERROR.reset(self._failure_token)
            self._failure_token = None

    def _attach_filter(self, handler: logging.Handler, suppress_filter: logging.Filter) -> None:
        if any(existing is suppress_filter for existing in getattr(handler, "filters", ())):
            return
        handler.addFilter(suppress_filter)
        self._filtered_handlers.append((handler, suppress_filter))

    def _patch_add_handler(self) -> None:
        if self._original_add_handler is not None:
            return

        original_add_handler = logging.Logger.addHandler
        suppress_filter = self._suppress_filter

        def traced_add_handler(logger: logging.Logger, handler: logging.Handler) -> None:
            original_add_handler(logger, handler)
            if (
                self._pretty_handler is not None
                and not logger.propagate
                and all(existing is not self._pretty_handler for existing in logger.handlers)
            ):
                original_add_handler(logger, self._pretty_handler)
                self._attached_loggers.append(logger)
            if suppress_filter is None or handler is self._pretty_handler:
                return
            self._attach_filter(handler, suppress_filter)

        self._original_add_handler = original_add_handler
        logging.Logger.addHandler = traced_add_handler


def _mark_primary_task_error(error: BaseException) -> None:
    _HAS_PRIMARY_TASK_ERROR.set(True)
    try:
        setattr(error, "_airflow_debug_primary_error", True)
    except Exception:
        pass


def _problem_warning_signature(
    record: logging.LogRecord,
    *,
    object_label: str | None,
) -> tuple[str, str, str] | None:
    if record.levelno != logging.WARNING:
        return None

    message = str(record.getMessage() or "").strip()
    if not message:
        return None

    lines = [line.strip() for line in message.splitlines() if line.strip()]
    headline = lines[0] if lines else message
    if not headline:
        return None

    object_name = object_label or getattr(record, "task_id", None) or getattr(record, "dag_id", None) or "<log>"
    logger_name = str(record.name or "<logger>")
    return str(object_name), logger_name, headline.lower()


def _maybe_mark_primary_problem_record(record: logging.LogRecord) -> None:
    exc = None
    if record.exc_info and len(record.exc_info) >= 2:
        exc = record.exc_info[1]

    message = str(record.getMessage() or "").lower()
    if record.name == "airflow.task" and "task failed with exception" in message:
        _HAS_PRIMARY_TASK_ERROR.set(True)
        if isinstance(exc, BaseException):
            try:
                setattr(exc, "_airflow_debug_primary_error", True)
            except Exception:
                pass


def _should_skip_pretty_problem_record(record: logging.LogRecord) -> bool:
    exc = None
    if record.exc_info and len(record.exc_info) >= 2:
        exc = record.exc_info[1]

    if isinstance(exc, BaseException) and bool(getattr(exc, "_airflow_debug_primary_error", False)):
        return True

    message = str(record.getMessage() or "").lower()
    if (
        _HAS_PRIMARY_TASK_ERROR.get()
        and record.name == "airflow.models.dag.DAG"
        and (
            ("no tasks to run" in message and "unrunnable tasks" in message)
            or message.startswith("task failed; ti=")
        )
    ):
        return True

    if (
        _HAS_PRIMARY_TASK_ERROR.get()
        and record.name == "airflow.models.dagrun.DagRun"
        and message.startswith("marking run ")
        and " failed" in message
    ):
        return True

    return False
