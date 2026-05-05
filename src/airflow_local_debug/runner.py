"""
Core entrypoints for local Airflow DAG execution.

This module keeps execution as close to native Airflow as possible:
- bootstrap local env from config file
- render the DAG graph before execution
- run the whole DAG via `dag.test()` when available
- fall back to legacy `dag.run()` for older Airflow versions
- return a structured `RunResult`

Use `debug_dag(...)` when you want a single call that both runs the DAG and
prints the final report. Use `run_full_dag(...)` when you want the raw result
object and will decide yourself how to render it.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import logging
import sys
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable

from airflow_local_debug.compat import (
    build_dag_test_kwargs,
    build_legacy_dag_run_kwargs,
    get_airflow_version,
    has_dag_test,
)
from airflow_local_debug.console import print_run_preamble
from airflow_local_debug.config_loader import get_default_config_path, load_local_config
from airflow_local_debug.env_bootstrap import bootstrap_airflow_env
from airflow_local_debug.graph import format_dag_graph
from airflow_local_debug.live_trace import live_task_trace
from airflow_local_debug.models import LocalConfig, RunResult, TaskRunInfo
from airflow_local_debug.plugins import (
    AirflowDebugPlugin,
    ConsoleTracePlugin,
    DebugPluginManager,
    ProblemLogPlugin,
    TaskContextPlugin,
)
from airflow_local_debug.topology import topological_task_order as _topological_task_order
from airflow_local_debug.traceback_utils import format_pretty_exception

_FAILED_TASK_STATES = {"failed", "up_for_retry", "upstream_failed", "shutdown"}
_UNFINISHED_TASK_STATES = {
    None,
    "none",
    "scheduled",
    "queued",
    "running",
    "restarting",
    "deferred",
    "up_for_reschedule",
}


def _serialize_datetime(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    return str(value)


def _coerce_logical_date(value: str | date | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        if "T" not in raw and " " not in raw:
            return datetime.fromisoformat(f"{raw}T00:00:00")
        raise


def _extract_task_runs(dagrun: Any, dag: Any) -> list[TaskRunInfo]:
    if dagrun is None or not hasattr(dagrun, "get_task_instances"):
        return []

    task_runs: list[TaskRunInfo] = []
    for ti in dagrun.get_task_instances():
        task_runs.append(
            TaskRunInfo(
                task_id=getattr(ti, "task_id", "<unknown>"),
                state=str(getattr(ti, "state", None)) if getattr(ti, "state", None) is not None else None,
                try_number=getattr(ti, "try_number", None),
                map_index=getattr(ti, "map_index", None),
                start_date=_serialize_datetime(getattr(ti, "start_date", None)),
                end_date=_serialize_datetime(getattr(ti, "end_date", None)),
            )
        )

    task_order = _topological_task_order(dag)
    task_runs.sort(
        key=lambda item: (
            task_order.get(item.task_id, 10**9),
            item.map_index if item.map_index is not None and item.map_index >= 0 else -1,
            item.task_id,
        )
    )
    return task_runs


def _best_effort_last_dagrun(dag: Any) -> Any | None:
    try:
        from airflow.models.dagrun import DagRun
    except Exception:
        return None

    try:
        from airflow.utils.session import create_session
    except Exception:
        create_session = None  # type: ignore[assignment]

    runs = None
    if create_session is not None:
        try:
            with create_session() as session:
                try:
                    runs = DagRun.find(dag_id=dag.dag_id, session=session)
                except TypeError:
                    runs = DagRun.find(dag_id=dag.dag_id)
        except Exception:
            runs = None
    if runs is None:
        try:
            runs = DagRun.find(dag_id=dag.dag_id)
        except Exception:
            return None

    if not runs:
        return None
    return max(
        runs,
        key=lambda run: (
            _serialize_datetime(getattr(run, "logical_date", None))
            or _serialize_datetime(getattr(run, "execution_date", None))
            or "",
            getattr(run, "run_id", "") or "",
        ),
    )


def _result_from_dagrun(
    dag: Any,
    dagrun: Any,
    *,
    config_path: str | None,
    notes: list[str],
    graph_ascii: str | None = None,
    backend: str | None = None,
    exception: str | None = None,
    exception_raw: str | None = None,
) -> RunResult:
    state = getattr(dagrun, "state", None) if dagrun is not None else None
    logical_date = None
    exception_was_logged = False
    tasks = _extract_task_runs(dagrun, dag)
    tasks = _normalize_task_states_for_backend(dag, tasks, backend=backend)
    if exception is None and dagrun is not None:
        local_exc = getattr(dagrun, "_airflow_debug_local_exception", None)
        if isinstance(local_exc, BaseException):
            exception_was_logged = bool(getattr(local_exc, "_airflow_debug_live_logged", False))
            local_label = getattr(dagrun, "_airflow_debug_local_task_label", None)
            exception = format_pretty_exception(
                local_exc,
                task_id=local_label or _failed_task_label(dagrun) or getattr(dag, "dag_id", None),
            )
            if exception_raw is None:
                exception_raw = "".join(
                    traceback.format_exception(
                        local_exc.__class__,
                        local_exc,
                        local_exc.__traceback__,
                    )
                )
    if dagrun is not None:
        logical_date = _serialize_datetime(getattr(dagrun, "logical_date", None) or getattr(dagrun, "execution_date", None))

    result = RunResult(
        dag_id=dag.dag_id,
        run_id=getattr(dagrun, "run_id", None) if dagrun is not None else None,
        state=str(state) if state is not None else None,
        logical_date=logical_date,
        backend=backend,
        airflow_version=get_airflow_version(),
        config_path=config_path,
        graph_ascii=graph_ascii,
        tasks=tasks,
        notes=notes,
        exception=exception,
        exception_raw=exception_raw,
        exception_was_logged=exception_was_logged,
    )
    return _normalize_result(result)


def _task_instance_label(ti: Any) -> str | None:
    task_id = getattr(ti, "task_id", None)
    if not task_id:
        return None
    map_index = getattr(ti, "map_index", None)
    if map_index is not None and map_index >= 0:
        return f"{task_id}[{map_index}]"
    return str(task_id)


def _trace_context_for_ti(ti: Any) -> dict[str, Any]:
    return {
        "ti": ti,
        "task_instance": ti,
        "run_id": getattr(ti, "run_id", None),
    }


def _best_effort_task_result(ti: Any) -> Any:
    map_index = getattr(ti, "map_index", None)
    if map_index == -1:
        map_index = None

    pull = getattr(ti, "xcom_pull", None)
    if not callable(pull):
        return None

    kwargs: dict[str, Any] = {"task_ids": getattr(ti, "task_id", None), "key": "return_value"}
    if map_index is not None:
        kwargs["map_indexes"] = map_index

    try:
        return pull(**kwargs)
    except TypeError:
        kwargs.pop("map_indexes", None)
        try:
            return pull(**kwargs)
        except Exception:
            return None
    except Exception:
        return None


def _failed_task_label(dagrun: Any) -> str | None:
    if dagrun is None or not hasattr(dagrun, "get_task_instances"):
        return None

    failed_states = {"failed", "up_for_retry", "shutdown"}
    labels = []
    for ti in dagrun.get_task_instances():
        state = str(getattr(ti, "state", None) or "")
        if state in failed_states:
            task_label = _task_instance_label(ti)
            if task_label:
                labels.append(task_label)

    if not labels:
        return None
    if len(labels) == 1:
        return labels[0]
    return ", ".join(sorted(labels[:3])) + (" ..." if len(labels) > 3 else "")


def _state_token(state: str | None) -> str | None:
    if state is None:
        return None
    return str(state).strip().lower() or None


def _task_state_buckets(task_runs: list[TaskRunInfo]) -> tuple[list[TaskRunInfo], list[TaskRunInfo]]:
    failed: list[TaskRunInfo] = []
    unfinished: list[TaskRunInfo] = []
    for task in task_runs:
        token = _state_token(task.state)
        if token in _FAILED_TASK_STATES:
            failed.append(task)
        elif token in _UNFINISHED_TASK_STATES:
            unfinished.append(task)
    return failed, unfinished


def _downstream_task_ids(dag: Any, roots: set[str]) -> set[str]:
    task_dict = dict(getattr(dag, "task_dict", {}) or {})
    pending = list(sorted(root for root in roots if root in task_dict))
    seen: set[str] = set()
    while pending:
        task_id = pending.pop(0)
        downstream_ids = set(getattr(task_dict[task_id], "downstream_task_ids", set()) or set()) & set(task_dict)
        for child_id in sorted(downstream_ids):
            if child_id in roots or child_id in seen:
                continue
            seen.add(child_id)
            pending.append(child_id)
    return seen


def _normalize_task_states_for_backend(
    dag: Any,
    task_runs: list[TaskRunInfo],
    *,
    backend: str | None,
) -> list[TaskRunInfo]:
    if backend != "dag.test.strict" or not task_runs:
        return task_runs

    hard_failed_ids = {
        task.task_id
        for task in task_runs
        if _state_token(task.state) in {"failed", "up_for_retry", "shutdown"}
    }
    if not hard_failed_ids:
        return task_runs

    downstream_ids = _downstream_task_ids(dag, hard_failed_ids)
    normalized: list[TaskRunInfo] = []
    for task in task_runs:
        token = _state_token(task.state)
        if token in {"success", "failed", "up_for_retry", "shutdown", "skipped", "removed", "upstream_failed"}:
            normalized.append(task)
            continue

        if token in _UNFINISHED_TASK_STATES:
            if task.task_id in downstream_ids:
                normalized.append(
                    TaskRunInfo(
                        task_id=task.task_id,
                        state="upstream_failed",
                        try_number=task.try_number,
                        map_index=task.map_index,
                        start_date=task.start_date,
                        end_date=task.end_date,
                    )
                )
            else:
                normalized.append(
                    TaskRunInfo(
                        task_id=task.task_id,
                        state="not_run",
                        try_number=task.try_number,
                        map_index=task.map_index,
                        start_date=task.start_date,
                        end_date=task.end_date,
                    )
                )
            continue

        normalized.append(task)

    return normalized


def _summarize_task_states(task_runs: list[TaskRunInfo], *, limit: int = 5) -> str:
    chunks: list[str] = []
    for task in task_runs[:limit]:
        map_suffix = ""
        if task.map_index is not None and task.map_index >= 0:
            map_suffix = f"[{task.map_index}]"
        chunks.append(f"{task.task_id}{map_suffix}={task.state or 'none'}")
    if len(task_runs) > limit:
        chunks.append(f"... +{len(task_runs) - limit} more")
    return ", ".join(chunks)


def _normalize_result(result: RunResult) -> RunResult:
    failed_tasks, unfinished_tasks = _task_state_buckets(result.tasks)
    state_token = _state_token(result.state)
    if failed_tasks:
        if state_token not in {"failed", "error"}:
            result.state = "failed"
        result.notes.append(
            "Local run detected failed or retry-pending tasks: "
            f"{_summarize_task_states(failed_tasks)}"
        )
        if result.exception is None:
            message = (
                "Local DAG run finished with failed or retry-pending tasks: "
                f"{_summarize_task_states(failed_tasks)}."
            )
            if unfinished_tasks:
                message += f" Unfinished tasks: {_summarize_task_states(unfinished_tasks)}."
            result.exception = message
            result.exception_raw = result.exception_raw or message
        return result

    if unfinished_tasks and state_token not in {"success", "failed", "error"}:
        result.state = "incomplete"
        result.notes.append(
            "Local run stopped with unfinished tasks: "
            f"{_summarize_task_states(unfinished_tasks)}"
        )
        if result.exception is None:
            message = (
                "Local DAG run stopped before all tasks reached a terminal state: "
                f"{_summarize_task_states(unfinished_tasks)}."
            )
            result.exception = message
            result.exception_raw = result.exception_raw or message
    return result


def _strict_dag_test(
    dag: Any,
    *,
    execution_date: datetime | None,
    run_conf: dict[str, Any] | None,
    trace_session: Any | None = None,
) -> Any:
    from airflow.models.dag import _get_or_create_dagrun, _run_task, _triggerer_is_healthy
    from airflow.models.dagrun import DagRun, DagRunType
    from airflow.utils import timezone
    from airflow.utils.session import create_session
    from airflow.utils.state import DagRunState, State, TaskInstanceState

    def add_logger_if_needed(ti: Any) -> None:
        formatter = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
        handler = logging.StreamHandler(sys.stdout)
        handler.level = logging.INFO
        handler.setFormatter(formatter)
        if not any(isinstance(existing, logging.StreamHandler) for existing in ti.log.handlers):
            ti.log.addHandler(handler)

    execution_date = execution_date or timezone.utcnow()
    dag.validate()
    task_order = _topological_task_order(dag)

    with create_session() as session:
        dag.clear(
            start_date=execution_date,
            end_date=execution_date,
            dag_run_state=False,  # type: ignore[arg-type]
            session=session,
        )
        logical_date = timezone.coerce_datetime(execution_date)
        data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date)
        dr = _get_or_create_dagrun(
            dag=dag,
            start_date=execution_date,
            execution_date=execution_date,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
            session=session,
            conf=run_conf,
            data_interval=data_interval,
        )
        dr.dag = dag

        tasks = dag.task_dict
        while dr.state == DagRunState.RUNNING:
            session.expire_all()
            dr.dag = dag
            schedulable_tis, _ = dr.update_state(session=session)
            for schedulable in schedulable_tis:
                if schedulable.state != TaskInstanceState.UP_FOR_RESCHEDULE:
                    schedulable.try_number += 1
                schedulable.state = TaskInstanceState.SCHEDULED
            session.commit()

            all_tis = list(dr.get_task_instances(session=session))
            scheduled_tis = [ti for ti in all_tis if ti.state == TaskInstanceState.SCHEDULED]
            scheduled_tis.sort(
                key=lambda ti: (
                    task_order.get(ti.task_id, 10**9),
                    getattr(ti, "map_index", -1) if getattr(ti, "map_index", -1) >= 0 else -1,
                    ti.task_id,
                )
            )

            if not scheduled_tis:
                failed_or_terminal = any(str(getattr(ti, "state", "") or "") in {"failed", "upstream_failed"} for ti in all_tis)
                if failed_or_terminal:
                    session.expire_all()
                    dr = session.merge(dr)
                    dr.dag = dag
                    dr.update_state(session=session)
                    session.commit()
                    break
                ids_unrunnable = [ti for ti in all_tis if ti.state not in State.finished]
                if ids_unrunnable:
                    dag.log.warning("No tasks to run. unrunnable tasks: %s", set(ids_unrunnable))
                break

            triggerer_running = _triggerer_is_healthy()
            for ti in scheduled_tis:
                ti.task = tasks[ti.task_id]
                add_logger_if_needed(ti)
                trace_context = _trace_context_for_ti(ti)
                if trace_session is not None:
                    trace_session.begin_task(ti.task, trace_context)
                try:
                    _run_task(
                        ti=ti,
                        inline_trigger=not triggerer_running,
                        session=session,
                        mark_success=False,
                    )
                    if trace_session is not None:
                        trace_session.complete_task(
                            ti.task,
                            trace_context,
                            _best_effort_task_result(ti),
                        )
                except Exception as exc:
                    if trace_session is not None:
                        trace_session.fail_task(ti.task, trace_context, exc)
                    task_label = _task_instance_label(ti)
                    try:
                        setattr(dr, "_airflow_debug_local_exception", exc)
                        setattr(dr, "_airflow_debug_local_task_label", task_label)
                    except Exception:
                        pass
                    dag.log.exception("Task failed; ti=%s", ti)
                    session.expire_all()
                    dr = session.merge(dr)
                    dr.dag = dag
                    try:
                        setattr(dr, "_airflow_debug_local_exception", exc)
                        setattr(dr, "_airflow_debug_local_task_label", task_label)
                    except Exception:
                        pass
                    dr.update_state(session=session)
                    session.commit()
                    return dr
        return dr


def _build_graph_ascii(dag: Any, notes: list[str]) -> str | None:
    try:
        return format_dag_graph(dag)
    except Exception as exc:
        notes.append(f"Graph rendering skipped: {exc}")
        return None


def _backend_hint(dag: Any, *, fail_fast: bool = False) -> str:
    if has_dag_test(dag):
        return "dag.test.strict" if fail_fast else "dag.test"
    if hasattr(dag, "run") and callable(getattr(dag, "run", None)):
        return "dag.run"
    return "unsupported"


def _bootstrap_pools(local_config: LocalConfig, notes: list[str]) -> None:
    if not local_config.pools:
        return

    try:
        from airflow.models.pool import Pool
        from airflow.utils.session import create_session
    except Exception as exc:
        notes.append(f"Skipping pool bootstrap: {exc}")
        return

    try:
        with create_session() as session:
            for pool_name, pool_payload in local_config.pools.items():
                slots = int(pool_payload.get("slots", 1))
                description = pool_payload.get("description") or "Loaded by airflow_debug"
                include_deferred = pool_payload.get("include_deferred")

                pool = session.query(Pool).filter(Pool.pool == pool_name).one_or_none()
                if pool is None:
                    pool = Pool(pool=pool_name, slots=slots, description=description)
                    session.add(pool)
                else:
                    pool.slots = slots
                    pool.description = description

                if include_deferred is not None and hasattr(pool, "include_deferred"):
                    pool.include_deferred = include_deferred

            session.commit()
        notes.append(f"Bootstrapped {len(local_config.pools)} pool(s) from local config.")
    except Exception as exc:
        notes.append(f"Pool bootstrap failed: {exc}")


@contextmanager
def _local_task_policy(
    dag: Any,
    *,
    fail_fast: bool,
    notes: list[str],
):
    if not fail_fast:
        yield
        return

    originals: dict[int, dict[str, Any]] = {}
    patched_count = 0
    for task in list(getattr(dag, "task_dict", {}).values()):
        original: dict[str, Any] = {}
        for attr_name in ("retries", "retry_delay", "retry_exponential_backoff", "max_retry_delay"):
            if hasattr(task, attr_name):
                original[attr_name] = getattr(task, attr_name)
        if not original:
            continue
        originals[id(task)] = original
        patched_count += 1
        if "retries" in original:
            task.retries = 0
        if "retry_delay" in original:
            task.retry_delay = timedelta(0)
        if "retry_exponential_backoff" in original:
            task.retry_exponential_backoff = False
        if "max_retry_delay" in original:
            task.max_retry_delay = timedelta(0)

    if patched_count:
        notes.append(f"Fail-fast mode enabled for local debug; disabled retries on {patched_count} task(s).")
    try:
        yield
    finally:
        for task in list(getattr(dag, "task_dict", {}).values()):
            original = originals.get(id(task))
            if not original:
                continue
            for attr_name, value in original.items():
                setattr(task, attr_name, value)


def _build_plugin_manager(
    *,
    trace: bool,
    plugins: Iterable[AirflowDebugPlugin] | None,
    notes: list[str],
) -> DebugPluginManager:
    user_plugins = list(plugins or [])
    user_types = {type(plugin) for plugin in user_plugins}

    active_plugins: list[AirflowDebugPlugin] = []
    if TaskContextPlugin not in user_types:
        active_plugins.append(TaskContextPlugin())
    if ProblemLogPlugin not in user_types:
        active_plugins.append(ProblemLogPlugin())
    if trace and ConsoleTracePlugin not in user_types:
        active_plugins.append(ConsoleTracePlugin())
    active_plugins.extend(user_plugins)
    return DebugPluginManager(active_plugins, notes=notes)


def _error_result(
    *,
    dag: Any,
    dagrun: Any,
    config_path: str | None,
    notes: list[str],
    graph_ascii: str | None,
    backend: str | None,
    exc: BaseException,
    error_raw: str,
) -> RunResult:
    return _result_from_dagrun(
        dag,
        dagrun,
        config_path=config_path,
        notes=notes,
        graph_ascii=graph_ascii,
        backend=backend,
        exception=format_pretty_exception(
            exc,
            task_id=_failed_task_label(dagrun) or getattr(dag, "dag_id", None),
        ),
        exception_raw=error_raw,
    )


def _execute_full_dag(
    dag: Any,
    *,
    local_config: LocalConfig,
    config_path: str | None,
    logical_date: str | date | datetime | None,
    conf: dict[str, Any] | None,
    extra_env: dict[str, str] | None,
    trace: bool,
    fail_fast: bool,
    plugins: Iterable[AirflowDebugPlugin] | None,
    notes: list[str],
) -> RunResult:
    backend: str | None = None
    dagrun = None
    result: RunResult | None = None
    pending_base_exception: BaseException | None = None
    run_logical_date = _coerce_logical_date(logical_date)
    graph_ascii = _build_graph_ascii(dag, notes)
    run_context = {
        "config_path": config_path,
        "logical_date": _serialize_datetime(run_logical_date),
        "conf": dict(conf or {}),
        "extra_env": dict(extra_env or {}),
        "backend_hint": _backend_hint(dag, fail_fast=fail_fast),
        "graph_ascii": graph_ascii,
        "notes": notes,
    }
    plugin_manager = _build_plugin_manager(trace=trace, plugins=plugins, notes=notes)
    print_run_preamble(
        dag,
        backend_hint=run_context["backend_hint"],
        config_path=config_path,
        logical_date=run_context["logical_date"],
        graph_text=graph_ascii,
    )
    plugin_manager.before_run(dag, run_context)
    try:
        with bootstrap_airflow_env(config=local_config, extra_env=extra_env), _local_task_policy(
            dag,
            fail_fast=fail_fast,
            notes=notes,
        ), live_task_trace(
            dag,
            plugin_manager=plugin_manager,
            wrap_task_methods=not fail_fast,
        ) as trace_session:
            _bootstrap_pools(local_config, notes)
            if has_dag_test(dag):
                kwargs = build_dag_test_kwargs(dag, run_logical_date, conf)
                if fail_fast:
                    backend = "dag.test.strict"
                    notes.append("Using strict local dag.test loop for deterministic fail-fast execution.")
                    dagrun = _strict_dag_test(
                        dag,
                        execution_date=kwargs.get("logical_date") or kwargs.get("execution_date"),
                        run_conf=kwargs.get("run_conf") or kwargs.get("conf"),
                        trace_session=trace_session,
                    )
                else:
                    backend = "dag.test"
                    dagrun = dag.test(**kwargs)
                result = _result_from_dagrun(
                    dag,
                    dagrun,
                    config_path=config_path,
                    notes=notes,
                    graph_ascii=graph_ascii,
                    backend=backend,
                )
                return result

            if hasattr(dag, "run") and callable(getattr(dag, "run", None)):
                backend = "dag.run"
                notes.append(
                    "Falling back to legacy dag.run(); behavior may differ slightly from dag.test()."
                )

                if hasattr(dag, "clear") and callable(getattr(dag, "clear", None)) and run_logical_date is not None:
                    dag.clear(start_date=run_logical_date, end_date=run_logical_date)

                kwargs = build_legacy_dag_run_kwargs(dag, run_logical_date, conf)
                dag.run(**kwargs)
                dagrun = _best_effort_last_dagrun(dag)
                result = _result_from_dagrun(
                    dag,
                    dagrun,
                    config_path=config_path,
                    notes=notes,
                    graph_ascii=graph_ascii,
                    backend=backend,
                )
                return result

            result = RunResult(
                dag_id=getattr(dag, "dag_id", "<unknown>"),
                backend="unsupported",
                airflow_version=get_airflow_version(),
                config_path=config_path,
                graph_ascii=graph_ascii,
                notes=notes,
                exception="This Airflow runtime exposes neither dag.test() nor dag.run().",
                exception_raw="This Airflow runtime exposes neither dag.test() nor dag.run().",
            )
            return result
    except Exception as exc:
        error_raw = traceback.format_exc()
        dagrun = dagrun or _best_effort_last_dagrun(dag)
        result = _error_result(
            dag=dag,
            dagrun=dagrun,
            config_path=config_path,
            notes=notes,
            graph_ascii=graph_ascii,
            backend=backend,
            exc=exc,
            error_raw=error_raw,
        )
    except BaseException as exc:
        # SystemExit(0) is a clean shutdown, not a DAG failure — re-raise without
        # constructing an error result so the user-visible state stays neutral.
        if isinstance(exc, SystemExit) and (exc.code in (None, 0)):
            pending_base_exception = exc
        else:
            error_raw = traceback.format_exc()
            dagrun = dagrun or _best_effort_last_dagrun(dag)
            result = _error_result(
                dag=dag,
                dagrun=dagrun,
                config_path=config_path,
                notes=notes,
                graph_ascii=graph_ascii,
                backend=backend,
                exc=exc,
                error_raw=error_raw,
            )
            pending_base_exception = exc
    finally:
        if result is None:
            result = RunResult(
                dag_id=getattr(dag, "dag_id", "<unknown>"),
                backend=backend,
                airflow_version=get_airflow_version(),
                config_path=config_path,
                graph_ascii=graph_ascii,
                notes=notes,
                exception="Local DAG run did not produce a result.",
                exception_raw="Local DAG run did not produce a result.",
            )
        plugin_manager.after_run(dag, run_context, result)

    if pending_base_exception is not None:
        raise pending_base_exception
    return result


def _load_module_from_file(path: str) -> ModuleType:
    resolved = str(Path(path).expanduser().resolve())
    if not Path(resolved).exists():
        raise FileNotFoundError(f"DAG file not found: {resolved}")

    module_name = f"airflow_debug_dag_{hashlib.md5(resolved.encode()).hexdigest()}"
    spec = importlib.util.spec_from_file_location(module_name, resolved)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load DAG module from {resolved}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def _resolve_dag_from_module(module: ModuleType, dag_id: str | None = None) -> Any:
    candidates = []
    for value in module.__dict__.values():
        if isinstance(value, type):
            continue
        if hasattr(value, "dag_id") and hasattr(value, "task_dict"):
            candidates.append(value)

    if dag_id:
        for dag in candidates:
            if getattr(dag, "dag_id", None) == dag_id:
                return dag
        raise ValueError(f"DAG with dag_id='{dag_id}' not found in module {module.__name__}")

    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        raise ValueError(f"No DAG objects found in module {module.__name__}")

    dag_ids = ", ".join(sorted(getattr(dag, "dag_id", "<unknown>") for dag in candidates))
    raise ValueError(f"Multiple DAG objects found in module {module.__name__}: {dag_ids}. Pass dag_id explicitly.")


def run_full_dag(
    dag: Any,
    *,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    trace: bool = True,
    fail_fast: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
) -> RunResult:
    """
    Run an ordinary Airflow DAG end-to-end in the current Python process.

    This is the low-level API:
    - it executes the DAG
    - it returns `RunResult`
    - it does not print the final summary for you

    If you want a single-call dev entrypoint, prefer `debug_dag(...)`.
    """
    selected_config_path = config_path if config_path is not None else get_default_config_path(required=False)
    notes: list[str] = []

    try:
        if selected_config_path:
            notes.append(f"Loaded local config from {selected_config_path}")
            local_config = load_local_config(selected_config_path)
        else:
            notes.append("No local config file provided; using current Airflow environment.")
            local_config = LocalConfig()
    except Exception as exc:
        error_raw = traceback.format_exc()
        return RunResult(
            dag_id=getattr(dag, "dag_id", "<unknown>"),
            backend=_backend_hint(dag, fail_fast=fail_fast),
            airflow_version=get_airflow_version(),
            config_path=selected_config_path,
            notes=notes,
            exception=format_pretty_exception(exc, task_id=getattr(dag, "dag_id", "<dag>")),
            exception_raw=error_raw,
        )
    return _execute_full_dag(
        dag,
        local_config=local_config,
        config_path=selected_config_path,
        logical_date=logical_date,
        conf=conf,
        extra_env=extra_env,
        trace=trace,
        fail_fast=fail_fast,
        plugins=plugins,
        notes=notes,
    )


def debug_dag(
    dag: Any,
    *,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    trace: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
    include_graph_in_report: bool = False,
    raise_on_failure: bool = True,
    fail_fast: bool = True,
) -> RunResult:
    """
    Run a DAG locally and immediately print the standard final report.

    This is the convenience API intended for `if __name__ == "__main__":`
    blocks in normal DAG files.
    """
    from airflow_local_debug.report import print_run_report

    result = run_full_dag(
        dag,
        config_path=config_path,
        logical_date=logical_date,
        conf=conf,
        extra_env=extra_env,
        trace=trace,
        fail_fast=fail_fast,
        plugins=plugins,
    )
    print_run_report(result, include_graph=include_graph_in_report)
    if raise_on_failure and not result.ok:
        raise SystemExit(1)
    return result


def debug_dag_cli(
    dag: Any,
    *,
    argv: list[str] | None = None,
    require_config_path: bool = False,
    **kwargs: Any,
) -> RunResult:
    """
    Small CLI wrapper around `debug_dag(...)` for `python my_dag.py ...` usage.

    Example:
        python my_dag.py --config-path ~/airflow_defaults.py --logical-date 2026-04-07
    """
    parser = argparse.ArgumentParser(description=f"Local debug runner for DAG '{getattr(dag, 'dag_id', '<unknown>')}'")
    parser.add_argument(
        "--config-path",
        dest="config_path",
        help="Path to local Airflow debug config (connections, variables, pools).",
    )
    parser.add_argument(
        "--logical-date",
        dest="logical_date",
        help="Logical date / execution date for the local DAG run.",
    )
    parser.add_argument(
        "--no-trace",
        action="store_true",
        help="Disable live per-task console tracing.",
    )
    parser.add_argument(
        "--no-fail-fast",
        action="store_true",
        help="Keep original task retry settings instead of forcing fail-fast local debug mode.",
    )
    args = parser.parse_args(argv)

    if require_config_path and not args.config_path:
        parser.error("--config-path is required for this DAG entrypoint.")

    return debug_dag(
        dag,
        config_path=args.config_path,
        logical_date=args.logical_date,
        trace=not args.no_trace,
        fail_fast=not args.no_fail_fast,
        **kwargs,
    )


def debug_dag_file_cli(
    *,
    argv: list[str] | None = None,
) -> RunResult:
    """
    Generic CLI entrypoint for local debugging of any DAG file.

    Example:
        airflow-debug-run /abs/path/to/dag.py --dag-id my_dag --config-path ~/airflow_defaults.py
    """
    parser = argparse.ArgumentParser(description="Local debug runner for an Airflow DAG file")
    parser.add_argument("dag_file", help="Absolute path to the DAG Python file.")
    parser.add_argument(
        "--dag-id",
        dest="dag_id",
        help="Optional DAG id when the file defines multiple DAGs.",
    )
    parser.add_argument(
        "--config-path",
        dest="config_path",
        help="Path to local Airflow debug config (connections, variables, pools).",
    )
    parser.add_argument(
        "--logical-date",
        dest="logical_date",
        help="Logical date / execution date for the local DAG run.",
    )
    parser.add_argument(
        "--no-trace",
        action="store_true",
        help="Disable live per-task console tracing.",
    )
    parser.add_argument(
        "--no-fail-fast",
        action="store_true",
        help="Keep original task retry settings instead of forcing fail-fast local debug mode.",
    )
    args = parser.parse_args(argv)

    return debug_dag_from_file(
        args.dag_file,
        dag_id=args.dag_id,
        config_path=args.config_path,
        logical_date=args.logical_date,
        trace=not args.no_trace,
        fail_fast=not args.no_fail_fast,
    )


def run_full_dag_from_file(
    dag_file: str,
    *,
    dag_id: str | None = None,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    trace: bool = True,
    fail_fast: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
) -> RunResult:
    """
    Import a normal Airflow DAG file and run the entire DAG.

    This path is useful when local environment bootstrap must happen before
    the DAG module is imported.
    """
    selected_config_path = config_path if config_path is not None else get_default_config_path(required=False)
    notes: list[str] = []

    if selected_config_path:
        notes.append(f"Loaded local config from {selected_config_path}")
    else:
        notes.append("No local config file provided; using current Airflow environment.")

    try:
        with bootstrap_airflow_env(config_path=selected_config_path, extra_env=extra_env) as local_config:
            module = _load_module_from_file(dag_file)
            dag = _resolve_dag_from_module(module, dag_id=dag_id)
            # extra_env is already applied by the outer bootstrap above (needed for DAG import).
            # Pass extra_env=None to _execute_full_dag to avoid a second redundant apply.
            return _execute_full_dag(
                dag,
                local_config=local_config,
                config_path=selected_config_path,
                logical_date=logical_date,
                conf=conf,
                extra_env=None,
                trace=trace,
                fail_fast=fail_fast,
                plugins=plugins,
                notes=notes,
            )
    except Exception as exc:
        error_raw = traceback.format_exc()
        return RunResult(
            dag_id=dag_id or "<unknown>",
            config_path=selected_config_path,
            notes=notes,
            exception=format_pretty_exception(exc, task_id=dag_id or "<dag-file>"),
            exception_raw=error_raw,
        )


def debug_dag_from_file(
    dag_file: str,
    *,
    dag_id: str | None = None,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    trace: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
    include_graph_in_report: bool = False,
    raise_on_failure: bool = True,
    fail_fast: bool = True,
) -> RunResult:
    """
    Import a DAG file, run it locally, and immediately print the standard report.

    This is the file-based equivalent of `debug_dag(...)`.
    """
    from airflow_local_debug.report import print_run_report

    result = run_full_dag_from_file(
        dag_file,
        dag_id=dag_id,
        config_path=config_path,
        logical_date=logical_date,
        conf=conf,
        extra_env=extra_env,
        trace=trace,
        fail_fast=fail_fast,
        plugins=plugins,
    )
    print_run_report(result, include_graph=include_graph_in_report)
    if raise_on_failure and not result.ok:
        raise SystemExit(1)
    return result
