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
import hashlib
import importlib.util
import json
import logging
import sys
import traceback
import warnings
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType
from typing import Any

from airflow_local_debug.cli_args import add_common_run_args, add_watch_args
from airflow_local_debug.compat import (
    build_dag_test_kwargs,
    build_legacy_dag_run_kwargs,
    get_airflow_version,
    has_dag_test,
)
from airflow_local_debug.config_loader import get_default_config_path, load_local_config
from airflow_local_debug.console import print_run_preamble
from airflow_local_debug.deferrables import detect_deferrable_tasks, format_deferrable_note
from airflow_local_debug.env_bootstrap import bootstrap_airflow_env
from airflow_local_debug.graph import format_dag_graph
from airflow_local_debug.live_trace import live_task_trace
from airflow_local_debug.mocks import TaskMockRegistry, TaskMockRule, load_task_mock_rules, local_task_mocks
from airflow_local_debug.models import DagFileInfo, LocalConfig, RunResult, TaskRunInfo, normalize_state
from airflow_local_debug.plugins import (
    AirflowDebugPlugin,
    ConsoleTracePlugin,
    DebugPluginManager,
    ProblemLogPlugin,
    TaskContextPlugin,
)
from airflow_local_debug.topology import topological_task_order as _topological_task_order
from airflow_local_debug.traceback_utils import format_pretty_exception

_log = logging.getLogger(__name__)

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
            return str(value.isoformat())
        except Exception:
            return str(value)
    return str(value)


def _duration_seconds(start: Any, end: Any) -> float | None:
    if start is None or end is None:
        return None
    try:
        seconds = (end - start).total_seconds()
    except Exception:
        return None
    if seconds < 0:
        return None
    return round(float(seconds), 6)


def _coerce_logical_date(value: str | date | datetime | None) -> datetime | None:
    def ensure_aware(value: datetime) -> datetime:
        if value.tzinfo is not None and value.utcoffset() is not None:
            return value
        return value.replace(tzinfo=timezone.utc)

    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_aware(value)
    if isinstance(value, date):
        return ensure_aware(datetime.combine(value, datetime.min.time()))
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return ensure_aware(datetime.fromisoformat(raw))
    except ValueError:
        if "T" not in raw and " " not in raw:
            return ensure_aware(datetime.fromisoformat(f"{raw}T00:00:00"))
        raise


def _load_cli_conf(*, conf_json: str | None = None, conf_file: str | None = None) -> dict[str, Any] | None:
    if conf_json is not None and conf_file is not None:
        raise ValueError("Use either --conf-json or --conf-file, not both.")

    if conf_file is not None:
        path = Path(conf_file).expanduser()
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ValueError(f"Could not read --conf-file {path}: {exc}") from exc
    elif conf_json is not None:
        raw = conf_json
    else:
        return None

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid DAG run conf JSON: {exc.msg} at line {exc.lineno} column {exc.colno}."
        ) from exc

    if not isinstance(payload, dict):
        raise ValueError("DAG run conf must be a JSON object.")
    return payload


def _load_cli_extra_env(values: list[str] | None) -> dict[str, str]:
    extra_env: dict[str, str] = {}
    for value in values or []:
        key, separator, raw = value.partition("=")
        key = key.strip()
        if not separator or not key:
            raise ValueError("--env values must use KEY=VALUE format.")
        extra_env[key] = raw
    return extra_env


def _load_cli_task_mocks(values: list[str] | None) -> list[TaskMockRule]:
    rules: list[TaskMockRule] = []
    for value in values or []:
        rules.extend(load_task_mock_rules(value))
    return rules


def _load_cli_env_files(
    values: list[str] | None,
    *,
    auto_discover: bool = True,
    notes: list[str] | None = None,
) -> dict[str, str]:
    """Load and merge values from `--env-file` paths and (optionally) auto-discovered `.env`.

    Later sources win over earlier ones. The auto-discovered `.env` is the
    lowest priority and is only used when no explicit `--env-file` is given.
    """
    from airflow_local_debug.dotenv import discover_dotenv_path, parse_dotenv_file

    layers: list[dict[str, str]] = []
    explicit_paths = list(values or [])

    if not explicit_paths and auto_discover:
        auto_path = discover_dotenv_path()
        if auto_path is not None:
            layers.append(parse_dotenv_file(auto_path))
            if notes is not None:
                notes.append(f"Loaded auto-discovered env file {auto_path}")

    for path in explicit_paths:
        layers.append(parse_dotenv_file(path))
        if notes is not None:
            notes.append(f"Loaded env file {path}")

    merged: dict[str, str] = {}
    for layer in layers:
        merged.update(layer)
    return merged


def _load_cli_selector_values(values: list[str] | None, *, option_name: str) -> list[str]:
    selectors: list[str] = []
    for value in values or []:
        parts = [part.strip() for part in str(value).split(",")]
        if any(not part for part in parts):
            raise ValueError(f"{option_name} values must not be blank.")
        selectors.extend(parts)
    return selectors


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    try:
        json.dumps(value)
        return value
    except (TypeError, ValueError):
        return str(value)


def _task_xcom_label(task_id: str, map_index: int | None) -> str:
    if map_index is not None and map_index >= 0:
        return f"{task_id}[{map_index}]"
    return task_id


def _extract_task_runs(
    dagrun: Any,
    dag: Any,
    *,
    mocked_task_ids: set[str] | None = None,
) -> list[TaskRunInfo]:
    if dagrun is None or not hasattr(dagrun, "get_task_instances"):
        return []

    mocked_task_ids = mocked_task_ids or set()
    task_runs: list[TaskRunInfo] = []
    for ti in dagrun.get_task_instances():
        start_date = getattr(ti, "start_date", None)
        end_date = getattr(ti, "end_date", None)
        task_id = getattr(ti, "task_id", "<unknown>")
        task_runs.append(
            TaskRunInfo(
                task_id=task_id,
                state=normalize_state(getattr(ti, "state", None)),
                try_number=getattr(ti, "try_number", None),
                map_index=getattr(ti, "map_index", None),
                start_date=_serialize_datetime(start_date),
                end_date=_serialize_datetime(end_date),
                duration_seconds=_duration_seconds(start_date, end_date),
                mocked=task_id in mocked_task_ids,
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


def _extract_xcoms(dagrun: Any, dag: Any) -> dict[str, dict[str, Any]]:
    if dagrun is None:
        return {}

    snapshot = _query_xcoms(dagrun, dag)
    fallback = _fallback_return_xcoms(dagrun)
    for task_label, values in fallback.items():
        snapshot.setdefault(task_label, {}).update(
            {key: value for key, value in values.items() if key not in snapshot.get(task_label, {})}
        )
    return snapshot


def _query_xcoms(dagrun: Any, dag: Any) -> dict[str, dict[str, Any]]:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            from airflow.models.xcom import XCom
        from airflow.utils.session import create_session
    except Exception as exc:
        _log.debug("XCom query unavailable: %s", exc, exc_info=True)
        return {}

    dag_id = getattr(dagrun, "dag_id", None) or getattr(dag, "dag_id", None)
    run_id = getattr(dagrun, "run_id", None)
    logical_date = getattr(dagrun, "logical_date", None) or getattr(dagrun, "execution_date", None)
    if dag_id is None:
        return {}

    try:
        with create_session() as session:
            query = session.query(XCom).filter(XCom.dag_id == dag_id)
            if run_id is not None and hasattr(XCom, "run_id"):
                query = query.filter(XCom.run_id == run_id)
            elif logical_date is not None and hasattr(XCom, "execution_date"):
                query = query.filter(XCom.execution_date == logical_date)
            else:
                return {}
            rows = list(query.all())
    except Exception as exc:
        _log.debug("XCom query failed for %s: %s", dag_id, exc, exc_info=True)
        return {}

    snapshot: dict[str, dict[str, Any]] = {}
    for row in rows:
        task_id = str(getattr(row, "task_id", "<unknown>"))
        map_index = getattr(row, "map_index", None)
        key = str(getattr(row, "key", "return_value"))
        snapshot.setdefault(_task_xcom_label(task_id, map_index), {})[key] = _json_safe(getattr(row, "value", None))
    return snapshot


def _fallback_return_xcoms(dagrun: Any) -> dict[str, dict[str, Any]]:
    if not hasattr(dagrun, "get_task_instances"):
        return {}

    snapshot: dict[str, dict[str, Any]] = {}
    for ti in dagrun.get_task_instances():
        value = _best_effort_task_result(ti)
        if value is None:
            continue
        task_id = str(getattr(ti, "task_id", "<unknown>"))
        map_index = getattr(ti, "map_index", None)
        snapshot[_task_xcom_label(task_id, map_index)] = {"return_value": _json_safe(value)}
    return snapshot


def _best_effort_last_dagrun(dag: Any) -> Any | None:
    try:
        from airflow.models.dagrun import DagRun
    except Exception as exc:
        _log.debug("airflow.models.dagrun unavailable: %s", exc, exc_info=True)
        return None

    try:
        from airflow.utils.session import create_session
    except Exception as exc:
        _log.debug("airflow.utils.session unavailable: %s", exc, exc_info=True)
        create_session = None  # type: ignore[assignment]

    runs = None
    if create_session is not None:
        try:
            with create_session() as session:
                try:
                    runs = DagRun.find(dag_id=dag.dag_id, session=session)
                except TypeError:
                    runs = DagRun.find(dag_id=dag.dag_id)
        except Exception as exc:
            _log.debug("DagRun.find via session failed for %s: %s", dag.dag_id, exc, exc_info=True)
            runs = None
    if runs is None:
        try:
            runs = DagRun.find(dag_id=dag.dag_id)
        except Exception as exc:
            _log.debug("DagRun.find without session failed for %s: %s", dag.dag_id, exc, exc_info=True)
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
    task_mock_registry: TaskMockRegistry | None = None,
    collect_xcoms: bool = False,
    deferrables: list[Any] | None = None,
    selected_tasks: list[str] | None = None,
) -> RunResult:
    state = getattr(dagrun, "state", None) if dagrun is not None else None
    logical_date = None
    exception_was_logged = False
    mocked_task_ids = task_mock_registry.mocked_task_ids if task_mock_registry is not None else set()
    mock_infos = task_mock_registry.mock_infos if task_mock_registry is not None else []
    tasks = _extract_task_runs(dagrun, dag, mocked_task_ids=mocked_task_ids)
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
        state=normalize_state(state),
        logical_date=logical_date,
        backend=backend,
        airflow_version=get_airflow_version(),
        config_path=config_path,
        graph_ascii=graph_ascii,
        selected_tasks=list(selected_tasks or []),
        tasks=tasks,
        mocks=mock_infos,
        deferrables=list(deferrables or []),
        xcoms=_extract_xcoms(dagrun, dag) if collect_xcoms else {},
        notes=notes,
        exception=exception,
        exception_raw=exception_raw,
        exception_was_logged=exception_was_logged,
    )
    result = _normalize_result(result)
    _annotate_deferred_result(result)
    return result


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
    sorted_labels = sorted(labels)
    return ", ".join(sorted_labels[:3]) + (" ..." if len(sorted_labels) > 3 else "")


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


def _task_group_path(task: Any) -> str | None:
    task_group = getattr(task, "task_group", None)
    if task_group is None:
        return None
    group_id = getattr(task_group, "group_id", None)
    if group_id is None:
        return None
    text = str(group_id).strip()
    return text or None


def _task_group_path_contains(group_path: str | None, requested_group: str) -> bool:
    if group_path is None:
        return False
    return group_path == requested_group or group_path.startswith(f"{requested_group}.")


def _available_task_group_ids(dag: Any) -> list[str]:
    groups: set[str] = set()
    for task in list(getattr(dag, "task_dict", {}).values()):
        path = _task_group_path(task)
        while path:
            groups.add(path)
            parent, _, child = path.rpartition(".")
            if not parent or not child:
                break
            path = parent
    return sorted(groups)


def _format_available(values: Iterable[str], *, limit: int = 10) -> str:
    rows = sorted(values)
    if not rows:
        return "<none>"
    suffix = "" if len(rows) <= limit else f", ... +{len(rows) - limit} more"
    return ", ".join(rows[:limit]) + suffix


def _resolve_partial_task_ids(
    dag: Any,
    *,
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
) -> list[str] | None:
    exact_ids = [str(task_id).strip() for task_id in task_ids or [] if str(task_id).strip()]
    start_ids = [str(task_id).strip() for task_id in start_task_ids or [] if str(task_id).strip()]
    group_ids = [str(group_id).strip() for group_id in task_group_ids or [] if str(group_id).strip()]
    if not exact_ids and not start_ids and not group_ids:
        return None

    task_dict = dict(getattr(dag, "task_dict", {}) or {})
    if not task_dict:
        raise ValueError("Cannot select tasks from a DAG with no tasks.")

    available_ids = set(task_dict)
    missing_ids = sorted((set(exact_ids) | set(start_ids)) - available_ids)
    if missing_ids:
        raise ValueError(
            "Unknown task id(s) for partial run: "
            f"{', '.join(missing_ids)}. Available task ids: {_format_available(available_ids)}."
        )

    selected: set[str] = set(exact_ids)
    if start_ids:
        roots = set(start_ids)
        selected.update(roots)
        selected.update(_downstream_task_ids(dag, roots))

    for group_id in group_ids:
        matching = {
            task_id
            for task_id, task in task_dict.items()
            if _task_group_path_contains(_task_group_path(task), group_id)
        }
        if not matching:
            raise ValueError(
                "Unknown task group id for partial run: "
                f"{group_id}. Available task groups: {_format_available(_available_task_group_ids(dag))}."
            )
        selected.update(matching)

    order = _topological_task_order(dag)
    return sorted(selected, key=lambda task_id: (order.get(task_id, 10**9), task_id))


def _build_partial_selection_note(dag: Any, selected_task_ids: list[str]) -> str:
    total = len(getattr(dag, "task_dict", {}) or {})
    selected = len(selected_task_ids)
    return (
        f"Partial DAG run selected {selected}/{total} task(s): "
        f"{_format_available(selected_task_ids, limit=12)}."
    )


def _detect_external_upstreams(dag: Any, selected_task_ids: list[str]) -> dict[str, list[str]]:
    """For each selected task, return upstream task ids that are NOT in the selection.

    This must be called against the original (unpartitioned) DAG so the upstream
    edges are still intact.
    """
    selected = set(selected_task_ids)
    task_dict = dict(getattr(dag, "task_dict", {}) or {})
    external: dict[str, list[str]] = {}
    for task_id in selected_task_ids:
        task = task_dict.get(task_id)
        if task is None:
            continue
        upstream_ids = set(getattr(task, "upstream_task_ids", set()) or set())
        unmet = sorted(upstream_ids - selected)
        if unmet:
            external[task_id] = unmet
    return external


def _format_external_upstream_note(external: dict[str, list[str]]) -> str:
    pairs = [f"{task_id} <- {', '.join(external[task_id])}" for task_id in sorted(external)]
    head = "; ".join(pairs[:5])
    suffix = f"; ... +{len(pairs) - 5} more" if len(pairs) > 5 else ""
    return (
        "Partial run skips upstream task(s) that selected task(s) depend on: "
        f"{head}{suffix}. XCom pulls from these upstreams will return None. "
        "Provide --mock-file to inject upstream XCom values, or include the upstream "
        "chain via additional --task / --start-task selectors."
    )


def _partial_dag_for_selected_tasks(dag: Any, selected_task_ids: list[str]) -> Any:
    partial_subset = getattr(dag, "partial_subset", None)
    if not callable(partial_subset):
        raise ValueError("This Airflow DAG object does not support partial task selection.")
    return partial_subset(selected_task_ids, include_upstream=False, include_downstream=False)


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
                        duration_seconds=task.duration_seconds,
                        mocked=task.mocked,
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
                        duration_seconds=task.duration_seconds,
                        mocked=task.mocked,
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


def _annotate_deferred_result(result: RunResult) -> None:
    deferred_tasks = [task for task in result.tasks if _state_token(task.state) == "deferred"]
    if not deferred_tasks:
        return

    result.notes.append(
        "Deferrable task instance(s) remained deferred in local run: "
        f"{_summarize_task_states(deferred_tasks)}. "
        "Use fail_fast=True for strict inline trigger handling, or mock these tasks with --mock-file."
    )


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


def _write_report_artifacts(result: RunResult, report_dir: str | Path, *, include_graph: bool) -> None:
    from airflow_local_debug.report import write_run_artifacts

    resolved_report_dir = Path(report_dir).expanduser()
    resolved_report_dir.mkdir(parents=True, exist_ok=True)
    resolved_report_dir = resolved_report_dir.resolve()
    result.notes.append(f"Wrote run artifacts to {resolved_report_dir}")
    write_run_artifacts(result, resolved_report_dir, include_graph=include_graph)


def _resolve_graph_svg_path(
    *,
    report_dir: str | Path | None,
    graph_svg_path: str | Path | None,
) -> str | Path | None:
    if graph_svg_path is not None:
        return graph_svg_path
    if report_dir is None:
        return None
    return Path(report_dir).expanduser() / "graph.svg"


def _attach_graph_svg(dag: Any, result: RunResult, graph_svg_path: str | Path | None) -> None:
    if graph_svg_path is None:
        return

    from airflow_local_debug.graph import write_dag_svg

    try:
        result.graph_svg_path = write_dag_svg(dag, str(graph_svg_path))
    except Exception as exc:
        result.notes.append(f"Could not write DAG graph SVG to {Path(graph_svg_path).expanduser()}: {exc}")


def _add_logger_if_needed(ti: Any) -> None:
    task_log = getattr(ti, "log", None)
    handlers = getattr(task_log, "handlers", None)
    add_handler = getattr(task_log, "addHandler", None)
    if handlers is None or not callable(add_handler):
        return

    formatter = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
    handler = logging.StreamHandler(sys.stdout)
    handler.level = logging.INFO
    handler.setFormatter(formatter)
    if not any(isinstance(existing, logging.StreamHandler) for existing in handlers):
        add_handler(handler)


def _set_local_task_exception(dagrun: Any, ti: Any, exc: BaseException) -> None:
    try:
        dagrun._airflow_debug_local_exception = exc
        dagrun._airflow_debug_local_task_label = _task_instance_label(ti)
    except Exception:
        pass


def _refresh_task_instance(session: Any, ti: Any) -> Any:
    try:
        session.refresh(ti)
        return ti
    except Exception:
        return ti


def _is_failed_task_instance(ti: Any) -> bool:
    return _state_token(getattr(ti, "state", None)) in _FAILED_TASK_STATES


@contextmanager
def _airflow3_serialization_fileloc(dag: Any) -> Iterator[None]:
    fileloc = getattr(dag, "fileloc", None)
    if fileloc and Path(str(fileloc)).exists():
        yield
        return

    fallback = str(Path(__file__).resolve())
    had_fileloc = hasattr(dag, "fileloc")
    try:
        dag.fileloc = fallback
        yield
    finally:
        if had_fileloc:
            try:
                dag.fileloc = fileloc
            except Exception:
                pass
        else:
            try:
                delattr(dag, "fileloc")
            except Exception:
                pass


def _ensure_airflow3_serialized_dag(dag: Any, *, session: Any) -> Any:
    from airflow.dag_processing.bundles.manager import DagBundlesManager
    from airflow.models.serialized_dag import SerializedDagModel

    try:
        from airflow.serialization.definitions.dag import SerializedDAG
        from airflow.serialization.serialized_objects import DagSerialization, LazyDeserializedDAG  # type: ignore[attr-defined]
    except ImportError:
        from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedDAG  # type: ignore[attr-defined]

        DagSerialization = SerializedDAG  # type: ignore[misc,assignment]

    bundle_name = "dags-folder"
    DagBundlesManager().sync_bundles_to_db(session=session)
    session.commit()

    with _airflow3_serialization_fileloc(dag):
        SerializedDAG.bulk_write_to_db(bundle_name, None, [dag], parse_duration=None, session=session)
        SerializedDagModel.write_dag(  # type: ignore[call-arg]
            LazyDeserializedDAG.from_dag(dag),
            bundle_name=bundle_name,
            bundle_version=None,
            min_update_interval=None,
            session=session,
        )
        session.commit()
        return DagSerialization.deserialize_dag(DagSerialization.serialize_dag(dag))


@dataclass
class _StrictTaskOutcome:
    """Outcome of running one scheduled TI inside the strict loop.

    `failed` is True for the airflow3 path where `_run_task` does not raise
    on task failure but instead leaves the TI in a failed state. `return_value`
    is the task's xcom return for trace `complete_task`. `synthesized_error`
    is used by trace `fail_task` when there is no real exception object.
    """

    failed: bool
    return_value: Any = None
    synthesized_error: BaseException | None = None


@dataclass
class _StrictStateNames:
    DagRunState_RUNNING: Any
    TaskInstanceState_SCHEDULED: Any
    TaskInstanceState_UP_FOR_RESCHEDULE: Any
    State_finished: Any


def _run_strict_scheduling_loop(
    dag: Any,
    dr: Any,
    *,
    session: Any,
    dr_target_dag: Any,
    sets_scheduled_dttm: bool,
    timezone_module: Any,
    state_names: _StrictStateNames,
    run_scheduled_ti: Any,
    task_order: dict[str, int],
    tasks: dict[str, Any],
    trace_session: Any | None,
) -> Any:
    """Shared schedulable-tick + run-task loop used by every strict backend."""
    max_iterations = max(50, len(tasks) * 10)
    iteration = 0
    while dr.state == state_names.DagRunState_RUNNING:
        iteration += 1
        if iteration > max_iterations:
            dag.log.warning(
                "Strict dag.test loop exceeded %s iterations; aborting to avoid hang.",
                max_iterations,
            )
            break
        session.expire_all()
        dr.dag = dr_target_dag
        schedulable_tis, _ = dr.update_state(session=session)
        for schedulable in schedulable_tis:
            if schedulable.state != state_names.TaskInstanceState_UP_FOR_RESCHEDULE:
                schedulable.try_number += 1
            schedulable.state = state_names.TaskInstanceState_SCHEDULED
            if sets_scheduled_dttm:
                schedulable.scheduled_dttm = timezone_module.utcnow()
        session.commit()

        all_tis = list(dr.get_task_instances(session=session))
        scheduled_tis = [ti for ti in all_tis if ti.state == state_names.TaskInstanceState_SCHEDULED]
        scheduled_tis.sort(
            key=lambda ti: (
                task_order.get(ti.task_id, 10**9),
                getattr(ti, "map_index", -1) if getattr(ti, "map_index", -1) >= 0 else -1,
                ti.task_id,
            )
        )

        if not scheduled_tis:
            failed_or_terminal = any(
                _state_token(getattr(ti, "state", None)) in {"failed", "upstream_failed"} for ti in all_tis
            )
            if failed_or_terminal:
                session.expire_all()
                dr = session.merge(dr)
                dr.dag = dr_target_dag
                dr.update_state(session=session)
                session.commit()
                break
            ids_unrunnable = [ti for ti in all_tis if ti.state not in state_names.State_finished]
            if ids_unrunnable:
                dag.log.warning("No tasks to run. unrunnable tasks: %s", set(ids_unrunnable))
            break

        for ti in scheduled_tis:
            task = tasks[ti.task_id]
            ti.task = task
            _add_logger_if_needed(ti)
            trace_context = _trace_context_for_ti(ti)
            if trace_session is not None:
                trace_session.begin_task(task, trace_context)

            try:
                outcome: _StrictTaskOutcome = run_scheduled_ti(ti, task)
            except Exception as exc:
                if trace_session is not None:
                    trace_session.fail_task(task, trace_context, exc)
                _set_local_task_exception(dr, ti, exc)
                dag.log.exception("Task failed; ti=%s", ti)
                session.expire_all()
                dr = session.merge(dr)
                dr.dag = dr_target_dag
                _set_local_task_exception(dr, ti, exc)
                dr.update_state(session=session)
                session.commit()
                return dr

            if outcome.failed:
                if trace_session is not None:
                    trace_session.fail_task(
                        task,
                        trace_context,
                        outcome.synthesized_error or RuntimeError(f"Task {_task_instance_label(ti) or ti.task_id} failed."),
                    )
                session.expire_all()
                dr = session.merge(dr)
                dr.dag = dr_target_dag
                dr.update_state(session=session)
                session.commit()
                return dr

            if trace_session is not None:
                trace_session.complete_task(task, trace_context, outcome.return_value)
    return dr


def _strict_dag_test_airflow2(
    dag: Any,
    *,
    execution_date: datetime | None,
    run_conf: dict[str, Any] | None,
    trace_session: Any | None = None,
) -> Any:
    from airflow.models.dag import _get_or_create_dagrun, _run_task, _triggerer_is_healthy
    from airflow.models.dagrun import DagRun, DagRunType  # type: ignore[attr-defined]
    from airflow.utils import timezone  # type: ignore[attr-defined]
    from airflow.utils.session import create_session
    from airflow.utils.state import DagRunState, State, TaskInstanceState

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
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),  # type: ignore[misc,call-arg]
            session=session,
            conf=run_conf,
            data_interval=data_interval,
        )
        dr.dag = dag

        def run_scheduled_ti(ti: Any, task: Any) -> _StrictTaskOutcome:
            triggerer_running = _triggerer_is_healthy()
            _run_task(
                ti=ti,
                inline_trigger=not triggerer_running,
                session=session,
                mark_success=False,
            )
            return _StrictTaskOutcome(failed=False, return_value=_best_effort_task_result(ti))

        return _run_strict_scheduling_loop(
            dag,
            dr,
            session=session,
            dr_target_dag=dag,
            sets_scheduled_dttm=False,
            timezone_module=timezone,
            state_names=_StrictStateNames(
                DagRunState_RUNNING=DagRunState.RUNNING,
                TaskInstanceState_SCHEDULED=TaskInstanceState.SCHEDULED,
                TaskInstanceState_UP_FOR_RESCHEDULE=TaskInstanceState.UP_FOR_RESCHEDULE,
                State_finished=State.finished,
            ),
            run_scheduled_ti=run_scheduled_ti,
            task_order=task_order,
            tasks=dag.task_dict,
            trace_session=trace_session,
        )


def _strict_dag_test_airflow3_legacy(
    dag: Any,
    *,
    execution_date: datetime | None,
    run_conf: dict[str, Any] | None,
    trace_session: Any | None = None,
) -> Any:
    from airflow.models.dag import DAG as SchedulerDAG
    from airflow.models.dag import _get_or_create_dagrun
    from airflow.models.dagrun import DagRun
    from airflow.sdk.definitions.dag import _run_task
    from airflow.serialization.serialized_objects import SerializedDAG  # type: ignore[attr-defined]
    from airflow.utils import timezone  # type: ignore[attr-defined]
    from airflow.utils.session import create_session
    from airflow.utils.state import DagRunState, State, TaskInstanceState
    from airflow.utils.types import DagRunTriggeredByType, DagRunType  # type: ignore[attr-defined]

    execution_date = execution_date or timezone.utcnow()
    dag.validate()
    task_order = _topological_task_order(dag)

    with create_session() as session:
        SchedulerDAG.clear_dags(
            dags=[dag],
            start_date=execution_date,
            end_date=execution_date,
            dag_run_state=False,  # type: ignore[arg-type]
        )
        logical_date = timezone.coerce_datetime(execution_date)
        run_after = logical_date or timezone.coerce_datetime(timezone.utcnow())
        data_interval = dag.timetable.infer_manual_data_interval(run_after=logical_date) if logical_date else None
        scheduler_dag = SerializedDAG.deserialize_dag(SerializedDAG.serialize_dag(dag))  # type: ignore[arg-type,attr-defined]
        dr = _get_or_create_dagrun(  # type: ignore[call-arg]
            dag=scheduler_dag,
            start_date=logical_date or run_after,
            logical_date=logical_date,
            data_interval=data_interval,
            run_after=run_after,
            run_id=DagRun.generate_run_id(  # type: ignore[call-arg]
                run_type=DagRunType.MANUAL,
                logical_date=logical_date,
                run_after=run_after,
            ),
            session=session,
            conf=run_conf,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        try:
            dr.start_dr_spans_if_needed(tis=[])
        except Exception:
            pass
        dr.dag = dag

        def run_scheduled_ti(ti: Any, task: Any) -> _StrictTaskOutcome:
            _run_task(ti=ti, run_triggerer=True)  # type: ignore[call-arg]
            ti = _refresh_task_instance(session, ti)
            if _is_failed_task_instance(ti):
                return _StrictTaskOutcome(failed=True)
            return _StrictTaskOutcome(failed=False, return_value=_best_effort_task_result(ti))

        return _run_strict_scheduling_loop(
            dag,
            dr,
            session=session,
            dr_target_dag=dag,
            sets_scheduled_dttm=True,
            timezone_module=timezone,
            state_names=_StrictStateNames(
                DagRunState_RUNNING=DagRunState.RUNNING,
                TaskInstanceState_SCHEDULED=TaskInstanceState.SCHEDULED,
                TaskInstanceState_UP_FOR_RESCHEDULE=TaskInstanceState.UP_FOR_RESCHEDULE,
                State_finished=State.finished,
            ),
            run_scheduled_ti=run_scheduled_ti,
            task_order=task_order,
            tasks=dag.task_dict,
            trace_session=trace_session,
        )


def _strict_dag_test_airflow3_serialized(
    dag: Any,
    *,
    execution_date: datetime | None,
    run_conf: dict[str, Any] | None,
    trace_session: Any | None = None,
) -> Any:
    from airflow.models.dagrun import DagRun, get_or_create_dagrun  # type: ignore[attr-defined]
    from airflow.sdk import DagRunState, TaskInstanceState, timezone
    from airflow.sdk.definitions.dag import _run_task
    from airflow.utils.session import create_session
    from airflow.utils.state import State
    from airflow.utils.types import DagRunTriggeredByType, DagRunType  # type: ignore[attr-defined]

    try:
        from airflow.serialization.definitions.dag import SerializedDAG
    except ImportError:
        from airflow.serialization.serialized_objects import SerializedDAG  # type: ignore[attr-defined,no-redef]

    try:
        from airflow.serialization.encoders import coerce_to_core_timetable
    except ImportError:
        coerce_to_core_timetable = None  # type: ignore[assignment]

    execution_date = execution_date or timezone.utcnow()
    dag.validate()
    task_order = _topological_task_order(dag)

    with create_session() as session:
        scheduler_dag = _ensure_airflow3_serialized_dag(dag, session=session)
        SerializedDAG.clear_dags(
            dags=[scheduler_dag],
            start_date=execution_date,
            end_date=execution_date,
            dag_run_state=False,  # type: ignore[arg-type]
        )
        logical_date = timezone.coerce_datetime(execution_date)
        run_after = timezone.coerce_datetime(timezone.utcnow())
        if logical_date is None:
            data_interval = None  # type: ignore[unreachable]
        else:
            timetable = coerce_to_core_timetable(dag.timetable) if coerce_to_core_timetable else dag.timetable  # type: ignore[truthy-function,misc]
            data_interval = timetable.infer_manual_data_interval(run_after=logical_date)

        scheduler_dag.on_success_callback = dag.on_success_callback  # type: ignore[attr-defined, union-attr]
        scheduler_dag.on_failure_callback = dag.on_failure_callback  # type: ignore[attr-defined, union-attr]
        dr = get_or_create_dagrun(
            dag=scheduler_dag,
            start_date=logical_date or run_after,
            logical_date=logical_date,
            data_interval=data_interval,
            run_after=run_after,
            run_id=DagRun.generate_run_id(  # type: ignore[call-arg]
                run_type=DagRunType.MANUAL,
                logical_date=logical_date,
                run_after=run_after,
            ),
            session=session,
            conf=run_conf,
            triggered_by=DagRunTriggeredByType.TEST,
            triggering_user_name="airflow-local-debug",
        )
        dr.dag = scheduler_dag

        def run_scheduled_ti(ti: Any, task: Any) -> _StrictTaskOutcome:
            _run_task(ti=ti, task=task, run_triggerer=True)
            ti = _refresh_task_instance(session, ti)
            if _is_failed_task_instance(ti):
                return _StrictTaskOutcome(failed=True)
            return _StrictTaskOutcome(failed=False, return_value=_best_effort_task_result(ti))

        return _run_strict_scheduling_loop(
            dag,
            dr,
            session=session,
            dr_target_dag=scheduler_dag,
            sets_scheduled_dttm=True,
            timezone_module=timezone,
            state_names=_StrictStateNames(
                DagRunState_RUNNING=DagRunState.RUNNING,
                TaskInstanceState_SCHEDULED=TaskInstanceState.SCHEDULED,
                TaskInstanceState_UP_FOR_RESCHEDULE=TaskInstanceState.UP_FOR_RESCHEDULE,
                State_finished=State.finished,
            ),
            run_scheduled_ti=run_scheduled_ti,
            task_order=task_order,
            tasks=dag.task_dict,
            trace_session=trace_session,
        )


def _strict_dag_test(
    dag: Any,
    *,
    execution_date: datetime | None,
    run_conf: dict[str, Any] | None,
    trace_session: Any | None = None,
) -> Any:
    has_airflow2_private_runner = False
    try:
        from airflow.models.dag import _run_task as _airflow2_run_task  # noqa: F401
    except ImportError:
        pass
    else:
        has_airflow2_private_runner = True

    if has_airflow2_private_runner:
        return _strict_dag_test_airflow2(
            dag,
            execution_date=execution_date,
            run_conf=run_conf,
            trace_session=trace_session,
        )

    has_airflow3_serialized_runner = False
    try:
        from airflow.models.dagrun import get_or_create_dagrun as _airflow3_get_or_create  # type: ignore[attr-defined]  # noqa: F401
    except ImportError:
        pass
    else:
        has_airflow3_serialized_runner = True

    if has_airflow3_serialized_runner:
        return _strict_dag_test_airflow3_serialized(
            dag,
            execution_date=execution_date,
            run_conf=run_conf,
            trace_session=trace_session,
        )

    return _strict_dag_test_airflow3_legacy(
        dag,
        execution_date=execution_date,
        run_conf=run_conf,
        trace_session=trace_session,
    )


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
) -> Iterator[None]:
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
            saved_original = originals.get(id(task))
            if not saved_original:
                continue
            for attr_name, value in saved_original.items():
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
    task_mock_registry: TaskMockRegistry | None = None,
    collect_xcoms: bool = False,
    deferrables: list[Any] | None = None,
    selected_tasks: list[str] | None = None,
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
        task_mock_registry=task_mock_registry,
        collect_xcoms=collect_xcoms,
        deferrables=deferrables,
        selected_tasks=selected_tasks,
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
    task_mocks: Iterable[TaskMockRule] | None,
    collect_xcoms: bool,
    notes: list[str],
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
    graph_svg_path: str | Path | None = None,
) -> RunResult:
    backend: str | None = None
    dagrun = None
    result: RunResult | None = None
    pending_base_exception: BaseException | None = None
    task_mock_registry: TaskMockRegistry | None = None
    selected_task_ids: list[str] = []
    run_logical_date = _coerce_logical_date(logical_date)
    task_mock_rules = list(task_mocks or [])
    graph_ascii: str | None = None
    deferrables: list[Any] = []
    run_context: dict[str, Any] = {}
    plugin_manager: DebugPluginManager | None = None
    try:
        selected = _resolve_partial_task_ids(
            dag,
            task_ids=task_ids,
            start_task_ids=start_task_ids,
            task_group_ids=task_group_ids,
        )
        if selected is not None:
            selected_task_ids = selected
            notes.append(_build_partial_selection_note(dag, selected_task_ids))
            external_upstreams = _detect_external_upstreams(dag, selected_task_ids)
            mocked_external = {
                task_id: ups
                for task_id, ups in external_upstreams.items()
                if any(rule.task_id in ups for rule in task_mock_rules)
            }
            unmocked_external = {
                task_id: ups
                for task_id, ups in external_upstreams.items()
                if not any(rule.task_id in ups for rule in task_mock_rules)
            }
            if unmocked_external:
                notes.append(_format_external_upstream_note(unmocked_external))
            if mocked_external:
                covered = sorted({up for ups in mocked_external.values() for up in ups})
                notes.append(
                    f"Partial run upstream XCom mocks active for: {', '.join(covered)}."
                )
            dag = _partial_dag_for_selected_tasks(dag, selected_task_ids)

        graph_ascii = _build_graph_ascii(dag, notes)
        backend_hint = _backend_hint(dag, fail_fast=fail_fast)
        deferrables = detect_deferrable_tasks(dag, backend_hint=backend_hint)
        deferrable_note = format_deferrable_note(deferrables)
        if deferrable_note:
            notes.append(deferrable_note)
        run_context = {
            "config_path": config_path,
            "logical_date": _serialize_datetime(run_logical_date),
            "conf": dict(conf or {}),
            "extra_env": dict(extra_env or {}),
            "backend_hint": backend_hint,
            "graph_ascii": graph_ascii,
            "task_mocks": [rule.describe() for rule in task_mock_rules],
            "selected_tasks": list(selected_task_ids),
            "deferrables": [info.task_id for info in deferrables],
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
        with bootstrap_airflow_env(config=local_config, extra_env=extra_env), _local_task_policy(
            dag,
            fail_fast=fail_fast,
            notes=notes,
        ), local_task_mocks(
            dag,
            task_mock_rules,
            notes=notes,
        ) as task_mock_registry, live_task_trace(
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
                    task_mock_registry=task_mock_registry,
                    collect_xcoms=collect_xcoms,
                    deferrables=deferrables,
                    selected_tasks=selected_task_ids,
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
                    task_mock_registry=task_mock_registry,
                    collect_xcoms=collect_xcoms,
                    deferrables=deferrables,
                    selected_tasks=selected_task_ids,
                )
                return result

            result = RunResult(
                dag_id=getattr(dag, "dag_id", "<unknown>"),
                backend="unsupported",
                airflow_version=get_airflow_version(),
                config_path=config_path,
                graph_ascii=graph_ascii,
                selected_tasks=selected_task_ids,
                deferrables=deferrables,
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
            task_mock_registry=task_mock_registry,
            collect_xcoms=collect_xcoms,
            deferrables=deferrables,
            selected_tasks=selected_task_ids,
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
                task_mock_registry=task_mock_registry,
                collect_xcoms=collect_xcoms,
                deferrables=deferrables,
                selected_tasks=selected_task_ids,
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
                selected_tasks=selected_task_ids,
                notes=notes,
                exception="Local DAG run did not produce a result.",
                exception_raw="Local DAG run did not produce a result.",
            )
        _attach_graph_svg(dag, result, graph_svg_path)
        if plugin_manager is not None:
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


def _dag_candidates_from_module(module: ModuleType) -> list[Any]:
    candidates: list[Any] = []
    seen: set[int] = set()
    for value in module.__dict__.values():
        if isinstance(value, type):
            continue
        if hasattr(value, "dag_id") and hasattr(value, "task_dict"):
            if id(value) in seen:
                continue
            seen.add(id(value))
            candidates.append(value)
    candidates.sort(key=lambda dag: str(getattr(dag, "dag_id", "<unknown>")))
    return candidates


def _dag_file_info(dag: Any) -> DagFileInfo:
    task_dict = getattr(dag, "task_dict", None)
    tasks = getattr(dag, "tasks", None)
    if task_dict:
        task_count = len(task_dict)
    elif tasks:
        task_count = len(tasks)
    else:
        task_count = 0
    fileloc = getattr(dag, "fileloc", None)
    return DagFileInfo(
        dag_id=str(getattr(dag, "dag_id", "<unknown>")),
        task_count=task_count,
        fileloc=str(fileloc) if fileloc else None,
    )


def format_dag_list(infos: Iterable[DagFileInfo], *, source_path: str | None = None) -> str:
    rows = list(infos)
    heading = "DAGs"
    if source_path:
        heading += f" in {Path(source_path).expanduser().resolve()}"
    lines = [heading]
    if not rows:
        lines.append("<none>")
        return "\n".join(lines)

    for info in rows:
        task_label = "task" if info.task_count == 1 else "tasks"
        lines.append(f"- {info.dag_id} ({info.task_count} {task_label})")
    return "\n".join(lines)


def _resolve_dag_from_module(module: ModuleType, dag_id: str | None = None) -> Any:
    candidates = _dag_candidates_from_module(module)

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


def list_dags_from_file(
    dag_file: str,
    *,
    config_path: str | None = None,
    extra_env: dict[str, str] | None = None,
) -> list[DagFileInfo]:
    selected_config_path = config_path if config_path is not None else get_default_config_path(required=False)
    with bootstrap_airflow_env(config_path=selected_config_path, extra_env=extra_env):
        module = _load_module_from_file(dag_file)
        return [_dag_file_info(dag) for dag in _dag_candidates_from_module(module)]


def run_full_dag(
    dag: Any,
    *,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    graph_svg_path: str | Path | None = None,
    trace: bool = True,
    fail_fast: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
    task_mocks: Iterable[TaskMockRule] | None = None,
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
    collect_xcoms: bool = False,
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
        result = RunResult(
            dag_id=getattr(dag, "dag_id", "<unknown>"),
            backend=_backend_hint(dag, fail_fast=fail_fast),
            airflow_version=get_airflow_version(),
            config_path=selected_config_path,
            notes=notes,
            exception=format_pretty_exception(exc, task_id=getattr(dag, "dag_id", "<dag>")),
            exception_raw=error_raw,
        )
        _attach_graph_svg(dag, result, graph_svg_path)
        return result
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
        task_mocks=task_mocks,
        collect_xcoms=collect_xcoms,
        notes=notes,
        task_ids=task_ids,
        start_task_ids=start_task_ids,
        task_group_ids=task_group_ids,
        graph_svg_path=graph_svg_path,
    )


def debug_dag(
    dag: Any,
    *,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    graph_svg_path: str | Path | None = None,
    trace: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
    include_graph_in_report: bool = False,
    report_dir: str | Path | None = None,
    xcom_json_path: str | Path | None = None,
    collect_xcoms: bool = False,
    raise_on_failure: bool = True,
    fail_fast: bool = True,
    task_mocks: Iterable[TaskMockRule] | None = None,
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
) -> RunResult:
    """
    Run a DAG locally and immediately print the standard final report.

    This is the convenience API intended for `if __name__ == "__main__":`
    blocks in normal DAG files.
    """
    from airflow_local_debug.report import print_run_report

    resolved_graph_svg_path = _resolve_graph_svg_path(report_dir=report_dir, graph_svg_path=graph_svg_path)
    result = run_full_dag(
        dag,
        config_path=config_path,
        logical_date=logical_date,
        conf=conf,
        extra_env=extra_env,
        graph_svg_path=resolved_graph_svg_path,
        trace=trace,
        fail_fast=fail_fast,
        plugins=plugins,
        task_mocks=task_mocks,
        task_ids=task_ids,
        start_task_ids=start_task_ids,
        task_group_ids=task_group_ids,
        collect_xcoms=collect_xcoms or xcom_json_path is not None,
    )
    if xcom_json_path is not None:
        from airflow_local_debug.report import write_xcom_snapshot

        xcom_path = write_xcom_snapshot(result, xcom_json_path)
        result.notes.append(f"Wrote XCom snapshot to {xcom_path}")
    if report_dir is not None:
        _write_report_artifacts(result, report_dir, include_graph=include_graph_in_report)
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
    add_common_run_args(parser)
    args = parser.parse_args(argv)

    if require_config_path and not args.config_path:
        parser.error("--config-path is required for this DAG entrypoint.")

    try:
        conf = _load_cli_conf(conf_json=args.conf_json, conf_file=args.conf_file)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        cli_extra_env = _load_cli_extra_env(args.env)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        env_file_values = _load_cli_env_files(
            args.env_file,
            auto_discover=not args.no_auto_env,
        )
    except ValueError as exc:
        parser.error(str(exc))
    try:
        task_mocks = _load_cli_task_mocks(args.mock_file)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        cli_task_ids = _load_cli_selector_values(args.task_ids, option_name="--task")
        cli_start_task_ids = _load_cli_selector_values(args.start_task_ids, option_name="--start-task")
        cli_task_group_ids = _load_cli_selector_values(args.task_group_ids, option_name="--task-group")
    except ValueError as exc:
        parser.error(str(exc))

    if conf is None:
        conf = kwargs.pop("conf", None)
    else:
        kwargs.pop("conf", None)
    programmatic_extra_env = kwargs.pop("extra_env", None)
    extra_env = dict(programmatic_extra_env or {})
    extra_env.update(env_file_values)
    extra_env.update(cli_extra_env)
    if task_mocks:
        kwargs.pop("task_mocks", None)
    else:
        task_mocks = kwargs.pop("task_mocks", None)
    programmatic_collect_xcoms = bool(kwargs.pop("collect_xcoms", False))
    programmatic_xcom_json_path = kwargs.pop("xcom_json_path", None)
    xcom_json_path = args.xcom_json_path or programmatic_xcom_json_path
    programmatic_task_ids = kwargs.pop("task_ids", None)
    programmatic_start_task_ids = kwargs.pop("start_task_ids", None)
    programmatic_task_group_ids = kwargs.pop("task_group_ids", None)
    task_ids = cli_task_ids or programmatic_task_ids
    start_task_ids = cli_start_task_ids or programmatic_start_task_ids
    task_group_ids = cli_task_group_ids or programmatic_task_group_ids

    return debug_dag(
        dag,
        config_path=args.config_path,
        logical_date=args.logical_date,
        conf=conf,
        extra_env=extra_env or None,
        trace=not args.no_trace,
        fail_fast=not args.no_fail_fast,
        include_graph_in_report=args.include_graph_in_report,
        report_dir=args.report_dir,
        graph_svg_path=args.graph_svg_path,
        task_mocks=task_mocks,
        task_ids=task_ids,
        start_task_ids=start_task_ids,
        task_group_ids=task_group_ids,
        collect_xcoms=programmatic_collect_xcoms or args.dump_xcom or xcom_json_path is not None,
        xcom_json_path=xcom_json_path,
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
        "--list-dags",
        action="store_true",
        help="List DAG ids discovered in the file and exit without running a DAG.",
    )
    add_common_run_args(parser)
    add_watch_args(parser)
    args = parser.parse_args(argv)

    try:
        extra_env = _load_cli_extra_env(args.env)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        env_file_values = _load_cli_env_files(
            args.env_file,
            auto_discover=not args.no_auto_env,
        )
    except ValueError as exc:
        parser.error(str(exc))
    # --env wins over --env-file / auto .env
    merged_extra_env = dict(env_file_values)
    merged_extra_env.update(extra_env)
    extra_env = merged_extra_env
    try:
        task_mocks = _load_cli_task_mocks(args.mock_file)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        task_ids = _load_cli_selector_values(args.task_ids, option_name="--task")
        start_task_ids = _load_cli_selector_values(args.start_task_ids, option_name="--start-task")
        task_group_ids = _load_cli_selector_values(args.task_group_ids, option_name="--task-group")
    except ValueError as exc:
        parser.error(str(exc))

    if args.list_dags:
        try:
            infos = list_dags_from_file(
                args.dag_file,
                config_path=args.config_path,
                extra_env=extra_env or None,
            )
        except Exception as exc:
            parser.error(str(exc))
        print(format_dag_list(infos, source_path=args.dag_file))
        return RunResult(
            dag_id="<list-dags>",
            state="success",
            config_path=args.config_path,
            notes=[f"Listed {len(infos)} DAG(s) from {args.dag_file}."],
        )

    try:
        conf = _load_cli_conf(conf_json=args.conf_json, conf_file=args.conf_file)
    except ValueError as exc:
        parser.error(str(exc))

    if args.watch:
        from airflow_local_debug.watch import watch_dag_file

        return watch_dag_file(
            args.dag_file,
            dag_id=args.dag_id,
            watch_paths=args.watch_path or [],
            poll_interval=args.watch_interval,
            config_path=args.config_path,
            logical_date=args.logical_date,
            conf=conf,
            extra_env=extra_env or None,
            trace=not args.no_trace,
            fail_fast=not args.no_fail_fast,
            task_mocks=task_mocks,
            task_ids=task_ids,
            start_task_ids=start_task_ids,
            task_group_ids=task_group_ids,
            collect_xcoms=args.dump_xcom or args.xcom_json_path is not None,
        )

    return debug_dag_from_file(
        args.dag_file,
        dag_id=args.dag_id,
        config_path=args.config_path,
        logical_date=args.logical_date,
        conf=conf,
        extra_env=extra_env or None,
        trace=not args.no_trace,
        fail_fast=not args.no_fail_fast,
        include_graph_in_report=args.include_graph_in_report,
        report_dir=args.report_dir,
        graph_svg_path=args.graph_svg_path,
        task_mocks=task_mocks,
        task_ids=task_ids,
        start_task_ids=start_task_ids,
        task_group_ids=task_group_ids,
        collect_xcoms=args.dump_xcom or args.xcom_json_path is not None,
        xcom_json_path=args.xcom_json_path,
    )


def run_full_dag_from_file(
    dag_file: str,
    *,
    dag_id: str | None = None,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    graph_svg_path: str | Path | None = None,
    trace: bool = True,
    fail_fast: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
    task_mocks: Iterable[TaskMockRule] | None = None,
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
    collect_xcoms: bool = False,
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
                task_mocks=task_mocks,
                collect_xcoms=collect_xcoms,
                notes=notes,
                task_ids=task_ids,
                start_task_ids=start_task_ids,
                task_group_ids=task_group_ids,
                graph_svg_path=graph_svg_path,
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
    graph_svg_path: str | Path | None = None,
    trace: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
    include_graph_in_report: bool = False,
    report_dir: str | Path | None = None,
    xcom_json_path: str | Path | None = None,
    collect_xcoms: bool = False,
    raise_on_failure: bool = True,
    fail_fast: bool = True,
    task_mocks: Iterable[TaskMockRule] | None = None,
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
) -> RunResult:
    """
    Import a DAG file, run it locally, and immediately print the standard report.

    This is the file-based equivalent of `debug_dag(...)`.
    """
    from airflow_local_debug.report import print_run_report

    resolved_graph_svg_path = _resolve_graph_svg_path(report_dir=report_dir, graph_svg_path=graph_svg_path)
    result = run_full_dag_from_file(
        dag_file,
        dag_id=dag_id,
        config_path=config_path,
        logical_date=logical_date,
        conf=conf,
        extra_env=extra_env,
        graph_svg_path=resolved_graph_svg_path,
        trace=trace,
        fail_fast=fail_fast,
        plugins=plugins,
        task_mocks=task_mocks,
        task_ids=task_ids,
        start_task_ids=start_task_ids,
        task_group_ids=task_group_ids,
        collect_xcoms=collect_xcoms or xcom_json_path is not None,
    )
    if xcom_json_path is not None:
        from airflow_local_debug.report import write_xcom_snapshot

        xcom_path = write_xcom_snapshot(result, xcom_json_path)
        result.notes.append(f"Wrote XCom snapshot to {xcom_path}")
    if report_dir is not None:
        _write_report_artifacts(result, report_dir, include_graph=include_graph_in_report)
    print_run_report(result, include_graph=include_graph_in_report)
    if raise_on_failure and not result.ok:
        raise SystemExit(1)
    return result
