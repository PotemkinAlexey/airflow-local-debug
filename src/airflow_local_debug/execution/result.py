"""
Building, normalising, and post-processing of `RunResult` objects.

`result_from_dagrun` is the orchestrator that converts an Airflow dagrun into
a `RunResult`. The other helpers handle state-specific normalisation:
inferring failure / incomplete states, marking unrun downstream tasks as
``upstream_failed`` / ``not_run`` for the strict backend, and emitting notes
about deferred tasks.
"""

from __future__ import annotations

import logging
import traceback
from typing import Any

from airflow_local_debug.compat import get_airflow_version
from airflow_local_debug.execution.mocks import TaskMockRegistry
from airflow_local_debug.execution.state import (
    FAILED_TASK_STATES,
    HARD_FAILED_TASK_STATES,
    UNFINISHED_TASK_STATES,
    serialize_datetime,
    state_token,
    task_instance_label,
)
from airflow_local_debug.execution.state import (
    duration_seconds as _duration_seconds,
)
from airflow_local_debug.execution.topology import downstream_task_ids, topological_task_order
from airflow_local_debug.execution.xcom import extract_xcoms
from airflow_local_debug.models import RunResult, TaskRunInfo, normalize_state
from airflow_local_debug.reporting.traceback_utils import format_pretty_exception

_log = logging.getLogger(__name__)


def extract_task_runs(
    dagrun: Any,
    dag: Any,
    *,
    mocked_task_ids: set[str] | None = None,
) -> list[TaskRunInfo]:
    """Convert dagrun task instances into `TaskRunInfo` rows, sorted topologically."""
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
                start_date=serialize_datetime(start_date),
                end_date=serialize_datetime(end_date),
                duration_seconds=_duration_seconds(start_date, end_date),
                mocked=task_id in mocked_task_ids,
            )
        )

    task_order = topological_task_order(dag)
    task_runs.sort(
        key=lambda item: (
            task_order.get(item.task_id, 10**9),
            item.map_index if item.map_index is not None and item.map_index >= 0 else -1,
            item.task_id,
        )
    )
    return task_runs


def best_effort_last_dagrun(dag: Any) -> Any | None:
    """Return the most recent dagrun for ``dag`` from the metadata DB, or None."""
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
            serialize_datetime(getattr(run, "logical_date", None))
            or serialize_datetime(getattr(run, "execution_date", None))
            or "",
            getattr(run, "run_id", "") or "",
        ),
    )


def failed_task_label(dagrun: Any) -> str | None:
    """Comma-joined label of failed task instances in this dagrun, or None."""
    if dagrun is None or not hasattr(dagrun, "get_task_instances"):
        return None

    labels = []
    for ti in dagrun.get_task_instances():
        state = str(getattr(ti, "state", None) or "")
        if state in HARD_FAILED_TASK_STATES:
            label = task_instance_label(ti)
            if label:
                labels.append(label)

    if not labels:
        return None
    if len(labels) == 1:
        return labels[0]
    sorted_labels = sorted(labels)
    return ", ".join(sorted_labels[:3]) + (" ..." if len(sorted_labels) > 3 else "")


def task_state_buckets(task_runs: list[TaskRunInfo]) -> tuple[list[TaskRunInfo], list[TaskRunInfo]]:
    """Split task rows into (failed-or-retry-pending, still-unfinished) buckets."""
    failed: list[TaskRunInfo] = []
    unfinished: list[TaskRunInfo] = []
    for task in task_runs:
        token = state_token(task.state)
        if token in FAILED_TASK_STATES:
            failed.append(task)
        elif token in UNFINISHED_TASK_STATES:
            unfinished.append(task)
    return failed, unfinished


def normalize_task_states_for_backend(
    dag: Any,
    task_runs: list[TaskRunInfo],
    *,
    backend: str | None,
) -> list[TaskRunInfo]:
    """For the strict backend, label downstream tasks of a failure as upstream_failed/not_run."""
    if backend != "dag.test.strict" or not task_runs:
        return task_runs

    hard_failed_ids = {
        task.task_id
        for task in task_runs
        if state_token(task.state) in HARD_FAILED_TASK_STATES
    }
    if not hard_failed_ids:
        return task_runs

    downstream_ids = downstream_task_ids(dag, hard_failed_ids)
    normalized: list[TaskRunInfo] = []
    for task in task_runs:
        token = state_token(task.state)
        if token in {"success", "failed", "up_for_retry", "shutdown", "skipped", "removed", "upstream_failed"}:
            normalized.append(task)
            continue

        if token in UNFINISHED_TASK_STATES:
            new_state = "upstream_failed" if task.task_id in downstream_ids else "not_run"
            normalized.append(
                TaskRunInfo(
                    task_id=task.task_id,
                    state=new_state,
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


def summarize_task_states(task_runs: list[TaskRunInfo], *, limit: int = 5) -> str:
    chunks: list[str] = []
    for task in task_runs[:limit]:
        map_suffix = ""
        if task.map_index is not None and task.map_index >= 0:
            map_suffix = f"[{task.map_index}]"
        chunks.append(f"{task.task_id}{map_suffix}={task.state or 'none'}")
    if len(task_runs) > limit:
        chunks.append(f"... +{len(task_runs) - limit} more")
    return ", ".join(chunks)


def annotate_deferred_result(result: RunResult) -> None:
    """Append a note to `result.notes` listing tasks that ended in `deferred`."""
    deferred = [task for task in result.tasks if state_token(task.state) == "deferred"]
    if not deferred:
        return

    result.notes.append(
        "Deferrable task instance(s) remained deferred in local run: "
        f"{summarize_task_states(deferred)}. "
        "Use fail_fast=True for strict inline trigger handling, or mock these tasks with --mock-file."
    )


def normalize_result(result: RunResult) -> RunResult:
    """Force `result.state` to reflect failed / incomplete tasks and add notes."""
    failed_tasks, unfinished_tasks = task_state_buckets(result.tasks)
    token = state_token(result.state)
    if failed_tasks:
        if token not in {"failed", "error"}:
            result.state = "failed"
        result.notes.append(
            "Local run detected failed or retry-pending tasks: "
            f"{summarize_task_states(failed_tasks)}"
        )
        if result.exception is None:
            message = (
                "Local DAG run finished with failed or retry-pending tasks: "
                f"{summarize_task_states(failed_tasks)}."
            )
            if unfinished_tasks:
                message += f" Unfinished tasks: {summarize_task_states(unfinished_tasks)}."
            result.exception = message
            result.exception_raw = result.exception_raw or message
        return result

    if unfinished_tasks and token not in {"success", "failed", "error"}:
        result.state = "incomplete"
        result.notes.append(
            "Local run stopped with unfinished tasks: "
            f"{summarize_task_states(unfinished_tasks)}"
        )
        if result.exception is None:
            message = (
                "Local DAG run stopped before all tasks reached a terminal state: "
                f"{summarize_task_states(unfinished_tasks)}."
            )
            result.exception = message
            result.exception_raw = result.exception_raw or message
    return result


def result_from_dagrun(
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
    """Build a normalized `RunResult` from an Airflow dagrun."""
    state = getattr(dagrun, "state", None) if dagrun is not None else None
    logical_date = None
    exception_was_logged = False
    mocked_task_ids = task_mock_registry.mocked_task_ids if task_mock_registry is not None else set()
    mock_infos = task_mock_registry.mock_infos if task_mock_registry is not None else []
    tasks = extract_task_runs(dagrun, dag, mocked_task_ids=mocked_task_ids)
    tasks = normalize_task_states_for_backend(dag, tasks, backend=backend)
    if exception is None and dagrun is not None:
        local_exc = getattr(dagrun, "_airflow_debug_local_exception", None)
        if isinstance(local_exc, BaseException):
            exception_was_logged = bool(getattr(local_exc, "_airflow_debug_live_logged", False))
            local_label = getattr(dagrun, "_airflow_debug_local_task_label", None)
            exception = format_pretty_exception(
                local_exc,
                task_id=local_label or failed_task_label(dagrun) or getattr(dag, "dag_id", None),
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
        logical_date = serialize_datetime(getattr(dagrun, "logical_date", None) or getattr(dagrun, "execution_date", None))

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
        xcoms=extract_xcoms(dagrun, dag) if collect_xcoms else {},
        notes=notes,
        exception=exception,
        exception_raw=exception_raw,
        exception_was_logged=exception_was_logged,
    )
    result = normalize_result(result)
    annotate_deferred_result(result)
    return result
