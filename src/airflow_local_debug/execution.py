"""
Strict-loop machinery for ``dag.test``-style local execution.

Three Airflow API surfaces — Airflow 2, Airflow 3 legacy, Airflow 3 serialized —
each need a slightly different dagrun-creation incantation and ``_run_task``
call. The common scheduling-tick loop lives in `_run_strict_scheduling_loop`
and the backend-specific bits are isolated in three thin wrappers.

`_strict_dag_test` is the public entry point; it picks the right backend by
sniffing import availability.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from airflow_local_debug._state_helpers import (
    FAILED_TASK_STATES,
    best_effort_task_result,
    state_token,
    task_instance_label,
    trace_context_for_ti,
)
from airflow_local_debug.topology import topological_task_order


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
        dagrun._airflow_debug_local_task_label = task_instance_label(ti)
    except Exception:
        pass


def _refresh_task_instance(session: Any, ti: Any) -> Any:
    try:
        session.refresh(ti)
        return ti
    except Exception:
        return ti


def _is_failed_task_instance(ti: Any) -> bool:
    return state_token(getattr(ti, "state", None)) in FAILED_TASK_STATES


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
                state_token(getattr(ti, "state", None)) in {"failed", "upstream_failed"} for ti in all_tis
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
            trace_context = trace_context_for_ti(ti)
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
                        outcome.synthesized_error or RuntimeError(f"Task {task_instance_label(ti) or ti.task_id} failed."),
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
    task_order = topological_task_order(dag)

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
            return _StrictTaskOutcome(failed=False, return_value=best_effort_task_result(ti))

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
    task_order = topological_task_order(dag)

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
            return _StrictTaskOutcome(failed=False, return_value=best_effort_task_result(ti))

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
    task_order = topological_task_order(dag)

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
            return _StrictTaskOutcome(failed=False, return_value=best_effort_task_result(ti))

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


def strict_dag_test(
    dag: Any,
    *,
    execution_date: datetime | None,
    run_conf: dict[str, Any] | None,
    trace_session: Any | None = None,
) -> Any:
    """Run a strict local dag.test against the installed Airflow version."""
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
