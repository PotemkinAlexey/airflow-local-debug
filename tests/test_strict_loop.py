from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from airflow_local_debug.execution import strict_loop


@dataclass
class FakeTask:
    task_id: str


@dataclass(eq=False)
class FakeTaskInstance:
    task_id: str
    state: str
    try_number: int = 0
    map_index: int = -1
    scheduled_dttm: object | None = None
    task: FakeTask | None = None
    log: Any = None


class FakeLogger:
    def __init__(self) -> None:
        self.handlers: list[logging.Handler] = []
        self.warnings: list[tuple[str, tuple[Any, ...]]] = []
        self.exceptions: list[tuple[str, tuple[Any, ...]]] = []

    def addHandler(self, handler: logging.Handler) -> None:  # noqa: N802 - mimic logging.Logger API
        self.handlers.append(handler)

    def warning(self, message: str, *args: Any) -> None:
        self.warnings.append((message, args))

    def exception(self, message: str, *args: Any) -> None:
        self.exceptions.append((message, args))


@dataclass
class FakeDag:
    task_dict: dict[str, FakeTask]
    log: FakeLogger = field(default_factory=FakeLogger)


class FakeSession:
    def __init__(self) -> None:
        self.commits = 0
        self.expire_all_calls = 0
        self.merges: list[Any] = []
        self.refresh_calls: list[Any] = []

    def expire_all(self) -> None:
        self.expire_all_calls += 1

    def commit(self) -> None:
        self.commits += 1

    def merge(self, value: Any) -> Any:
        self.merges.append(value)
        return value

    def refresh(self, value: Any) -> None:
        self.refresh_calls.append(value)


class FakeDagRun:
    def __init__(self, tis: list[FakeTaskInstance], scheduled_batches: list[list[FakeTaskInstance]]) -> None:
        self.state = "running"
        self.dag: Any = None
        self._tis = tis
        self._scheduled_batches = scheduled_batches
        self.update_state_calls = 0

    def update_state(self, *, session: FakeSession) -> tuple[list[FakeTaskInstance], None]:
        self.update_state_calls += 1
        if self._scheduled_batches:
            return self._scheduled_batches.pop(0), None
        return [], None

    def get_task_instances(self, *, session: FakeSession) -> list[FakeTaskInstance]:
        return list(self._tis)


class FakeTraceSession:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, Any]] = []

    def begin_task(self, task: FakeTask, context: dict[str, Any]) -> None:
        self.events.append(("begin", task.task_id, context["ti"]))

    def complete_task(self, task: FakeTask, context: dict[str, Any], return_value: Any) -> None:
        self.events.append(("complete", task.task_id, return_value))

    def fail_task(self, task: FakeTask, context: dict[str, Any], exc: BaseException) -> None:
        self.events.append(("fail", task.task_id, exc))


class FakeTimezone:
    marker = object()

    @staticmethod
    def utcnow() -> object:
        return FakeTimezone.marker


def _state_names() -> strict_loop._StrictStateNames:
    return strict_loop._StrictStateNames(
        DagRunState_RUNNING="running",
        TaskInstanceState_SCHEDULED="scheduled",
        TaskInstanceState_UP_FOR_RESCHEDULE="up_for_reschedule",
        State_finished={"success", "failed", "upstream_failed"},
    )


def test_add_logger_if_needed_attaches_stdout_handler_once() -> None:
    ti = FakeTaskInstance("extract", "scheduled", log=FakeLogger())

    strict_loop._add_logger_if_needed(ti)
    strict_loop._add_logger_if_needed(ti)

    assert len(ti.log.handlers) == 1
    assert isinstance(ti.log.handlers[0], logging.StreamHandler)


def test_refresh_task_instance_returns_original_on_refresh_error() -> None:
    class BrokenSession:
        def refresh(self, value: Any) -> None:
            raise RuntimeError("refresh failed")

    ti = FakeTaskInstance("extract", "failed")

    assert strict_loop._refresh_task_instance(BrokenSession(), ti) is ti
    assert strict_loop._is_failed_task_instance(ti) is True
    ti.state = "success"
    assert strict_loop._is_failed_task_instance(ti) is False


def test_airflow3_serialization_fileloc_restores_missing_attribute() -> None:
    class DagWithoutFileloc:
        pass

    dag = DagWithoutFileloc()

    with strict_loop._airflow3_serialization_fileloc(dag):
        assert Path(dag.fileloc).exists()

    assert not hasattr(dag, "fileloc")


def test_airflow3_serialization_fileloc_restores_existing_value(tmp_path: Path) -> None:
    class DagWithFileloc:
        fileloc = str(tmp_path / "missing.py")

    dag = DagWithFileloc()

    with strict_loop._airflow3_serialization_fileloc(dag):
        assert Path(dag.fileloc).exists()

    assert dag.fileloc == str(tmp_path / "missing.py")


def test_strict_scheduling_loop_runs_scheduled_tis_in_topological_order() -> None:
    tasks = {"a": FakeTask("a"), "b": FakeTask("b")}
    ti_b = FakeTaskInstance("b", "none", log=FakeLogger())
    ti_a = FakeTaskInstance("a", "none", log=FakeLogger())
    dag = FakeDag(tasks)
    dr = FakeDagRun([ti_b, ti_a], scheduled_batches=[[ti_b, ti_a]])
    session = FakeSession()
    trace = FakeTraceSession()
    calls: list[str] = []

    def run_scheduled_ti(ti: FakeTaskInstance, task: FakeTask) -> strict_loop._StrictTaskOutcome:
        calls.append(task.task_id)
        if calls == ["a", "b"]:
            dr.state = "success"
        return strict_loop._StrictTaskOutcome(failed=False, return_value=f"{task.task_id}-return")

    result = strict_loop._run_strict_scheduling_loop(
        dag,
        dr,
        session=session,
        dr_target_dag=dag,
        sets_scheduled_dttm=True,
        timezone_module=FakeTimezone,
        state_names=_state_names(),
        run_scheduled_ti=run_scheduled_ti,
        task_order={"a": 0, "b": 1},
        tasks=tasks,
        trace_session=trace,
    )

    assert result is dr
    assert calls == ["a", "b"]
    assert [ti_a.try_number, ti_b.try_number] == [1, 1]
    assert ti_a.scheduled_dttm is FakeTimezone.marker
    assert ti_b.scheduled_dttm is FakeTimezone.marker
    assert trace.events == [
        ("begin", "a", ti_a),
        ("complete", "a", "a-return"),
        ("begin", "b", ti_b),
        ("complete", "b", "b-return"),
    ]
    assert session.commits == 1


def test_strict_scheduling_loop_records_exception_and_returns_merged_dagrun() -> None:
    task = FakeTask("extract")
    ti = FakeTaskInstance("extract", "none", log=FakeLogger())
    dag = FakeDag({"extract": task})
    dr = FakeDagRun([ti], scheduled_batches=[[ti]])
    session = FakeSession()
    trace = FakeTraceSession()
    exc = ValueError("broken task")

    def run_scheduled_ti(ti: FakeTaskInstance, task: FakeTask) -> strict_loop._StrictTaskOutcome:
        raise exc

    result = strict_loop._run_strict_scheduling_loop(
        dag,
        dr,
        session=session,
        dr_target_dag=dag,
        sets_scheduled_dttm=False,
        timezone_module=FakeTimezone,
        state_names=_state_names(),
        run_scheduled_ti=run_scheduled_ti,
        task_order={"extract": 0},
        tasks={"extract": task},
        trace_session=trace,
    )

    assert result is dr
    assert dr._airflow_debug_local_exception is exc
    assert dr._airflow_debug_local_task_label == "extract"
    assert trace.events == [
        ("begin", "extract", ti),
        ("fail", "extract", exc),
    ]
    assert dag.log.exceptions == [("Task failed; ti=%s", (ti,))]
    assert session.merges == [dr]
    assert session.commits == 2


def test_strict_scheduling_loop_handles_failed_outcome_without_real_exception() -> None:
    task = FakeTask("extract")
    ti = FakeTaskInstance("extract", "scheduled", log=FakeLogger())
    dag = FakeDag({"extract": task})
    dr = FakeDagRun([ti], scheduled_batches=[[ti]])
    session = FakeSession()
    trace = FakeTraceSession()

    def run_scheduled_ti(ti: FakeTaskInstance, task: FakeTask) -> strict_loop._StrictTaskOutcome:
        ti.state = "failed"
        return strict_loop._StrictTaskOutcome(failed=True)

    result = strict_loop._run_strict_scheduling_loop(
        dag,
        dr,
        session=session,
        dr_target_dag=dag,
        sets_scheduled_dttm=False,
        timezone_module=FakeTimezone,
        state_names=_state_names(),
        run_scheduled_ti=run_scheduled_ti,
        task_order={"extract": 0},
        tasks={"extract": task},
        trace_session=trace,
    )

    assert result is dr
    assert trace.events[0] == ("begin", "extract", ti)
    event_type, task_id, synthesized = trace.events[1]
    assert (event_type, task_id) == ("fail", "extract")
    assert isinstance(synthesized, RuntimeError)
    assert str(synthesized) == "Task extract failed."
    assert session.merges == [dr]
    assert session.commits == 2


def test_strict_scheduling_loop_breaks_when_no_scheduled_tis_are_runnable() -> None:
    ti = FakeTaskInstance("extract", "none")
    dag = FakeDag({"extract": FakeTask("extract")})
    dr = FakeDagRun([ti], scheduled_batches=[[]])
    session = FakeSession()

    result = strict_loop._run_strict_scheduling_loop(
        dag,
        dr,
        session=session,
        dr_target_dag=dag,
        sets_scheduled_dttm=False,
        timezone_module=FakeTimezone,
        state_names=_state_names(),
        run_scheduled_ti=lambda ti, task: strict_loop._StrictTaskOutcome(failed=False),
        task_order={"extract": 0},
        tasks={"extract": FakeTask("extract")},
        trace_session=None,
    )

    assert result is dr
    assert dag.log.warnings == [("No tasks to run. unrunnable tasks: %s", ({ti},))]
