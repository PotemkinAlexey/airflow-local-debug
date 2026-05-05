from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

import pytest

from airflow_local_debug import runner
from airflow_local_debug.models import RunResult, TaskRunInfo


# --- helpers --------------------------------------------------------------


@dataclass
class FakeTask:
    task_id: str
    upstream_task_ids: set[str] = field(default_factory=set)
    downstream_task_ids: set[str] = field(default_factory=set)


@dataclass
class FakeDag:
    task_dict: dict[str, FakeTask]
    dag_id: str = "demo"


def _link(parent: FakeTask, child: FakeTask) -> None:
    parent.downstream_task_ids.add(child.task_id)
    child.upstream_task_ids.add(parent.task_id)


@dataclass
class FakeTI:
    task_id: str
    state: Any = None
    map_index: int | None = -1


class FakeDagrun:
    def __init__(self, tis: list[FakeTI]) -> None:
        self._tis = tis

    def get_task_instances(self) -> list[FakeTI]:
        return list(self._tis)


# --- _serialize_datetime --------------------------------------------------


def test_serialize_datetime_handles_none_and_iso() -> None:
    assert runner._serialize_datetime(None) is None
    assert runner._serialize_datetime(datetime(2026, 1, 1, 12, 0)) == "2026-01-01T12:00:00"


def test_serialize_datetime_falls_back_to_str_for_unsupported() -> None:
    class WeirdValue:
        def isoformat(self) -> str:
            raise RuntimeError("nope")

        def __str__(self) -> str:  # noqa: D401
            return "weird"

    assert runner._serialize_datetime(WeirdValue()) == "weird"


# --- _coerce_logical_date -------------------------------------------------


def test_coerce_logical_date_passthrough_and_iso() -> None:
    assert runner._coerce_logical_date(None) is None
    moment = datetime(2026, 5, 5, 9, 30)
    assert runner._coerce_logical_date(moment) is moment
    assert runner._coerce_logical_date(date(2026, 5, 5)) == datetime(2026, 5, 5)


def test_coerce_logical_date_parses_date_only_string() -> None:
    assert runner._coerce_logical_date("2026-05-05") == datetime(2026, 5, 5)


def test_coerce_logical_date_parses_iso_string() -> None:
    assert runner._coerce_logical_date("2026-05-05T11:00:00") == datetime(2026, 5, 5, 11, 0)


def test_coerce_logical_date_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        runner._coerce_logical_date("not a date at all")


# --- _state_token / _task_state_buckets -----------------------------------


def test_state_token_lowercases_and_handles_none() -> None:
    assert runner._state_token(None) is None
    assert runner._state_token("  FAILED ") == "failed"
    assert runner._state_token("") is None


def test_task_state_buckets_splits_failed_and_unfinished() -> None:
    tasks = [
        TaskRunInfo(task_id="a", state="success"),
        TaskRunInfo(task_id="b", state="failed"),
        TaskRunInfo(task_id="c", state="upstream_failed"),
        TaskRunInfo(task_id="d", state="scheduled"),
        TaskRunInfo(task_id="e", state="up_for_retry"),
    ]

    failed, unfinished = runner._task_state_buckets(tasks)

    assert {task.task_id for task in failed} == {"b", "c", "e"}
    assert {task.task_id for task in unfinished} == {"d"}


# --- _task_instance_label -------------------------------------------------


def test_task_instance_label_handles_map_index() -> None:
    assert runner._task_instance_label(FakeTI(task_id="a")) == "a"
    assert runner._task_instance_label(FakeTI(task_id="a", map_index=2)) == "a[2]"
    assert runner._task_instance_label(FakeTI(task_id="")) is None


# --- _failed_task_label ---------------------------------------------------


def test_failed_task_label_returns_none_when_nothing_failed() -> None:
    dagrun = FakeDagrun([FakeTI(task_id="a", state="success")])
    assert runner._failed_task_label(dagrun) is None


def test_failed_task_label_collects_multiple_with_truncation() -> None:
    dagrun = FakeDagrun(
        [
            FakeTI(task_id="a", state="failed"),
            FakeTI(task_id="b", state="failed"),
            FakeTI(task_id="c", state="up_for_retry"),
            FakeTI(task_id="d", state="failed"),
        ]
    )
    label = runner._failed_task_label(dagrun)
    assert label is not None
    # First three are joined alphabetically; the rest is summarized.
    assert label.startswith("a, b, c") and "..." in label


# --- _normalize_task_states_for_backend -----------------------------------


def test_normalize_marks_downstream_as_upstream_failed_in_strict_mode() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    c = FakeTask("c")
    _link(a, b)
    _link(b, c)
    dag = FakeDag(task_dict={"a": a, "b": b, "c": c})
    tasks = [
        TaskRunInfo(task_id="a", state="failed"),
        TaskRunInfo(task_id="b", state="scheduled"),
        TaskRunInfo(task_id="c", state="scheduled"),
    ]

    normalized = runner._normalize_task_states_for_backend(dag, tasks, backend="dag.test.strict")

    states = {task.task_id: task.state for task in normalized}
    assert states == {"a": "failed", "b": "upstream_failed", "c": "upstream_failed"}


def test_normalize_marks_unrelated_unfinished_as_not_run() -> None:
    a = FakeTask("a")
    b = FakeTask("b")  # independent, not downstream of failed
    dag = FakeDag(task_dict={"a": a, "b": b})
    tasks = [
        TaskRunInfo(task_id="a", state="failed"),
        TaskRunInfo(task_id="b", state="scheduled"),
    ]

    normalized = runner._normalize_task_states_for_backend(dag, tasks, backend="dag.test.strict")
    states = {task.task_id: task.state for task in normalized}

    assert states == {"a": "failed", "b": "not_run"}


def test_normalize_is_noop_for_non_strict_backend() -> None:
    dag = FakeDag(task_dict={"a": FakeTask("a")})
    tasks = [TaskRunInfo(task_id="a", state="failed")]
    assert runner._normalize_task_states_for_backend(dag, tasks, backend="dag.test") == tasks


# --- _normalize_result ----------------------------------------------------


def test_normalize_result_sets_failed_state_and_exception() -> None:
    result = RunResult(
        dag_id="demo",
        state="success",  # incorrectly success even though tasks failed
        tasks=[TaskRunInfo(task_id="a", state="failed")],
    )
    out = runner._normalize_result(result)
    assert out.state == "failed"
    assert out.exception is not None
    assert "failed or retry-pending" in out.exception


def test_normalize_result_handles_enum_repr_state() -> None:
    # Even when state arrives as the enum repr, a failed task forces "failed".
    result = RunResult(
        dag_id="demo",
        state="DagRunState.SUCCESS",
        tasks=[TaskRunInfo(task_id="a", state="failed")],
    )
    out = runner._normalize_result(result)
    assert out.state == "failed"


def test_normalize_result_marks_incomplete_when_only_unfinished() -> None:
    result = RunResult(
        dag_id="demo",
        state="running",
        tasks=[TaskRunInfo(task_id="a", state="scheduled")],
    )
    out = runner._normalize_result(result)
    assert out.state == "incomplete"
    assert out.exception is not None


def test_normalize_result_keeps_success_when_all_tasks_done() -> None:
    result = RunResult(
        dag_id="demo",
        state="success",
        tasks=[TaskRunInfo(task_id="a", state="success")],
    )
    out = runner._normalize_result(result)
    assert out.state == "success"
    assert out.exception is None


# --- _backend_hint --------------------------------------------------------


def test_backend_hint_picks_strict_when_test_available_and_fail_fast() -> None:
    class HasTest:
        def test(self) -> None: ...

    assert runner._backend_hint(HasTest(), fail_fast=True) == "dag.test.strict"
    assert runner._backend_hint(HasTest(), fail_fast=False) == "dag.test"


def test_backend_hint_falls_back_to_dag_run_or_unsupported() -> None:
    class HasRun:
        def run(self) -> None: ...

    class HasNothing: ...

    assert runner._backend_hint(HasRun(), fail_fast=False) == "dag.run"
    assert runner._backend_hint(HasNothing(), fail_fast=False) == "unsupported"
