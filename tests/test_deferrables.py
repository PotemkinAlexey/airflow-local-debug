from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from airflow_local_debug.execution.deferrables import detect_deferrable_tasks, format_deferrable_note


@dataclass
class FakeTask:
    task_id: str
    task_type: str = "FakeOperator"
    deferrable: bool = False
    start_from_trigger: bool = False
    start_trigger_args: Any = None


@dataclass
class FakeDag:
    task_dict: dict[str, FakeTask]


def test_detect_deferrable_tasks_by_deferrable_attr() -> None:
    dag = FakeDag(
        task_dict={
            "wait": FakeTask("wait", task_type="ExternalTaskSensor", deferrable=True),
            "regular": FakeTask("regular"),
        }
    )

    infos = detect_deferrable_tasks(dag, backend_hint="dag.test.strict")

    assert len(infos) == 1
    assert infos[0].task_id == "wait"
    assert infos[0].operator == "ExternalTaskSensor"
    assert infos[0].local_mode == "inline-trigger"
    assert "Strict local mode" in (infos[0].reason or "")


def test_detect_deferrable_tasks_by_start_trigger_args() -> None:
    dag = FakeDag(
        task_dict={
            "asset": FakeTask(
                "asset",
                start_from_trigger=True,
                start_trigger_args={"trigger_cls": "airflow.triggers.temporal.DateTimeTrigger"},
            )
        }
    )

    infos = detect_deferrable_tasks(dag, backend_hint="dag.test")

    assert infos[0].trigger == "airflow.triggers.temporal.DateTimeTrigger"
    assert infos[0].local_mode == "native-dag-test"


def test_format_deferrable_note_summarizes_local_mode() -> None:
    infos = detect_deferrable_tasks(
        FakeDag(task_dict={"wait": FakeTask("wait", deferrable=True)}),
        backend_hint="dag.run",
    )

    note = format_deferrable_note(infos)

    assert note is not None
    assert "wait (FakeOperator, mode=legacy-dag-run)" in note
    assert "prefer fail_fast=True" in note


def test_format_deferrable_note_handles_empty() -> None:
    assert format_deferrable_note([]) is None
