from __future__ import annotations

from airflow_local_debug.models import RunResult


def test_run_result_ok_requires_success_state() -> None:
    assert RunResult(dag_id="d", state="success").ok is True


def test_run_result_ok_false_when_state_missing() -> None:
    assert RunResult(dag_id="d").ok is False
    assert RunResult(dag_id="d", state="running").ok is False
    assert RunResult(dag_id="d", state="incomplete").ok is False


def test_run_result_ok_false_when_exception_present() -> None:
    assert RunResult(dag_id="d", state="success", exception="boom").ok is False


def test_run_result_ok_handles_enum_repr_state() -> None:
    # Some Airflow/Python combinations produce "DagRunState.SUCCESS" via str().
    assert RunResult(dag_id="d", state="DagRunState.SUCCESS").ok is True
    assert RunResult(dag_id="d", state="DagRunState.FAILED").ok is False
