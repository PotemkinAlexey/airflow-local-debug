from __future__ import annotations

from datetime import datetime

from airflow_local_debug.compat import (
    build_dag_test_kwargs,
    build_legacy_dag_run_kwargs,
    has_dag_test,
)


class _AirflowTwoDag:
    def test(self, execution_date=None, logical_date=None, run_conf=None):  # noqa: D401
        return None

    def run(self, start_date=None, end_date=None, conf=None):  # noqa: D401
        return None


class _AirflowThreeDag:
    def test(self, logical_date=None, conf=None):  # noqa: D401
        return None


class _DagWithoutInspectableSignature:
    # `signature` will fail because `test` is a builtin descriptor here.
    test = staticmethod(lambda *args, **kwargs: None)


MOMENT = datetime(2026, 5, 5)


def test_has_dag_test_detects_callable() -> None:
    assert has_dag_test(_AirflowTwoDag()) is True

    class NoTest: ...

    assert has_dag_test(NoTest()) is False


def test_build_dag_test_kwargs_prefers_logical_date_for_airflow_three() -> None:
    kwargs = build_dag_test_kwargs(_AirflowThreeDag(), MOMENT, {"foo": 1})
    assert kwargs == {"logical_date": MOMENT, "conf": {"foo": 1}}


def test_build_dag_test_kwargs_uses_run_conf_when_supported() -> None:
    kwargs = build_dag_test_kwargs(_AirflowTwoDag(), MOMENT, {"foo": 1})
    assert kwargs["run_conf"] == {"foo": 1}
    # Airflow 2 prefers logical_date when both are available.
    assert kwargs["logical_date"] == MOMENT


def test_build_dag_test_kwargs_fallback_uses_logical_date_not_execution_date() -> None:
    kwargs = build_dag_test_kwargs(_DagWithoutInspectableSignature(), MOMENT, None)
    # Critical: never default to execution_date — it is removed in Airflow 3.
    assert "logical_date" in kwargs
    assert "execution_date" not in kwargs


def test_build_legacy_dag_run_kwargs_uses_start_and_end() -> None:
    kwargs = build_legacy_dag_run_kwargs(_AirflowTwoDag(), MOMENT, {"foo": 1})
    assert kwargs["start_date"] == MOMENT
    assert kwargs["end_date"] == MOMENT
    assert kwargs["conf"] == {"foo": 1}
