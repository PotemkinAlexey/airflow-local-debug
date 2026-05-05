from __future__ import annotations

import inspect
from datetime import datetime
from typing import Any


def get_airflow_version() -> str | None:
    try:
        import airflow
    except Exception:
        return None
    return getattr(airflow, "__version__", None)


def has_dag_test(dag: Any) -> bool:
    return hasattr(dag, "test") and callable(getattr(dag, "test", None))


def build_dag_test_kwargs(
    dag: Any,
    logical_date: datetime | None,
    conf: dict[str, Any] | None,
) -> dict[str, Any]:
    """
    Build kwargs for `dag.test(...)` across Airflow versions.

    Airflow 2.x exposed both `execution_date` and `logical_date`; Airflow 3
    keeps only `logical_date`. When the signature is unreadable we prefer the
    modern name — passing `execution_date` to Airflow 3 raises TypeError.
    """
    kwargs: dict[str, Any] = {}
    try:
        sig = inspect.signature(dag.test)
    except Exception:
        sig = None

    if logical_date is not None:
        if sig and "logical_date" in sig.parameters:
            kwargs["logical_date"] = logical_date
        elif sig and "execution_date" in sig.parameters:
            kwargs["execution_date"] = logical_date
        else:
            kwargs["logical_date"] = logical_date

    if conf:
        if sig and "run_conf" in sig.parameters:
            kwargs["run_conf"] = conf
        elif sig and "conf" in sig.parameters:
            kwargs["conf"] = conf
        else:
            kwargs["conf"] = conf

    return kwargs


def build_legacy_dag_run_kwargs(
    dag: Any,
    logical_date: datetime | None,
    conf: dict[str, Any] | None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    try:
        sig = inspect.signature(dag.run)
    except Exception:
        sig = None

    if logical_date is not None:
        if sig and "start_date" in sig.parameters:
            kwargs["start_date"] = logical_date
        if sig and "end_date" in sig.parameters:
            kwargs["end_date"] = logical_date
        elif sig and "execution_date" in sig.parameters:
            kwargs["execution_date"] = logical_date

    if conf:
        if sig and "conf" in sig.parameters:
            kwargs["conf"] = conf
        elif sig and "run_conf" in sig.parameters:
            kwargs["run_conf"] = conf

    return kwargs
