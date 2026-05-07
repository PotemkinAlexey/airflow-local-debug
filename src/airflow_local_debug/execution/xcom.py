"""
XCom collection: query the metadata DB plus a per-TI xcom_pull fallback.

The two-pass strategy exists because some local-only XCom values are visible
through `ti.xcom_pull` but not yet flushed into the XCom table at the moment
the runner inspects the dagrun.
"""

from __future__ import annotations

import json
import logging
import warnings
from typing import Any

from airflow_local_debug.execution.state import best_effort_task_result

_log = logging.getLogger(__name__)


def json_safe(value: Any) -> Any:
    """Coerce a value into a JSON-serializable shape, str()-ing anything exotic."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
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


def task_xcom_label(task_id: str, map_index: int | None) -> str:
    """`task_id[map_index]` for mapped instances, else just `task_id`."""
    if map_index is not None and map_index >= 0:
        return f"{task_id}[{map_index}]"
    return task_id


def extract_xcoms(dagrun: Any, dag: Any) -> dict[str, dict[str, Any]]:
    """Collect XComs from this dagrun: DB query first, then xcom_pull fallback."""
    if dagrun is None:
        return {}

    snapshot = query_xcoms(dagrun, dag)
    fallback = fallback_return_xcoms(dagrun)
    for label, values in fallback.items():
        snapshot.setdefault(label, {}).update(
            {key: value for key, value in values.items() if key not in snapshot.get(label, {})}
        )
    return snapshot


def query_xcoms(dagrun: Any, dag: Any) -> dict[str, dict[str, Any]]:
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
        snapshot.setdefault(task_xcom_label(task_id, map_index), {})[key] = json_safe(getattr(row, "value", None))
    return snapshot


def fallback_return_xcoms(dagrun: Any) -> dict[str, dict[str, Any]]:
    if not hasattr(dagrun, "get_task_instances"):
        return {}

    snapshot: dict[str, dict[str, Any]] = {}
    for ti in dagrun.get_task_instances():
        value = best_effort_task_result(ti)
        if value is None:
            continue
        task_id = str(getattr(ti, "task_id", "<unknown>"))
        map_index = getattr(ti, "map_index", None)
        snapshot[task_xcom_label(task_id, map_index)] = {"return_value": json_safe(value)}
    return snapshot
