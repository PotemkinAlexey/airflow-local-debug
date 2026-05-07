"""
Leaf helpers for task / dagrun state inspection.

Lives in its own module so both `runner` and `execution` can import these
without circular dependency. Contains nothing that pulls in Airflow imports.
"""

from __future__ import annotations

from typing import Any

FAILED_TASK_STATES = {"failed", "up_for_retry", "upstream_failed", "shutdown"}
"""States that mean a task did not succeed for any reason, including upstream failures."""

HARD_FAILED_TASK_STATES = {"failed", "up_for_retry", "shutdown"}
"""States that mean the task itself broke. Excludes ``upstream_failed`` because
that state describes a downstream task whose ancestor failed — re-running such
a task is pointless without first re-running the actual failure source."""

UNFINISHED_TASK_STATES = {
    None,
    "none",
    "scheduled",
    "queued",
    "running",
    "restarting",
    "deferred",
    "up_for_reschedule",
}


def state_token(state: Any) -> str | None:
    """Lowercased state string, or None if blank."""
    if state is None:
        return None
    return str(state).strip().lower() or None


def task_instance_label(ti: Any) -> str | None:
    """Render a `task_id[map_index]` label, or `task_id` if not mapped."""
    task_id = getattr(ti, "task_id", None)
    if not task_id:
        return None
    map_index = getattr(ti, "map_index", None)
    if map_index is not None and map_index >= 0:
        return f"{task_id}[{map_index}]"
    return str(task_id)


def trace_context_for_ti(ti: Any) -> dict[str, Any]:
    """Minimal context dict for plugin trace hooks."""
    return {
        "ti": ti,
        "task_instance": ti,
        "run_id": getattr(ti, "run_id", None),
    }


def serialize_datetime(value: Any) -> str | None:
    """Render a datetime-ish value as an ISO string, falling back to str()."""
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return str(value.isoformat())
        except Exception:
            return str(value)
    return str(value)


def duration_seconds(start: Any, end: Any) -> float | None:
    """Compute (end - start).total_seconds() defensively."""
    if start is None or end is None:
        return None
    try:
        seconds = (end - start).total_seconds()
    except Exception:
        return None
    if seconds < 0:
        return None
    return round(float(seconds), 6)


def best_effort_task_result(ti: Any) -> Any:
    """Pull this TI's `return_value` XCom; tolerate API variants."""
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
