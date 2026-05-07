"""
Deterministic topological ordering for DAG tasks.

Used by the runner (for task scheduling) and by the graph renderer (for
column placement). Ordering is stable: tasks at the same depth are sorted
by `task_id` so that two runs of the same DAG produce identical output.
"""

from __future__ import annotations

from typing import Any


def downstream_task_ids(dag: Any, roots: set[str]) -> set[str]:
    """Return task ids transitively downstream of ``roots`` (excluding the roots).

    Walks ``downstream_task_ids`` edges via BFS. Tasks not present in
    ``dag.task_dict`` are ignored. Used for partial-run selection and
    upstream-failure propagation.
    """
    task_dict = dict(getattr(dag, "task_dict", {}) or {})
    pending = list(sorted(root for root in roots if root in task_dict))
    seen: set[str] = set()
    while pending:
        task_id = pending.pop(0)
        downstream = set(getattr(task_dict[task_id], "downstream_task_ids", set()) or set()) & set(task_dict)
        for child_id in sorted(downstream):
            if child_id in roots or child_id in seen:
                continue
            seen.add(child_id)
            pending.append(child_id)
    return seen


def topological_task_order(dag: Any) -> dict[str, int]:
    """
    Return a mapping `task_id -> ordinal index` in topological order.

    Tasks in cycles or detached from the main graph fall to the tail in
    sorted-by-id order. The result is a dict so callers can use it as a
    lookup table for sorting task instances.
    """
    task_dict = dict(getattr(dag, "task_dict", {}) or {})
    if not task_dict:
        return {}

    task_ids = set(task_dict)
    remaining_upstream: dict[str, int] = {}
    downstream_map: dict[str, list[str]] = {}
    for task_id, task in task_dict.items():
        upstream_ids = set(getattr(task, "upstream_task_ids", set()) or set()) & task_ids
        remaining_upstream[task_id] = len(upstream_ids)
        downstream_map[task_id] = sorted(
            child_id for child_id in (getattr(task, "downstream_task_ids", set()) or set()) if child_id in task_ids
        )

    ready = sorted(task_id for task_id, count in remaining_upstream.items() if count == 0)
    ordered: list[str] = []
    while ready:
        task_id = ready.pop(0)
        ordered.append(task_id)
        for child_id in downstream_map.get(task_id, []):
            remaining_upstream[child_id] -= 1
            if remaining_upstream[child_id] == 0:
                ready.append(child_id)
        ready.sort()

    for task_id in sorted(task_ids):
        if task_id not in ordered:
            ordered.append(task_id)
    return {task_id: index for index, task_id in enumerate(ordered)}


def topological_task_ids(tasks: list[Any]) -> list[str]:
    """List variant for callers that work with a flat list of task objects."""
    task_dict: dict[str, Any] = {task.task_id: task for task in tasks}
    if not task_dict:
        return []

    task_ids = set(task_dict)
    remaining_upstream: dict[str, int] = {}
    downstream_map: dict[str, list[str]] = {}
    for task_id, task in task_dict.items():
        upstream_ids = set(getattr(task, "upstream_task_ids", set()) or set()) & task_ids
        remaining_upstream[task_id] = len(upstream_ids)
        downstream_map[task_id] = sorted(
            child_id for child_id in (getattr(task, "downstream_task_ids", set()) or set()) if child_id in task_ids
        )

    ready = sorted(task_id for task_id, count in remaining_upstream.items() if count == 0)
    ordered: list[str] = []
    while ready:
        task_id = ready.pop(0)
        ordered.append(task_id)
        for child_id in downstream_map.get(task_id, []):
            remaining_upstream[child_id] -= 1
            if remaining_upstream[child_id] == 0:
                ready.append(child_id)
        ready.sort()

    for task_id in sorted(task_ids):
        if task_id not in ordered:
            ordered.append(task_id)
    return ordered
