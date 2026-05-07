"""
Partial DAG run selection: --task / --start-task / --task-group resolution
plus the "external upstream" detection that warns when the selected subgraph
has unmet upstream XCom dependencies.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from airflow_local_debug.topology import downstream_task_ids, topological_task_order


def task_group_path(task: Any) -> str | None:
    """Return the task's TaskGroup id (full dotted path), or None for top-level."""
    task_group = getattr(task, "task_group", None)
    if task_group is None:
        return None
    group_id = getattr(task_group, "group_id", None)
    if group_id is None:
        return None
    text = str(group_id).strip()
    return text or None


def task_group_path_contains(group_path: str | None, requested_group: str) -> bool:
    """True when ``group_path`` is exactly ``requested_group`` or a nested child of it."""
    if group_path is None:
        return False
    return group_path == requested_group or group_path.startswith(f"{requested_group}.")


def available_task_group_ids(dag: Any) -> list[str]:
    """All TaskGroup ids that exist in this DAG, including nested ancestors."""
    groups: set[str] = set()
    for task in list(getattr(dag, "task_dict", {}).values()):
        path = task_group_path(task)
        while path:
            groups.add(path)
            parent, _, child = path.rpartition(".")
            if not parent or not child:
                break
            path = parent
    return sorted(groups)


def format_available(values: Iterable[str], *, limit: int = 10) -> str:
    rows = sorted(values)
    if not rows:
        return "<none>"
    suffix = "" if len(rows) <= limit else f", ... +{len(rows) - limit} more"
    return ", ".join(rows[:limit]) + suffix


def resolve_partial_task_ids(
    dag: Any,
    *,
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
) -> list[str] | None:
    """Resolve `--task` / `--start-task` / `--task-group` to concrete task ids.

    Returns None when no selector is provided. Raises ``ValueError`` for
    unknown task ids or unknown task group ids. The returned list is sorted
    in topological order, matching the order in which tasks would normally
    execute.
    """
    exact_ids = [str(task_id).strip() for task_id in task_ids or [] if str(task_id).strip()]
    start_ids = [str(task_id).strip() for task_id in start_task_ids or [] if str(task_id).strip()]
    group_ids = [str(group_id).strip() for group_id in task_group_ids or [] if str(group_id).strip()]
    if not exact_ids and not start_ids and not group_ids:
        return None

    task_dict = dict(getattr(dag, "task_dict", {}) or {})
    if not task_dict:
        raise ValueError("Cannot select tasks from a DAG with no tasks.")

    available_ids = set(task_dict)
    missing_ids = sorted((set(exact_ids) | set(start_ids)) - available_ids)
    if missing_ids:
        raise ValueError(
            "Unknown task id(s) for partial run: "
            f"{', '.join(missing_ids)}. Available task ids: {format_available(available_ids)}."
        )

    selected: set[str] = set(exact_ids)
    if start_ids:
        roots = set(start_ids)
        selected.update(roots)
        selected.update(downstream_task_ids(dag, roots))

    for group_id in group_ids:
        matching = {
            task_id
            for task_id, task in task_dict.items()
            if task_group_path_contains(task_group_path(task), group_id)
        }
        if not matching:
            raise ValueError(
                "Unknown task group id for partial run: "
                f"{group_id}. Available task groups: {format_available(available_task_group_ids(dag))}."
            )
        selected.update(matching)

    order = topological_task_order(dag)
    return sorted(selected, key=lambda task_id: (order.get(task_id, 10**9), task_id))


def build_partial_selection_note(dag: Any, selected_task_ids: list[str]) -> str:
    total = len(getattr(dag, "task_dict", {}) or {})
    selected = len(selected_task_ids)
    return (
        f"Partial DAG run selected {selected}/{total} task(s): "
        f"{format_available(selected_task_ids, limit=12)}."
    )


def detect_external_upstreams(dag: Any, selected_task_ids: list[str]) -> dict[str, list[str]]:
    """For each selected task, return upstream task ids that are NOT in the selection.

    Must be called against the original (unpartitioned) DAG so the upstream
    edges are still intact.
    """
    selected = set(selected_task_ids)
    task_dict = dict(getattr(dag, "task_dict", {}) or {})
    external: dict[str, list[str]] = {}
    for task_id in selected_task_ids:
        task = task_dict.get(task_id)
        if task is None:
            continue
        upstream_ids = set(getattr(task, "upstream_task_ids", set()) or set())
        unmet = sorted(upstream_ids - selected)
        if unmet:
            external[task_id] = unmet
    return external


def format_external_upstream_note(external: dict[str, list[str]]) -> str:
    pairs = [f"{task_id} <- {', '.join(external[task_id])}" for task_id in sorted(external)]
    head = "; ".join(pairs[:5])
    suffix = f"; ... +{len(pairs) - 5} more" if len(pairs) > 5 else ""
    return (
        "Partial run skips upstream task(s) that selected task(s) depend on: "
        f"{head}{suffix}. XCom pulls from these upstreams will return None. "
        "Provide --mock-file to inject upstream XCom values, or include the upstream "
        "chain via additional --task / --start-task selectors."
    )


def partial_dag_for_selected_tasks(dag: Any, selected_task_ids: list[str]) -> Any:
    partial_subset = getattr(dag, "partial_subset", None)
    if not callable(partial_subset):
        raise ValueError("This Airflow DAG object does not support partial task selection.")
    return partial_subset(selected_task_ids, include_upstream=False, include_downstream=False)
