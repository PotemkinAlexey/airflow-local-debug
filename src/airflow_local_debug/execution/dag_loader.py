"""
DAG file import and DAG-object discovery.

Loading user DAG files via ``importlib`` is fiddly enough to deserve its own
module. The MD5-of-path-based module name keeps cache hits stable when the
same file is loaded from multiple call sites within a single process.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from types import ModuleType
from typing import Any

from airflow_local_debug._module_loading import load_python_module
from airflow_local_debug.models import DagFileInfo


def load_module_from_file(path: str) -> ModuleType:
    """Import a Python file by absolute path and return the resulting module."""
    return load_python_module(
        path,
        module_prefix="airflow_debug_dag",
        missing_message="DAG file not found: {path}",
        import_error_message="Unable to load DAG module from {path}",
    )


def dag_candidates_from_module(module: ModuleType) -> list[Any]:
    """Return module-level objects that look like Airflow DAGs (sorted by dag_id)."""
    candidates: list[Any] = []
    seen: set[int] = set()
    for value in module.__dict__.values():
        if isinstance(value, type):
            continue
        if hasattr(value, "dag_id") and hasattr(value, "task_dict"):
            if id(value) in seen:
                continue
            seen.add(id(value))
            candidates.append(value)
    candidates.sort(key=lambda dag: str(getattr(dag, "dag_id", "<unknown>")))
    return candidates


def dag_file_info(dag: Any) -> DagFileInfo:
    """Build a `DagFileInfo` snapshot for one DAG object."""
    task_dict = getattr(dag, "task_dict", None)
    tasks = getattr(dag, "tasks", None)
    if task_dict:
        task_count = len(task_dict)
    elif tasks:
        task_count = len(tasks)
    else:
        task_count = 0
    fileloc = getattr(dag, "fileloc", None)
    return DagFileInfo(
        dag_id=str(getattr(dag, "dag_id", "<unknown>")),
        task_count=task_count,
        fileloc=str(fileloc) if fileloc else None,
    )


def format_dag_list(infos: Iterable[DagFileInfo], *, source_path: str | None = None) -> str:
    """Render a `DAGs in /path` listing for `--list-dags` output."""
    rows = list(infos)
    heading = "DAGs"
    if source_path:
        heading += f" in {Path(source_path).expanduser().resolve()}"
    lines = [heading]
    if not rows:
        lines.append("<none>")
        return "\n".join(lines)

    for info in rows:
        task_label = "task" if info.task_count == 1 else "tasks"
        lines.append(f"- {info.dag_id} ({info.task_count} {task_label})")
    return "\n".join(lines)


def resolve_dag_from_module(module: ModuleType, dag_id: str | None = None) -> Any:
    """Pick a DAG object from a module, optionally filtered by `dag_id`."""
    candidates = dag_candidates_from_module(module)

    if dag_id:
        for dag in candidates:
            if getattr(dag, "dag_id", None) == dag_id:
                return dag
        raise ValueError(f"DAG with dag_id='{dag_id}' not found in module {module.__name__}")

    if len(candidates) == 1:
        return candidates[0]

    if not candidates:
        raise ValueError(f"No DAG objects found in module {module.__name__}")

    dag_ids = ", ".join(sorted(getattr(dag, "dag_id", "<unknown>") for dag in candidates))
    raise ValueError(f"Multiple DAG objects found in module {module.__name__}: {dag_ids}. Pass dag_id explicitly.")
