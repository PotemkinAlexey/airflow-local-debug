from __future__ import annotations

from airflow_local_debug.models import RunResult


def format_run_report(result: RunResult, *, include_graph: bool = False) -> str:
    lines = [
        f"DAG: {result.dag_id}",
        f"State: {result.state or 'unknown'}",
    ]

    if result.backend:
        lines.append(f"Backend: {result.backend}")
    if result.airflow_version:
        lines.append(f"Airflow: {result.airflow_version}")
    if result.logical_date:
        lines.append(f"Logical date: {result.logical_date}")
    if result.config_path:
        lines.append(f"Config: {result.config_path}")
    if result.graph_svg_path:
        lines.append(f"Graph SVG: {result.graph_svg_path}")

    if include_graph and result.graph_ascii:
        lines.append("")
        lines.append(result.graph_ascii)

    if result.notes:
        lines.append("Notes:")
        for note in result.notes:
            lines.append(f"- {note}")

    if result.tasks:
        lines.append("Tasks:")
        for task in result.tasks:
            map_suffix = ""
            if task.map_index is not None and task.map_index >= 0:
                map_suffix = f"[{task.map_index}]"
            lines.append(f"- {task.task_id}{map_suffix}: {task.state or 'unknown'}")

    if result.exception and not result.exception_was_logged:
        lines.append("Exception:")
        lines.append(result.exception.rstrip())

    return "\n".join(lines)


def print_run_report(result: RunResult, *, include_graph: bool = False) -> None:
    print(format_run_report(result, include_graph=include_graph))
