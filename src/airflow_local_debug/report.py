from __future__ import annotations

from collections import Counter
import csv
from dataclasses import asdict
import json
from pathlib import Path
from typing import Literal

from airflow_local_debug.models import RunResult

RunArtifactName = Literal["result", "report", "exception", "graph", "tasks"]


def _format_duration(seconds: float | int | None) -> str | None:
    if seconds is None:
        return None
    try:
        value = float(seconds)
    except (TypeError, ValueError):
        return None
    if value < 0:
        return None
    if value < 1:
        return f"{round(value * 1000):.0f}ms"
    if value < 60:
        if value.is_integer():
            return f"{value:.0f}s"
        return f"{value:.2f}s"

    total_seconds = int(round(value))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    return f"{minutes}m {seconds}s"


def _task_state_summary(result: RunResult) -> str | None:
    if not result.tasks:
        return None
    counts = Counter(task.state or "unknown" for task in result.tasks)
    return ", ".join(f"{state}={counts[state]}" for state in sorted(counts))


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
        state_summary = _task_state_summary(result)
        if state_summary:
            lines.append(f"Task summary: {state_summary}")
        lines.append("Tasks:")
        for task in result.tasks:
            map_suffix = ""
            if task.map_index is not None and task.map_index >= 0:
                map_suffix = f"[{task.map_index}]"
            duration = _format_duration(task.duration_seconds)
            duration_suffix = f" ({duration})" if duration else ""
            lines.append(f"- {task.task_id}{map_suffix}: {task.state or 'unknown'}{duration_suffix}")

    if result.exception and not result.exception_was_logged:
        lines.append("Exception:")
        lines.append(result.exception.rstrip())

    return "\n".join(lines)


def print_run_report(result: RunResult, *, include_graph: bool = False) -> None:
    print(format_run_report(result, include_graph=include_graph))


def write_run_artifacts(
    result: RunResult,
    report_dir: str | Path,
    *,
    include_graph: bool = False,
) -> dict[RunArtifactName, Path]:
    target_dir = Path(report_dir).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)
    target_dir = target_dir.resolve()

    artifacts: dict[RunArtifactName, Path] = {}

    result_path = target_dir / "result.json"
    result_path.write_text(
        json.dumps(asdict(result), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["result"] = result_path

    report_path = target_dir / "report.md"
    report_path.write_text(
        format_run_report(result, include_graph=include_graph) + "\n",
        encoding="utf-8",
    )
    artifacts["report"] = report_path

    exception_text = result.exception_raw or result.exception
    if exception_text:
        exception_path = target_dir / "exception.txt"
        exception_path.write_text(exception_text.rstrip() + "\n", encoding="utf-8")
        artifacts["exception"] = exception_path

    if result.graph_ascii:
        graph_path = target_dir / "graph.txt"
        graph_path.write_text(result.graph_ascii.rstrip() + "\n", encoding="utf-8")
        artifacts["graph"] = graph_path

    if result.tasks:
        tasks_path = target_dir / "tasks.csv"
        with tasks_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "task_id",
                    "map_index",
                    "state",
                    "try_number",
                    "start_date",
                    "end_date",
                    "duration_seconds",
                ],
            )
            writer.writeheader()
            for task in result.tasks:
                writer.writerow(
                    {
                        "task_id": task.task_id,
                        "map_index": "" if task.map_index is None else task.map_index,
                        "state": task.state or "",
                        "try_number": "" if task.try_number is None else task.try_number,
                        "start_date": task.start_date or "",
                        "end_date": task.end_date or "",
                        "duration_seconds": "" if task.duration_seconds is None else task.duration_seconds,
                    }
                )
        artifacts["tasks"] = tasks_path

    return artifacts
