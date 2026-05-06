from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from typing import Literal

from airflow_local_debug.models import RunResult

RunArtifactName = Literal["result", "report", "exception", "graph"]


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

    return artifacts
