from __future__ import annotations

from collections import Counter
import csv
from dataclasses import asdict
import json
from pathlib import Path
from typing import Literal
from xml.etree import ElementTree

from airflow_local_debug.models import RunResult

RunArtifactName = Literal["result", "report", "exception", "graph", "tasks", "junit", "xcom"]

_FAILED_TASK_STATES = {"failed", "up_for_retry", "upstream_failed", "shutdown"}
_SKIPPED_TASK_STATES = {"skipped", "not_run", "removed"}


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


def _write_junit_xml(result: RunResult, path: Path) -> None:
    total_time = sum(float(task.duration_seconds or 0) for task in result.tasks)
    failures = sum(1 for task in result.tasks if (task.state or "unknown") in _FAILED_TASK_STATES)
    skipped = sum(1 for task in result.tasks if (task.state or "unknown") in _SKIPPED_TASK_STATES)
    suite = ElementTree.Element(
        "testsuite",
        {
            "name": result.dag_id,
            "tests": str(len(result.tasks)),
            "failures": str(failures),
            "errors": "0",
            "skipped": str(skipped),
            "time": f"{total_time:.6f}",
        },
    )
    if result.run_id:
        suite.set("id", result.run_id)

    for task in result.tasks:
        state = task.state or "unknown"
        case = ElementTree.SubElement(
            suite,
            "testcase",
            {
                "classname": result.dag_id,
                "name": task.task_id if task.map_index is None or task.map_index < 0 else f"{task.task_id}[{task.map_index}]",
                "time": f"{float(task.duration_seconds or 0):.6f}",
            },
        )
        if state in _FAILED_TASK_STATES:
            failure = ElementTree.SubElement(case, "failure", {"message": f"Task state: {state}", "type": state})
            failure.text = result.exception_raw or result.exception or f"Task {task.task_id} finished in state {state}."
        elif state in _SKIPPED_TASK_STATES:
            ElementTree.SubElement(case, "skipped", {"message": f"Task state: {state}"})

    tree = ElementTree.ElementTree(suite)
    ElementTree.indent(tree, space="  ")
    tree.write(path, encoding="utf-8", xml_declaration=True)


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

    if result.mocks:
        lines.append("Mocked tasks:")
        for mock in result.mocks:
            suffix = f" via {mock.rule_name}" if mock.rule_name else ""
            xcom_suffix = f" xcom={','.join(mock.xcom_keys)}" if mock.xcom_keys else ""
            lines.append(f"- {mock.task_id}: {mock.mode}{suffix}{xcom_suffix}")

    if result.deferrables:
        lines.append("Deferrable tasks:")
        for item in result.deferrables:
            trigger = f" trigger={item.trigger}" if item.trigger else ""
            mode = f" mode={item.local_mode}" if item.local_mode else ""
            lines.append(f"- {item.task_id}: {item.operator}{trigger}{mode}")

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
            mock_suffix = " [mocked]" if task.mocked else ""
            lines.append(f"- {task.task_id}{map_suffix}: {task.state or 'unknown'}{duration_suffix}{mock_suffix}")

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
                    "mocked",
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
                        "mocked": "true" if task.mocked else "false",
                    }
                )
        artifacts["tasks"] = tasks_path

        junit_path = target_dir / "junit.xml"
        _write_junit_xml(result, junit_path)
        artifacts["junit"] = junit_path

    if result.xcoms:
        artifacts["xcom"] = write_xcom_snapshot(result, target_dir / "xcom.json")

    return artifacts


def write_xcom_snapshot(result: RunResult, path: str | Path) -> Path:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    target = target.resolve()
    target.write_text(
        json.dumps(result.xcoms, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return target
