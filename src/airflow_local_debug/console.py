from __future__ import annotations

from typing import Any


def print_run_preamble(
    dag: Any,
    *,
    backend_hint: str | None = None,
    config_path: str | None = None,
    logical_date: str | None = None,
    graph_text: str | None = None,
) -> str:
    lines = [f"Starting local DAG run: {getattr(dag, 'dag_id', '<unknown>')}"]

    if backend_hint:
        lines.append(f"Backend: {backend_hint}")
    if logical_date:
        lines.append(f"Logical date: {logical_date}")
    if config_path:
        lines.append(f"Config: {config_path}")
    if graph_text:
        lines.append("")
        lines.append(graph_text)
    output = "\n".join(lines)
    print(output)
    return output
