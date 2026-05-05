from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class LocalConfig:
    source_path: str | None = None
    connections: dict[str, Any] = field(default_factory=dict)
    variables: dict[str, Any] = field(default_factory=dict)
    pools: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class TaskRunInfo:
    task_id: str
    state: str | None = None
    try_number: int | None = None
    map_index: int | None = None
    start_date: str | None = None
    end_date: str | None = None


@dataclass
class RunResult:
    dag_id: str
    run_id: str | None = None
    state: str | None = None
    logical_date: str | None = None
    backend: str | None = None
    airflow_version: str | None = None
    config_path: str | None = None
    graph_ascii: str | None = None
    graph_svg_path: str | None = None
    tasks: list[TaskRunInfo] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    exception: str | None = None
    exception_raw: str | None = None
    exception_was_logged: bool = False

    @property
    def ok(self) -> bool:
        return self.exception is None and self.state not in {"failed", "error"}
