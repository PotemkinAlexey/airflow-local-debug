from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def normalize_state(state: Any) -> str | None:
    """
    Normalize an Airflow state value to a stable lowercase string.

    Strips the `DagRunState.` / `TaskInstanceState.` prefix that some
    Airflow / Python combinations emit via `str(enum_member)`.
    """
    if state is None:
        return None
    text = str(state).strip()
    if not text:
        return None
    if "." in text:
        prefix, _, suffix = text.partition(".")
        if prefix.endswith("State") and suffix:
            text = suffix
    return text.lower()


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
    duration_seconds: float | None = None
    mocked: bool = False


@dataclass
class TaskMockInfo:
    task_id: str
    mode: str
    rule_name: str | None = None
    xcom_keys: list[str] = field(default_factory=list)


@dataclass
class DeferrableTaskInfo:
    task_id: str
    operator: str
    trigger: str | None = None
    local_mode: str | None = None
    reason: str | None = None


@dataclass
class DagFileInfo:
    dag_id: str
    task_count: int
    fileloc: str | None = None


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
    selected_tasks: list[str] = field(default_factory=list)
    tasks: list[TaskRunInfo] = field(default_factory=list)
    mocks: list[TaskMockInfo] = field(default_factory=list)
    deferrables: list[DeferrableTaskInfo] = field(default_factory=list)
    xcoms: dict[str, dict[str, Any]] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)
    exception: str | None = None
    exception_raw: str | None = None
    exception_was_logged: bool = False

    @property
    def ok(self) -> bool:
        if self.exception is not None:
            return False
        return normalize_state(self.state) == "success"
