from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType

import pytest

from airflow_local_debug.execution.dag_loader import (
    dag_file_info,
    load_module_from_file,
    resolve_dag_from_module,
)


@dataclass
class FakeDag:
    dag_id: str
    task_dict: dict[str, object] = field(default_factory=dict)
    tasks: list[object] = field(default_factory=list)
    fileloc: str | None = None


def test_load_module_from_file_imports_python_file(tmp_path: Path) -> None:
    dag_file = tmp_path / "dag_file.py"
    dag_file.write_text('DAG_ID = "demo"\n', encoding="utf-8")

    module = load_module_from_file(str(dag_file))

    assert module.DAG_ID == "demo"
    assert module.__name__.startswith("airflow_debug_dag_")


def test_load_module_from_file_formats_missing_file_error(tmp_path: Path) -> None:
    dag_file = tmp_path / "missing.py"

    with pytest.raises(FileNotFoundError, match=f"DAG file not found: {dag_file}"):
        load_module_from_file(str(dag_file))


def test_dag_file_info_prefers_task_dict_count() -> None:
    dag = FakeDag(
        dag_id="demo",
        task_dict={"a": object(), "b": object()},
        tasks=[object()],
        fileloc="/tmp/demo.py",
    )

    info = dag_file_info(dag)

    assert info.dag_id == "demo"
    assert info.task_count == 2
    assert info.fileloc == "/tmp/demo.py"


def test_dag_file_info_falls_back_to_tasks_list() -> None:
    dag = FakeDag(dag_id="demo", tasks=[object(), object(), object()])

    info = dag_file_info(dag)

    assert info.task_count == 3
    assert info.fileloc is None


def test_resolve_dag_from_module_returns_single_candidate() -> None:
    module = ModuleType("fake_dags")
    dag = FakeDag(dag_id="demo")
    module.demo = dag

    assert resolve_dag_from_module(module) is dag


def test_resolve_dag_from_module_returns_matching_dag_id() -> None:
    module = ModuleType("fake_dags")
    first = FakeDag(dag_id="first")
    second = FakeDag(dag_id="second")
    module.first = first
    module.second = second

    assert resolve_dag_from_module(module, dag_id="second") is second


def test_resolve_dag_from_module_rejects_missing_dag_id() -> None:
    module = ModuleType("fake_dags")
    module.demo = FakeDag(dag_id="demo")

    with pytest.raises(ValueError, match="DAG with dag_id='other' not found"):
        resolve_dag_from_module(module, dag_id="other")


def test_resolve_dag_from_module_rejects_module_without_dags() -> None:
    module = ModuleType("fake_dags")
    module.not_a_dag = object()

    with pytest.raises(ValueError, match="No DAG objects found"):
        resolve_dag_from_module(module)


def test_resolve_dag_from_module_rejects_ambiguous_module() -> None:
    module = ModuleType("fake_dags")
    module.zeta = FakeDag(dag_id="zeta")
    module.alpha = FakeDag(dag_id="alpha")

    with pytest.raises(ValueError, match="Multiple DAG objects found.*alpha, zeta"):
        resolve_dag_from_module(module)
