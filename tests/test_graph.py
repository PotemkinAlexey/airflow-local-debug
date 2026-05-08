from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from airflow_local_debug.reporting.graph import (
    ASCII_MAX_TASKS,
    SVG_MAX_TASKS,
    format_dag_graph,
    print_dag_graph,
    render_dag_svg,
    write_dag_svg,
)


@dataclass
class FakeTaskGroup:
    group_id: str


@dataclass
class FakeTask:
    task_id: str
    task_type: str = "PythonOperator"
    task_group: Any = None
    upstream_task_ids: set[str] = field(default_factory=set)
    downstream_task_ids: set[str] = field(default_factory=set)


@dataclass
class FakeDag:
    dag_id: str = "demo"
    task_dict: dict[str, FakeTask] = field(default_factory=dict)
    tasks: list[FakeTask] | None = None


def _link(parent: FakeTask, child: FakeTask) -> None:
    parent.downstream_task_ids.add(child.task_id)
    child.upstream_task_ids.add(parent.task_id)


def _dag(*tasks: FakeTask, dag_id: str = "demo") -> FakeDag:
    return FakeDag(dag_id=dag_id, task_dict={task.task_id: task for task in tasks})


def test_format_dag_graph_empty_dag() -> None:
    assert format_dag_graph(FakeDag(dag_id="empty"), enable_colors=False) == "✨ DAG Structure: empty\n\n<empty>"


def test_format_dag_graph_linear_tree_without_colors() -> None:
    start = FakeTask("start")
    finish = FakeTask("finish")
    _link(start, finish)

    rendered = format_dag_graph(_dag(start, finish), enable_colors=False)

    assert rendered == "✨ DAG Structure: demo\n\n🚀 start\n└── finish"


def test_format_dag_graph_groups_tasks_and_trims_group_prefix() -> None:
    group = FakeTaskGroup("extract")
    start = FakeTask("extract.start", task_group=group)
    finish = FakeTask("extract.finish", task_group=group)
    _link(start, finish)

    rendered = format_dag_graph(_dag(start, finish), enable_colors=False)

    assert rendered == "✨ DAG Structure: demo\n\n🚀 extract\n└── start\n    └── finish"


def test_format_dag_graph_handles_multiple_roots_and_repeated_child() -> None:
    left = FakeTask("left")
    right = FakeTask("right")
    join = FakeTask("join")
    _link(left, join)
    _link(right, join)

    rendered = format_dag_graph(_dag(left, right, join), enable_colors=False)

    assert "🚀 ROOTS: left + right" in rendered
    assert "└── join" in rendered


def test_format_dag_graph_large_dag_placeholder() -> None:
    tasks = [FakeTask(f"task_{index}") for index in range(ASCII_MAX_TASKS + 1)]

    rendered = format_dag_graph(_dag(*tasks), enable_colors=False)

    assert f"DAG has {ASCII_MAX_TASKS + 1} tasks" in rendered
    assert "ASCII graph rendering disabled" in rendered
    assert "write_dag_svg" in rendered


def test_print_dag_graph_writes_to_stdout(capsys: Any) -> None:
    task = FakeTask("only")

    print_dag_graph(_dag(task), enable_colors=False)

    assert capsys.readouterr().out == "✨ DAG Structure: demo\n\n🚀 only\n"


def test_render_dag_svg_empty_dag_escapes_dag_id() -> None:
    rendered = render_dag_svg(FakeDag(dag_id="empty & <safe>"))

    assert rendered.startswith('<svg xmlns="http://www.w3.org/2000/svg"')
    assert "empty &amp; &lt;safe&gt;" in rendered
    assert "&lt;empty dag&gt;" in rendered


def test_render_dag_svg_large_dag_placeholder() -> None:
    tasks = [FakeTask(f"task_{index}") for index in range(SVG_MAX_TASKS + 1)]

    rendered = render_dag_svg(_dag(*tasks, dag_id="large"))

    assert f"DAG has {SVG_MAX_TASKS + 1} tasks" in rendered
    assert "SVG rendering disabled" in rendered
    assert "print_dag_graph" in rendered


def test_render_dag_svg_includes_edges_groups_and_escaped_labels() -> None:
    group = FakeTaskGroup("group & one")
    start = FakeTask("start <one>", task_type="BashOperator")
    middle = FakeTask("group.middle", task_group=group)
    end = FakeTask("end")
    _link(start, middle)
    _link(middle, end)

    rendered = render_dag_svg(_dag(start, middle, end, dag_id="dag & demo"))

    assert "dag &amp; demo" in rendered
    assert "group &amp; one" in rendered
    assert "start &lt;one&gt;" in rendered
    assert "[BashOperator]" in rendered
    assert 'marker-end="url(#arrowhead)"' in rendered
    assert 'fill="#fefce8"' in rendered


def test_render_dag_svg_uses_tasks_list_when_task_dict_is_empty() -> None:
    task = FakeTask("from_tasks")
    dag = FakeDag(task_dict={}, tasks=[task])

    rendered = render_dag_svg(dag)

    assert "from_tasks" in rendered


def test_write_dag_svg_writes_explicit_path(tmp_path: Path) -> None:
    task = FakeTask("task")
    output_path = tmp_path / "nested" / "graph.svg"

    written_path = write_dag_svg(_dag(task), str(output_path))

    assert written_path == str(output_path.resolve())
    assert output_path.exists()
    assert "<svg" in output_path.read_text(encoding="utf-8")
