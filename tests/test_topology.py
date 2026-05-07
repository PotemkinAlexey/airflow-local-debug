from __future__ import annotations

from dataclasses import dataclass, field

from airflow_local_debug.execution.topology import topological_task_ids, topological_task_order


@dataclass
class FakeTask:
    task_id: str
    upstream_task_ids: set[str] = field(default_factory=set)
    downstream_task_ids: set[str] = field(default_factory=set)


@dataclass
class FakeDag:
    task_dict: dict[str, FakeTask]


def _link(parent: FakeTask, child: FakeTask) -> None:
    parent.downstream_task_ids.add(child.task_id)
    child.upstream_task_ids.add(parent.task_id)


def test_topological_task_order_linear() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    c = FakeTask("c")
    _link(a, b)
    _link(b, c)
    dag = FakeDag(task_dict={"a": a, "b": b, "c": c})

    order = topological_task_order(dag)

    assert order == {"a": 0, "b": 1, "c": 2}


def test_topological_task_order_diamond_is_deterministic() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    c = FakeTask("c")
    d = FakeTask("d")
    _link(a, b)
    _link(a, c)
    _link(b, d)
    _link(c, d)
    dag = FakeDag(task_dict={"a": a, "b": b, "c": c, "d": d})

    order = topological_task_order(dag)

    assert order["a"] == 0
    assert order["d"] == 3
    # b and c are siblings; alphabetic tie-break keeps b before c.
    assert order["b"] == 1
    assert order["c"] == 2


def test_topological_task_order_handles_cycle_gracefully() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    _link(a, b)
    _link(b, a)
    dag = FakeDag(task_dict={"a": a, "b": b})

    order = topological_task_order(dag)

    # Both tasks unreachable in the BFS; tail-fallback orders them alphabetically.
    assert set(order) == {"a", "b"}
    assert order["a"] < order["b"]


def test_topological_task_ids_returns_list_form() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    _link(a, b)

    ids = topological_task_ids([a, b])

    assert ids == ["a", "b"]


def test_topological_task_order_empty_dag() -> None:
    assert topological_task_order(FakeDag(task_dict={})) == {}
    assert topological_task_ids([]) == []
