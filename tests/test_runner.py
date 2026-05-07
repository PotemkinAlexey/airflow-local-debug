from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from types import ModuleType
from typing import Any

import pytest

from airflow_local_debug import runner
from airflow_local_debug.execution.dag_loader import dag_candidates_from_module
from airflow_local_debug.execution.mocks import TaskMockRule
from airflow_local_debug.execution.partial_runs import (
    detect_external_upstreams,
    format_external_upstream_note,
    partial_dag_for_selected_tasks,
    resolve_partial_task_ids,
)
from airflow_local_debug.execution.result import (
    annotate_deferred_result,
    extract_task_runs,
    failed_task_label,
    normalize_result,
    normalize_task_states_for_backend,
    task_state_buckets,
)
from airflow_local_debug.execution.state import duration_seconds, serialize_datetime, state_token, task_instance_label
from airflow_local_debug.models import DagFileInfo, RunResult, TaskRunInfo

# --- helpers --------------------------------------------------------------


@dataclass
class FakeTask:
    task_id: str
    upstream_task_ids: set[str] = field(default_factory=set)
    downstream_task_ids: set[str] = field(default_factory=set)
    task_group: Any = None


@dataclass
class FakeTaskGroup:
    group_id: str


@dataclass
class FakeDag:
    task_dict: dict[str, FakeTask]
    dag_id: str = "demo"

    def partial_subset(
        self,
        task_ids_or_regex: list[str],
        include_downstream: bool = False,
        include_upstream: bool = True,
        include_direct_upstream: bool = False,
    ) -> FakeDag:
        subset = FakeDag(
            task_dict={task_id: self.task_dict[task_id] for task_id in task_ids_or_regex},
            dag_id=self.dag_id,
        )
        subset.partial_subset_args = {
            "task_ids_or_regex": list(task_ids_or_regex),
            "include_downstream": include_downstream,
            "include_upstream": include_upstream,
            "include_direct_upstream": include_direct_upstream,
        }
        return subset


def _link(parent: FakeTask, child: FakeTask) -> None:
    parent.downstream_task_ids.add(child.task_id)
    child.upstream_task_ids.add(parent.task_id)


@dataclass
class FakeTI:
    task_id: str
    state: Any = None
    map_index: int | None = -1
    start_date: Any = None
    end_date: Any = None


class FakeDagrun:
    def __init__(self, tis: list[FakeTI]) -> None:
        self._tis = tis

    def get_task_instances(self) -> list[FakeTI]:
        return list(self._tis)


# --- _serialize_datetime --------------------------------------------------


def test_serialize_datetime_handles_none_and_iso() -> None:
    assert serialize_datetime(None) is None
    assert serialize_datetime(datetime(2026, 1, 1, 12, 0)) == "2026-01-01T12:00:00"


def test_serialize_datetime_falls_back_to_str_for_unsupported() -> None:
    class WeirdValue:
        def isoformat(self) -> str:
            raise RuntimeError("nope")

        def __str__(self) -> str:  # noqa: D401
            return "weird"

    assert serialize_datetime(WeirdValue()) == "weird"


def test_duration_seconds_handles_datetimes() -> None:
    start = datetime(2026, 1, 1, 12, 0, 0)
    end = datetime(2026, 1, 1, 12, 0, 1, 250000)

    assert duration_seconds(start, end) == 1.25
    assert duration_seconds(end, start) is None
    assert duration_seconds(None, end) is None


# --- _coerce_logical_date -------------------------------------------------


def test_coerce_logical_date_returns_none_for_none_or_blank() -> None:
    assert runner._coerce_logical_date(None) is None
    assert runner._coerce_logical_date("") is None


def test_coerce_logical_date_makes_naive_datetime_tz_aware() -> None:
    naive = datetime(2026, 5, 5, 9, 30)
    coerced = runner._coerce_logical_date(naive)
    assert coerced == datetime(2026, 5, 5, 9, 30, tzinfo=timezone.utc)
    assert coerced.tzinfo is timezone.utc


def test_coerce_logical_date_preserves_existing_timezone() -> None:
    aware = datetime(2026, 5, 5, 9, 30, tzinfo=timezone.utc)
    coerced = runner._coerce_logical_date(aware)
    assert coerced is aware


def test_coerce_logical_date_handles_date_value() -> None:
    coerced = runner._coerce_logical_date(date(2026, 5, 5))
    assert coerced == datetime(2026, 5, 5, tzinfo=timezone.utc)


def test_coerce_logical_date_parses_date_only_string() -> None:
    coerced = runner._coerce_logical_date("2026-05-05")
    assert coerced == datetime(2026, 5, 5, tzinfo=timezone.utc)


def test_coerce_logical_date_parses_iso_string() -> None:
    coerced = runner._coerce_logical_date("2026-05-05T11:00:00")
    assert coerced == datetime(2026, 5, 5, 11, 0, tzinfo=timezone.utc)


def test_coerce_logical_date_rejects_garbage() -> None:
    with pytest.raises(ValueError):
        runner._coerce_logical_date("not a date at all")


# --- _load_cli_conf -------------------------------------------------------


def test_load_cli_conf_parses_json_object() -> None:
    assert runner._load_cli_conf(conf_json='{"dataset": "daily", "limit": 10}') == {
        "dataset": "daily",
        "limit": 10,
    }


def test_load_cli_conf_reads_json_file(tmp_path) -> None:
    conf_file = tmp_path / "conf.json"
    conf_file.write_text('{"enabled": true}', encoding="utf-8")

    assert runner._load_cli_conf(conf_file=str(conf_file)) == {"enabled": True}


def test_load_cli_conf_rejects_invalid_payloads(tmp_path) -> None:
    conf_file = tmp_path / "conf.json"
    conf_file.write_text("[1, 2, 3]", encoding="utf-8")

    with pytest.raises(ValueError, match="either --conf-json or --conf-file"):
        runner._load_cli_conf(conf_json="{}", conf_file=str(conf_file))
    with pytest.raises(ValueError, match="must be a JSON object"):
        runner._load_cli_conf(conf_file=str(conf_file))
    with pytest.raises(ValueError, match="Invalid DAG run conf JSON"):
        runner._load_cli_conf(conf_json="{broken")


# --- _load_cli_extra_env --------------------------------------------------


def test_load_cli_extra_env_parses_key_value_pairs() -> None:
    assert runner._load_cli_extra_env(["FOO=bar", "EMPTY=", "WITH_EQUALS=a=b"]) == {
        "FOO": "bar",
        "EMPTY": "",
        "WITH_EQUALS": "a=b",
    }


def test_load_cli_extra_env_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="KEY=VALUE"):
        runner._load_cli_extra_env(["NOPE"])
    with pytest.raises(ValueError, match="KEY=VALUE"):
        runner._load_cli_extra_env(["=value"])


# --- partial task selection ------------------------------------------------


def test_load_cli_selector_values_splits_repeated_and_comma_values() -> None:
    assert runner._load_cli_selector_values(["a,b", "c"], option_name="--task") == ["a", "b", "c"]


def test_load_cli_selector_values_rejects_blanks() -> None:
    with pytest.raises(ValueError, match="--task"):
        runner._load_cli_selector_values(["a,"], option_name="--task")


def test_resolve_partial_task_ids_selects_exact_tasks_only() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    c = FakeTask("c")
    _link(a, b)
    _link(b, c)
    dag = FakeDag(task_dict={"a": a, "b": b, "c": c})

    assert resolve_partial_task_ids(dag, task_ids=["b"]) == ["b"]


def test_resolve_partial_task_ids_selects_start_task_downstream() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    c = FakeTask("c")
    side = FakeTask("side")
    _link(a, b)
    _link(b, c)
    dag = FakeDag(task_dict={"a": a, "b": b, "c": c, "side": side})

    assert resolve_partial_task_ids(dag, start_task_ids=["b"]) == ["b", "c"]


def test_resolve_partial_task_ids_selects_task_group_descendants() -> None:
    group = FakeTaskGroup("extract")
    nested_group = FakeTaskGroup("extract.clean")
    a = FakeTask("extract.raw", task_group=group)
    b = FakeTask("extract.clean.normalize", task_group=nested_group)
    c = FakeTask("load")
    _link(a, b)
    _link(b, c)
    dag = FakeDag(task_dict={"extract.raw": a, "extract.clean.normalize": b, "load": c})

    assert resolve_partial_task_ids(dag, task_group_ids=["extract"]) == [
        "extract.raw",
        "extract.clean.normalize",
    ]


def test_resolve_partial_task_ids_rejects_unknown_selector() -> None:
    dag = FakeDag(task_dict={"a": FakeTask("a")})

    with pytest.raises(ValueError, match="Unknown task id"):
        resolve_partial_task_ids(dag, task_ids=["missing"])
    with pytest.raises(ValueError, match="Unknown task group"):
        resolve_partial_task_ids(dag, task_group_ids=["missing"])


def test_detect_external_upstreams_finds_unmet_dependencies() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    c = FakeTask("c")
    side = FakeTask("side")
    _link(a, b)
    _link(b, c)
    _link(side, c)
    dag = FakeDag(task_dict={"a": a, "b": b, "c": c, "side": side})

    external = detect_external_upstreams(dag, ["b", "c"])

    assert external == {"b": ["a"], "c": ["side"]}


def test_detect_external_upstreams_returns_empty_for_root_task() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    _link(a, b)
    dag = FakeDag(task_dict={"a": a, "b": b})

    assert detect_external_upstreams(dag, ["a", "b"]) == {}


def test_detect_external_upstreams_handles_singleton_root() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    _link(a, b)
    dag = FakeDag(task_dict={"a": a, "b": b})

    assert detect_external_upstreams(dag, ["a"]) == {}


def test_format_external_upstream_note_lists_pairs() -> None:
    note = format_external_upstream_note({"b": ["a"], "c": ["side"]})
    assert "b <- a" in note
    assert "c <- side" in note
    assert "XCom pulls" in note
    assert "--mock-file" in note


def test_format_external_upstream_note_truncates_long_lists() -> None:
    external = {f"t{i}": [f"u{i}"] for i in range(8)}
    note = format_external_upstream_note(external)
    assert "+3 more" in note


def test_partial_dag_for_selected_tasks_uses_airflow_subset_semantics() -> None:
    dag = FakeDag(task_dict={"a": FakeTask("a"), "b": FakeTask("b")})

    subset = partial_dag_for_selected_tasks(dag, ["b"])

    assert list(subset.task_dict) == ["b"]
    assert subset.partial_subset_args == {
        "task_ids_or_regex": ["b"],
        "include_downstream": False,
        "include_upstream": False,
        "include_direct_upstream": False,
    }


# --- _state_token / _task_state_buckets -----------------------------------


def test_state_token_lowercases_and_handles_none() -> None:
    assert state_token(None) is None
    assert state_token("  FAILED ") == "failed"
    assert state_token("") is None


def test_task_state_buckets_splits_failed_and_unfinished() -> None:
    tasks = [
        TaskRunInfo(task_id="a", state="success"),
        TaskRunInfo(task_id="b", state="failed"),
        TaskRunInfo(task_id="c", state="upstream_failed"),
        TaskRunInfo(task_id="d", state="scheduled"),
        TaskRunInfo(task_id="e", state="up_for_retry"),
    ]

    failed, unfinished = task_state_buckets(tasks)

    assert {task.task_id for task in failed} == {"b", "c", "e"}
    assert {task.task_id for task in unfinished} == {"d"}


def test_extract_task_runs_records_duration() -> None:
    start = datetime(2026, 1, 1, 12, 0, 0)
    end = datetime(2026, 1, 1, 12, 0, 2)
    dagrun = FakeDagrun([FakeTI(task_id="a", state="success", start_date=start, end_date=end)])
    dag = FakeDag(task_dict={"a": FakeTask("a")})

    tasks = extract_task_runs(dagrun, dag)

    assert len(tasks) == 1
    assert tasks[0].start_date == "2026-01-01T12:00:00"
    assert tasks[0].end_date == "2026-01-01T12:00:02"
    assert tasks[0].duration_seconds == 2.0


def test_extract_task_runs_marks_mocked_tasks() -> None:
    dagrun = FakeDagrun([FakeTI(task_id="load", state="success")])
    dag = FakeDag(task_dict={"load": FakeTask("load")})

    tasks = extract_task_runs(dagrun, dag, mocked_task_ids={"load"})

    assert tasks[0].mocked is True


# --- _task_instance_label -------------------------------------------------


def test_task_instance_label_handles_map_index() -> None:
    assert task_instance_label(FakeTI(task_id="a")) == "a"
    assert task_instance_label(FakeTI(task_id="a", map_index=2)) == "a[2]"
    assert task_instance_label(FakeTI(task_id="")) is None


# --- _failed_task_label ---------------------------------------------------


def test_failed_task_label_returns_none_when_nothing_failed() -> None:
    dagrun = FakeDagrun([FakeTI(task_id="a", state="success")])
    assert failed_task_label(dagrun) is None


def test_failed_task_label_collects_multiple_with_truncation() -> None:
    dagrun = FakeDagrun(
        [
            FakeTI(task_id="d", state="failed"),
            FakeTI(task_id="b", state="failed"),
            FakeTI(task_id="c", state="up_for_retry"),
            FakeTI(task_id="a", state="failed"),
        ]
    )
    label = failed_task_label(dagrun)
    assert label is not None
    # Labels are sorted before truncation so DB task instance order does not leak into the report.
    assert label == "a, b, c ..."


# --- _normalize_task_states_for_backend -----------------------------------


def test_normalize_marks_downstream_as_upstream_failed_in_strict_mode() -> None:
    a = FakeTask("a")
    b = FakeTask("b")
    c = FakeTask("c")
    _link(a, b)
    _link(b, c)
    dag = FakeDag(task_dict={"a": a, "b": b, "c": c})
    tasks = [
        TaskRunInfo(task_id="a", state="failed"),
        TaskRunInfo(task_id="b", state="scheduled"),
        TaskRunInfo(task_id="c", state="scheduled"),
    ]

    normalized = normalize_task_states_for_backend(dag, tasks, backend="dag.test.strict")

    states = {task.task_id: task.state for task in normalized}
    assert states == {"a": "failed", "b": "upstream_failed", "c": "upstream_failed"}


def test_normalize_marks_unrelated_unfinished_as_not_run() -> None:
    a = FakeTask("a")
    b = FakeTask("b")  # independent, not downstream of failed
    dag = FakeDag(task_dict={"a": a, "b": b})
    tasks = [
        TaskRunInfo(task_id="a", state="failed"),
        TaskRunInfo(task_id="b", state="scheduled"),
    ]

    normalized = normalize_task_states_for_backend(dag, tasks, backend="dag.test.strict")
    states = {task.task_id: task.state for task in normalized}

    assert states == {"a": "failed", "b": "not_run"}


def test_normalize_is_noop_for_non_strict_backend() -> None:
    dag = FakeDag(task_dict={"a": FakeTask("a")})
    tasks = [TaskRunInfo(task_id="a", state="failed")]
    assert normalize_task_states_for_backend(dag, tasks, backend="dag.test") == tasks


# --- _normalize_result ----------------------------------------------------


def test_normalize_result_sets_failed_state_and_exception() -> None:
    result = RunResult(
        dag_id="demo",
        state="success",  # incorrectly success even though tasks failed
        tasks=[TaskRunInfo(task_id="a", state="failed")],
    )
    out = normalize_result(result)
    assert out.state == "failed"
    assert out.exception is not None
    assert "failed or retry-pending" in out.exception


def test_normalize_result_handles_enum_repr_state() -> None:
    # Even when state arrives as the enum repr, a failed task forces "failed".
    result = RunResult(
        dag_id="demo",
        state="DagRunState.SUCCESS",
        tasks=[TaskRunInfo(task_id="a", state="failed")],
    )
    out = normalize_result(result)
    assert out.state == "failed"


def test_normalize_result_marks_incomplete_when_only_unfinished() -> None:
    result = RunResult(
        dag_id="demo",
        state="running",
        tasks=[TaskRunInfo(task_id="a", state="scheduled")],
    )
    out = normalize_result(result)
    assert out.state == "incomplete"
    assert out.exception is not None


def test_annotate_deferred_result_adds_actionable_note() -> None:
    result = RunResult(dag_id="demo", tasks=[TaskRunInfo(task_id="wait", state="deferred")])

    annotate_deferred_result(result)

    assert result.notes
    assert "wait=deferred" in result.notes[0]
    assert "--mock-file" in result.notes[0]


def test_normalize_result_keeps_success_when_all_tasks_done() -> None:
    result = RunResult(
        dag_id="demo",
        state="success",
        tasks=[TaskRunInfo(task_id="a", state="success")],
    )
    out = normalize_result(result)
    assert out.state == "success"
    assert out.exception is None


# --- _backend_hint --------------------------------------------------------


def test_backend_hint_picks_strict_when_test_available_and_fail_fast() -> None:
    class HasTest:
        def test(self) -> None: ...

    assert runner._backend_hint(HasTest(), fail_fast=True) == "dag.test.strict"
    assert runner._backend_hint(HasTest(), fail_fast=False) == "dag.test"


def test_backend_hint_falls_back_to_dag_run_or_unsupported() -> None:
    class HasRun:
        def run(self) -> None: ...

    class HasNothing: ...

    assert runner._backend_hint(HasRun(), fail_fast=False) == "dag.run"
    assert runner._backend_hint(HasNothing(), fail_fast=False) == "unsupported"


# --- graph SVG artifact plumbing -----------------------------------------


def test_resolve_graph_svg_path_defaults_to_report_dir(tmp_path) -> None:
    assert runner._resolve_graph_svg_path(report_dir=tmp_path, graph_svg_path=None) == tmp_path / "graph.svg"
    assert runner._resolve_graph_svg_path(report_dir=tmp_path, graph_svg_path="/tmp/custom.svg") == "/tmp/custom.svg"
    assert runner._resolve_graph_svg_path(report_dir=None, graph_svg_path=None) is None


def test_attach_graph_svg_writes_path(tmp_path) -> None:
    result = RunResult(dag_id="demo", state="success")
    output_path = tmp_path / "graph.svg"

    runner._attach_graph_svg(FakeDag(task_dict={}), result, output_path)

    assert result.graph_svg_path == str(output_path.resolve())
    assert output_path.read_text(encoding="utf-8").startswith("<svg")


# --- DAG file listing -----------------------------------------------------


def test_dag_candidates_from_module_deduplicates_and_sorts() -> None:
    module = ModuleType("fake_dags")
    first = FakeDag(task_dict={}, dag_id="first")
    second = FakeDag(task_dict={}, dag_id="second")
    module.second = second
    module.first = first
    module.alias = first
    module.not_a_dag = object()

    candidates = dag_candidates_from_module(module)

    assert candidates == [first, second]


def test_load_cli_env_files_merges_explicit_files(tmp_path) -> None:
    file_a = tmp_path / "a.env"
    file_a.write_text("A=1\nSHARED=from_a\n")
    file_b = tmp_path / "b.env"
    file_b.write_text("B=2\nSHARED=from_b\n")

    result = runner._load_cli_env_files([str(file_a), str(file_b)])

    assert result == {"A": "1", "B": "2", "SHARED": "from_b"}


def test_load_cli_env_files_auto_discovers_when_no_explicit_path(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("AUTO=found\n")
    monkeypatch.chdir(tmp_path)

    result = runner._load_cli_env_files(None)

    assert result == {"AUTO": "found"}


def test_load_cli_env_files_skips_auto_discovery_when_disabled(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("AUTO=found\n")
    monkeypatch.chdir(tmp_path)

    result = runner._load_cli_env_files(None, auto_discover=False)

    assert result == {}


def test_load_cli_env_files_explicit_path_skips_auto_discovery(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("AUTO=ignored\n")
    explicit = tmp_path / "explicit.env"
    explicit.write_text("EXPLICIT=yes\n")
    monkeypatch.chdir(tmp_path)

    result = runner._load_cli_env_files([str(explicit)])

    assert result == {"EXPLICIT": "yes"}
    assert "AUTO" not in result


def test_format_dag_list_renders_task_counts(tmp_path) -> None:
    rendered = runner.format_dag_list(
        [
            DagFileInfo(dag_id="daily", task_count=1),
            DagFileInfo(dag_id="hourly", task_count=3),
        ],
        source_path=str(tmp_path / "dags.py"),
    )

    assert "DAGs in" in rendered
    assert "- daily (1 task)" in rendered
    assert "- hourly (3 tasks)" in rendered


def test_format_dag_list_handles_empty() -> None:
    assert runner.format_dag_list([]) == "DAGs\n<none>"


# --- CLI argument plumbing ------------------------------------------------


def test_debug_dag_cli_passes_report_dir(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    captured: dict[str, Any] = {}

    def fake_debug_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured["dag"] = dag
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag", fake_debug_dag)
    dag = FakeDag(task_dict={})

    result = runner.debug_dag_cli(
        dag,
        argv=["--report-dir", str(tmp_path), "--include-graph-in-report"],
    )

    assert result.ok
    assert captured["dag"] is dag
    assert captured["report_dir"] == str(tmp_path)
    assert captured["graph_svg_path"] is None
    assert captured["include_graph_in_report"] is True


def test_debug_dag_cli_passes_graph_svg_path(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_debug_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag", fake_debug_dag)

    result = runner.debug_dag_cli(
        FakeDag(task_dict={}),
        argv=["--graph-svg-path", "/tmp/graph.svg"],
    )

    assert result.ok
    assert captured["graph_svg_path"] == "/tmp/graph.svg"


def test_debug_dag_cli_passes_conf_json(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_debug_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag", fake_debug_dag)

    result = runner.debug_dag_cli(
        FakeDag(task_dict={}),
        argv=["--conf-json", '{"dataset": "daily"}'],
    )

    assert result.ok
    assert captured["conf"] == {"dataset": "daily"}


def test_debug_dag_cli_preserves_programmatic_conf(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_debug_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag", fake_debug_dag)

    result = runner.debug_dag_cli(
        FakeDag(task_dict={}),
        argv=[],
        conf={"dataset": "daily"},
    )

    assert result.ok
    assert captured["conf"] == {"dataset": "daily"}


def test_debug_dag_cli_passes_extra_env(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_debug_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag", fake_debug_dag)

    result = runner.debug_dag_cli(
        FakeDag(task_dict={}),
        argv=["--env", "FOO=bar", "--env", "BAZ=qux"],
        extra_env={"FOO": "old", "KEEP": "yes"},
    )

    assert result.ok
    assert captured["extra_env"] == {"FOO": "bar", "KEEP": "yes", "BAZ": "qux"}


def test_debug_dag_cli_loads_env_file_and_explicit_env_wins(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    env_file = tmp_path / "creds.env"
    env_file.write_text("DB_PASSWORD=from_file\nSHARED=from_file\n")

    captured: dict[str, Any] = {}

    def fake_debug_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag", fake_debug_dag)
    monkeypatch.chdir(tmp_path)

    result = runner.debug_dag_cli(
        FakeDag(task_dict={}),
        argv=["--env-file", str(env_file), "--env", "SHARED=from_cli"],
    )

    assert result.ok
    assert captured["extra_env"]["DB_PASSWORD"] == "from_file"
    assert captured["extra_env"]["SHARED"] == "from_cli"


def test_debug_dag_cli_auto_discovers_dotenv_in_cwd(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    (tmp_path / ".env").write_text("AUTO_LOADED=1\n")

    captured: dict[str, Any] = {}

    def fake_debug_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag", fake_debug_dag)
    monkeypatch.chdir(tmp_path)

    runner.debug_dag_cli(FakeDag(task_dict={}), argv=[])

    assert captured["extra_env"]["AUTO_LOADED"] == "1"


def test_debug_dag_cli_no_auto_env_disables_dotenv_discovery(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    (tmp_path / ".env").write_text("AUTO_LOADED=1\n")

    captured: dict[str, Any] = {}

    def fake_debug_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag", fake_debug_dag)
    monkeypatch.chdir(tmp_path)

    runner.debug_dag_cli(FakeDag(task_dict={}), argv=["--no-auto-env"])

    assert captured.get("extra_env") in (None, {})


def test_debug_dag_cli_passes_task_mocks_and_xcom_flags(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    captured: dict[str, Any] = {}
    mock_file = tmp_path / "mocks.json"
    mock_file.write_text('[{"task_id": "load", "return_value": {"rows": 3}}]', encoding="utf-8")

    def fake_debug_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag", fake_debug_dag)

    result = runner.debug_dag_cli(
        FakeDag(task_dict={}),
        argv=[
            "--mock-file",
            str(mock_file),
            "--dump-xcom",
            "--xcom-json-path",
            str(tmp_path / "xcom.json"),
        ],
    )

    assert result.ok
    assert captured["task_mocks"] == [TaskMockRule(task_id="load", xcom={"return_value": {"rows": 3}})]
    assert captured["collect_xcoms"] is True
    assert captured["xcom_json_path"] == str(tmp_path / "xcom.json")


def test_debug_dag_cli_passes_partial_selectors(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_debug_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag", fake_debug_dag)

    result = runner.debug_dag_cli(
        FakeDag(task_dict={}),
        argv=[
            "--task",
            "one,two",
            "--start-task",
            "middle",
            "--task-group",
            "warehouse",
        ],
    )

    assert result.ok
    assert captured["task_ids"] == ["one", "two"]
    assert captured["start_task_ids"] == ["middle"]
    assert captured["task_group_ids"] == ["warehouse"]


def test_debug_dag_writes_report_dir(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    captured: dict[str, Any] = {}

    def fake_run_full_dag(dag: Any, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        graph_svg_path = kwargs["graph_svg_path"]
        return RunResult(
            dag_id="demo",
            state="success",
            graph_ascii="demo\n  first",
            graph_svg_path=str(graph_svg_path),
        )

    monkeypatch.setattr(runner, "run_full_dag", fake_run_full_dag)

    result = runner.debug_dag(
        FakeDag(task_dict={}),
        report_dir=tmp_path / "artifacts",
        include_graph_in_report=True,
        raise_on_failure=False,
    )

    assert result.ok
    assert captured["graph_svg_path"] == tmp_path / "artifacts" / "graph.svg"
    assert result.graph_svg_path == str(tmp_path / "artifacts" / "graph.svg")
    assert result.notes[-1] == f"Wrote run artifacts to {(tmp_path / 'artifacts').resolve()}"
    assert (tmp_path / "artifacts" / "result.json").exists()
    assert (tmp_path / "artifacts" / "report.md").exists()
    assert (tmp_path / "artifacts" / "graph.txt").exists()
    assert "Graph SVG:" in (tmp_path / "artifacts" / "report.md").read_text(encoding="utf-8")


def test_debug_dag_file_cli_passes_report_dir(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    captured: dict[str, Any] = {}

    def fake_debug_dag_from_file(dag_file: str, **kwargs: Any) -> RunResult:
        captured["dag_file"] = dag_file
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag_from_file", fake_debug_dag_from_file)

    result = runner.debug_dag_file_cli(
        argv=["/tmp/demo_dag.py", "--report-dir", str(tmp_path)],
    )

    assert result.ok
    assert captured["dag_file"] == "/tmp/demo_dag.py"
    assert captured["report_dir"] == str(tmp_path)
    assert captured["graph_svg_path"] is None


def test_debug_dag_file_cli_lists_dags(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    captured: dict[str, Any] = {}

    def fake_list_dags_from_file(dag_file: str, **kwargs: Any) -> list[DagFileInfo]:
        captured["dag_file"] = dag_file
        captured.update(kwargs)
        return [DagFileInfo(dag_id="daily", task_count=2)]

    monkeypatch.setattr(runner, "list_dags_from_file", fake_list_dags_from_file)

    result = runner.debug_dag_file_cli(
        argv=["/tmp/demo_dag.py", "--list-dags", "--env", "FOO=bar"],
    )

    assert result.ok
    assert result.dag_id == "<list-dags>"
    assert captured == {
        "dag_file": "/tmp/demo_dag.py",
        "config_path": None,
        "extra_env": {"FOO": "bar"},
    }
    assert "- daily (2 tasks)" in capsys.readouterr().out


def test_debug_dag_file_cli_passes_conf_file(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    captured: dict[str, Any] = {}
    conf_file = tmp_path / "conf.json"
    conf_file.write_text('{"dataset": "hourly"}', encoding="utf-8")

    def fake_debug_dag_from_file(dag_file: str, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag_from_file", fake_debug_dag_from_file)

    result = runner.debug_dag_file_cli(
        argv=["/tmp/demo_dag.py", "--conf-file", str(conf_file)],
    )

    assert result.ok
    assert captured["conf"] == {"dataset": "hourly"}


def test_debug_dag_file_cli_passes_extra_env(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_debug_dag_from_file(dag_file: str, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag_from_file", fake_debug_dag_from_file)

    result = runner.debug_dag_file_cli(
        argv=["/tmp/demo_dag.py", "--env", "FOO=bar"],
    )

    assert result.ok
    assert captured["extra_env"] == {"FOO": "bar"}


def test_debug_dag_file_cli_passes_task_mocks(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    captured: dict[str, Any] = {}
    mock_file = tmp_path / "mocks.json"
    mock_file.write_text('[{"task_id_glob": "load_*"}]', encoding="utf-8")

    def fake_debug_dag_from_file(dag_file: str, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag_from_file", fake_debug_dag_from_file)

    result = runner.debug_dag_file_cli(
        argv=["/tmp/demo_dag.py", "--mock-file", str(mock_file)],
    )

    assert result.ok
    assert captured["task_mocks"] == [TaskMockRule(task_id_glob="load_*")]


def test_debug_dag_file_cli_passes_partial_selectors(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_debug_dag_from_file(dag_file: str, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    monkeypatch.setattr(runner, "debug_dag_from_file", fake_debug_dag_from_file)

    result = runner.debug_dag_file_cli(
        argv=[
            "/tmp/demo_dag.py",
            "--task",
            "one",
            "--start-task",
            "middle,end",
            "--task-group",
            "warehouse",
        ],
    )

    assert result.ok
    assert captured["task_ids"] == ["one"]
    assert captured["start_task_ids"] == ["middle", "end"]
    assert captured["task_group_ids"] == ["warehouse"]


# --- xcom extraction skip-when-covered ------------------------------------


def test_extract_xcoms_skips_fallback_for_already_covered_labels(monkeypatch: pytest.MonkeyPatch) -> None:
    from airflow_local_debug.execution import xcom as xcom_module

    monkeypatch.setattr(
        xcom_module,
        "query_xcoms",
        lambda dagrun, dag: {"task_a": {"return_value": "from_db"}},
    )

    fallback_calls: list[set[str]] = []

    def fake_fallback(dagrun: Any, *, skip_labels: set[str] | None = None) -> dict[str, dict[str, Any]]:
        fallback_calls.append(skip_labels or set())
        return {"task_b": {"return_value": "from_pull"}}

    monkeypatch.setattr(xcom_module, "fallback_return_xcoms", fake_fallback)

    result = xcom_module.extract_xcoms(object(), object())

    assert fallback_calls == [{"task_a"}]
    assert result == {
        "task_a": {"return_value": "from_db"},
        "task_b": {"return_value": "from_pull"},
    }
