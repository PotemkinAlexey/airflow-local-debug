from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import Any

import pytest

from airflow_local_debug import watch
from airflow_local_debug.models import RunResult, TaskRunInfo


def test_resolve_watch_roots_includes_dag_file_and_parent(tmp_path: Path) -> None:
    dag_file = tmp_path / "demo.py"
    dag_file.write_text("# placeholder")

    roots = watch.resolve_watch_roots(str(dag_file))

    assert dag_file.resolve() in roots
    assert dag_file.parent.resolve() in roots


def test_resolve_watch_roots_appends_unique_extras(tmp_path: Path) -> None:
    dag_file = tmp_path / "demo.py"
    dag_file.write_text("# placeholder")
    sql_dir = tmp_path / "sql"
    sql_dir.mkdir()

    roots = watch.resolve_watch_roots(
        str(dag_file),
        extra=[str(sql_dir), str(sql_dir), str(dag_file)],
    )

    assert sql_dir.resolve() in roots
    assert len(roots) == len(set(roots))


def test_snapshot_mtimes_filters_by_suffix(tmp_path: Path) -> None:
    (tmp_path / "a.py").write_text("a")
    (tmp_path / "b.sql").write_text("b")
    (tmp_path / "c.txt").write_text("c")

    snapshot = watch.snapshot_mtimes([tmp_path])

    paths = {path.name for path in snapshot}
    assert paths == {"a.py", "b.sql"}


def test_diff_snapshots_detects_change(tmp_path: Path) -> None:
    file = tmp_path / "x.py"
    file.write_text("v1")
    baseline = watch.snapshot_mtimes([tmp_path])
    file.write_text("v2-different-mtime")
    import os
    os.utime(file, (file.stat().st_atime, file.stat().st_mtime + 5))
    current = watch.snapshot_mtimes([tmp_path])

    changed = watch.diff_snapshots(baseline, current)

    assert file.resolve() in changed


def test_diff_snapshots_detects_added_and_removed(tmp_path: Path) -> None:
    (tmp_path / "old.py").write_text("o")
    baseline = watch.snapshot_mtimes([tmp_path])
    (tmp_path / "old.py").unlink()
    (tmp_path / "new.py").write_text("n")
    current = watch.snapshot_mtimes([tmp_path])

    changed = {path.name for path in watch.diff_snapshots(baseline, current)}

    assert changed == {"old.py", "new.py"}


def test_first_failed_task_id_returns_first_hard_failure() -> None:
    result = RunResult(
        dag_id="demo",
        tasks=[
            TaskRunInfo(task_id="a", state="success"),
            TaskRunInfo(task_id="b", state="failed"),
            TaskRunInfo(task_id="c", state="up_for_retry"),
        ],
    )

    assert watch.first_failed_task_id(result) == "b"


def test_first_failed_task_id_ignores_non_failure_states() -> None:
    result = RunResult(
        dag_id="demo",
        tasks=[
            TaskRunInfo(task_id="a", state="success"),
            TaskRunInfo(task_id="b", state="skipped"),
        ],
    )

    assert watch.first_failed_task_id(result) is None


def test_first_failed_task_id_skips_upstream_failed_to_find_real_root() -> None:
    """`upstream_failed` is not a hard failure — re-running such a task
    without first re-running its broken ancestor is pointless.
    """
    result = RunResult(
        dag_id="demo",
        tasks=[
            TaskRunInfo(task_id="extract", state="success"),
            TaskRunInfo(task_id="transform", state="upstream_failed"),
            TaskRunInfo(task_id="load", state="failed"),
        ],
    )

    assert watch.first_failed_task_id(result) == "load"


def test_purge_module_cache_drops_modules_under_root(tmp_path: Path) -> None:
    fake_module_file = tmp_path / "fake_helper.py"
    fake_module_file.write_text("# placeholder")

    class _FakeModule:
        __file__ = str(fake_module_file)

    sys.modules["airflow_local_debug_test_fake_helper"] = _FakeModule()  # type: ignore[assignment]
    try:
        dropped = watch.purge_module_cache([tmp_path])
        assert dropped >= 1
        assert "airflow_local_debug_test_fake_helper" not in sys.modules
    finally:
        sys.modules.pop("airflow_local_debug_test_fake_helper", None)


def test_purge_module_cache_keeps_modules_outside_roots(tmp_path: Path) -> None:
    other_dir = tmp_path / "other"
    other_dir.mkdir()
    inside = tmp_path / "watched"
    inside.mkdir()

    class _Outside:
        __file__ = str(other_dir / "x.py")

    sys.modules["test_outside_module"] = _Outside()  # type: ignore[assignment]
    try:
        watch.purge_module_cache([inside])
        assert "test_outside_module" in sys.modules
    finally:
        sys.modules.pop("test_outside_module", None)


def test_watch_dag_file_retries_failed_task_then_succeeds(tmp_path: Path) -> None:
    dag_file = tmp_path / "demo.py"
    dag_file.write_text("# placeholder")

    calls: list[dict[str, Any]] = []

    def fake_runner(path: str, *, dag_id: str | None = None, **kwargs: Any) -> RunResult:
        calls.append({"path": path, "dag_id": dag_id, **kwargs})
        if len(calls) == 1:
            return RunResult(
                dag_id="demo",
                state="failed",
                tasks=[
                    TaskRunInfo(task_id="extract", state="success"),
                    TaskRunInfo(task_id="transform", state="failed"),
                ],
                exception="boom",
            )
        return RunResult(
            dag_id="demo",
            state="success",
            tasks=[
                TaskRunInfo(task_id="transform", state="success"),
                TaskRunInfo(task_id="load", state="success"),
            ],
        )

    reports: list[RunResult] = []
    fake_sleep_calls: list[float] = []
    iteration_state = {"count": 0}

    def fake_sleep(seconds: float) -> None:
        fake_sleep_calls.append(seconds)
        iteration_state["count"] += 1
        # On the second sleep tick, simulate a file edit that bumps mtime.
        if iteration_state["count"] >= 2:
            import os
            stat = dag_file.stat()
            os.utime(dag_file, (stat.st_atime, stat.st_mtime + 100))

    stream = io.StringIO()
    result = watch.watch_dag_file(
        str(dag_file),
        runner=fake_runner,
        reporter=reports.append,
        stream=stream,
        sleep=fake_sleep,
        max_iterations=2,
        config_path="/tmp/cfg.py",
    )

    assert len(calls) == 2
    assert calls[0].get("start_task_ids") in (None, [])
    assert calls[1]["start_task_ids"] == ["transform"]
    assert calls[1]["config_path"] == "/tmp/cfg.py"
    assert result.ok
    assert "starting from failed task: transform" in stream.getvalue()
    assert reports[0].state == "failed"
    assert reports[1].state == "success"


def test_watch_dag_file_full_rerun_after_success(tmp_path: Path) -> None:
    dag_file = tmp_path / "demo.py"
    dag_file.write_text("# placeholder")

    calls: list[dict[str, Any]] = []

    def fake_runner(path: str, *, dag_id: str | None = None, **kwargs: Any) -> RunResult:
        calls.append(kwargs)
        return RunResult(
            dag_id="demo",
            state="success",
            tasks=[TaskRunInfo(task_id="only", state="success")],
        )

    iteration_state = {"count": 0}

    def fake_sleep(seconds: float) -> None:
        iteration_state["count"] += 1
        if iteration_state["count"] >= 1:
            import os
            stat = dag_file.stat()
            os.utime(dag_file, (stat.st_atime, stat.st_mtime + 100))

    stream = io.StringIO()
    watch.watch_dag_file(
        str(dag_file),
        runner=fake_runner,
        reporter=lambda result: None,
        stream=stream,
        sleep=fake_sleep,
        max_iterations=2,
    )

    assert len(calls) == 2
    assert "start_task_ids" not in calls[1] or calls[1].get("start_task_ids") in (None, [])


def test_watch_dag_file_rejects_non_positive_interval(tmp_path: Path) -> None:
    dag_file = tmp_path / "demo.py"
    dag_file.write_text("# placeholder")

    with pytest.raises(ValueError, match="poll_interval"):
        watch.watch_dag_file(str(dag_file), poll_interval=0)


def test_debug_dag_file_cli_dispatches_to_watch(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dag_file = tmp_path / "demo.py"
    dag_file.write_text("# placeholder")

    captured: dict[str, Any] = {}

    def fake_watch(dag_file: str, **kwargs: Any) -> RunResult:
        captured["dag_file"] = dag_file
        captured["kwargs"] = kwargs
        return RunResult(dag_id="demo", state="success")

    import airflow_local_debug.watch as watch_module

    monkeypatch.setattr(watch_module, "watch_dag_file", fake_watch)

    from airflow_local_debug.runner import debug_dag_file_cli

    debug_dag_file_cli(
        argv=[
            str(dag_file),
            "--watch",
            "--watch-path",
            str(tmp_path / "sql"),
            "--watch-interval",
            "1.5",
        ]
    )

    assert captured["dag_file"] == str(dag_file)
    assert captured["kwargs"]["poll_interval"] == 1.5
    assert captured["kwargs"]["watch_paths"] == [str(tmp_path / "sql")]


def test_watch_dag_file_writes_artifacts_each_iteration(tmp_path: Path) -> None:
    """P1 regression: --report-dir / --xcom-json-path must produce files in watch mode."""
    dag_file = tmp_path / "demo.py"
    dag_file.write_text("# placeholder")

    def fake_runner(path: str, *, dag_id: str | None = None, **kwargs: Any) -> RunResult:
        return RunResult(
            dag_id="demo",
            state="success",
            tasks=[TaskRunInfo(task_id="only", state="success")],
            xcoms={"only": {"return_value": 42}},
        )

    iteration_state = {"count": 0}

    def fake_sleep(seconds: float) -> None:
        iteration_state["count"] += 1
        if iteration_state["count"] >= 1:
            import os

            stat = dag_file.stat()
            os.utime(dag_file, (stat.st_atime, stat.st_mtime + 100))

    report_dir = tmp_path / "report"
    xcom_path = tmp_path / "xcoms.json"
    stream = io.StringIO()

    watch.watch_dag_file(
        str(dag_file),
        runner=fake_runner,
        reporter=lambda r: None,
        stream=stream,
        sleep=fake_sleep,
        max_iterations=2,
        report_dir=report_dir,
        xcom_json_path=str(xcom_path),
    )

    assert (report_dir / "result.json").exists()
    assert (report_dir / "report.md").exists()
    assert xcom_path.exists()


def test_debug_dag_file_cli_forwards_artifact_flags_to_watch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """P1 regression: --report-dir et al. must reach watch_dag_file, not be silently dropped."""
    dag_file = tmp_path / "demo.py"
    dag_file.write_text("# placeholder")

    captured: dict[str, Any] = {}

    def fake_watch(file_path: str, **kwargs: Any) -> RunResult:
        captured.update(kwargs)
        return RunResult(dag_id="demo", state="success")

    import airflow_local_debug.watch as watch_module

    monkeypatch.setattr(watch_module, "watch_dag_file", fake_watch)

    from airflow_local_debug.runner import debug_dag_file_cli

    debug_dag_file_cli(
        argv=[
            str(dag_file),
            "--watch",
            "--report-dir",
            str(tmp_path / "report"),
            "--xcom-json-path",
            str(tmp_path / "xc.json"),
            "--graph-svg-path",
            str(tmp_path / "g.svg"),
            "--include-graph-in-report",
        ]
    )

    assert captured["report_dir"] == str(tmp_path / "report")
    assert captured["xcom_json_path"] == str(tmp_path / "xc.json")
    assert captured["graph_svg_path"] == str(tmp_path / "g.svg")
    assert captured["include_graph_in_report"] is True
