"""
File watch / hot-reload loop for the file-based debug runner.

`watch_dag_file` runs a DAG, reports the result, then polls a set of watched
paths for mtime changes. On any change it purges cached modules, re-imports
the DAG file, and re-runs:

- if the previous run failed at a task, the next run uses
  `start_task_ids=[failed_task]` so only the failed subgraph re-executes;
- if the previous run succeeded, the next run is a full re-run.

The watcher uses periodic polling rather than a native filesystem API so that
no extra dependencies are required.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Iterable
from pathlib import Path
from typing import IO, Any

from airflow_local_debug.execution.state import HARD_FAILED_TASK_STATES
from airflow_local_debug.models import RunResult

_WATCH_FILE_SUFFIXES = {".py", ".sql", ".yaml", ".yml", ".json"}


def resolve_watch_roots(dag_file: str, extra: Iterable[str] | None = None) -> list[Path]:
    """Return absolute paths to watch: the DAG file, its parent dir, and extras."""
    dag_path = Path(dag_file).expanduser().resolve()
    roots: list[Path] = [dag_path]
    seen: set[Path] = {dag_path}

    parent = dag_path.parent
    if parent not in seen:
        roots.append(parent)
        seen.add(parent)

    for path in extra or []:
        resolved = Path(path).expanduser().resolve()
        if resolved in seen:
            continue
        roots.append(resolved)
        seen.add(resolved)

    return roots


def snapshot_mtimes(roots: Iterable[Path]) -> dict[Path, float]:
    """Snapshot mtimes of all watched files under the given roots."""
    snapshot: dict[Path, float] = {}
    for root in roots:
        if root.is_file():
            try:
                snapshot[root] = root.stat().st_mtime
            except OSError:
                continue
            continue
        if not root.is_dir():
            continue
        for entry in root.rglob("*"):
            if not entry.is_file():
                continue
            if entry.suffix.lower() not in _WATCH_FILE_SUFFIXES:
                continue
            try:
                snapshot[entry] = entry.stat().st_mtime
            except OSError:
                continue
    return snapshot


def diff_snapshots(baseline: dict[Path, float], current: dict[Path, float]) -> list[Path]:
    """Return paths that were added, removed, or modified between snapshots."""
    changed: list[Path] = []
    for path, mtime in current.items():
        if path not in baseline or baseline[path] != mtime:
            changed.append(path)
    for path in baseline:
        if path not in current:
            changed.append(path)
    return sorted(changed)


def first_failed_task_id(result: RunResult) -> str | None:
    """Return the first task id whose state indicates a hard failure."""
    for task in result.tasks:
        token = (task.state or "").strip().lower()
        if token in HARD_FAILED_TASK_STATES:
            return task.task_id
    return None


def _modules_under_watch_roots(names: set[str], roots: Iterable[Path]) -> set[str]:
    """Filter `names` to those whose `__file__` lives under a watch root.

    Used to scope the snapshot diff so unrelated lazy-imported modules
    (Airflow providers loaded on first use, etc.) are not also dropped.
    """
    target_prefixes: list[str] = []
    for root in roots:
        try:
            resolved = root.resolve()
        except OSError:
            continue
        if resolved.is_dir():
            target_prefixes.append(str(resolved) + "/")
        elif resolved.is_file():
            target_prefixes.append(str(resolved.parent) + "/")
    if not target_prefixes:
        return set()

    matched: set[str] = set()
    for name in names:
        module = sys.modules.get(name)
        if module is None:  # type: ignore[unreachable]
            continue
        module_file = getattr(module, "__file__", None)
        if not module_file:
            continue
        for prefix in target_prefixes:
            if module_file.startswith(prefix):
                matched.add(name)
                break
    return matched


def purge_module_cache(roots: Iterable[Path]) -> int:
    """Drop sys.modules entries whose source file lives under any watch root.

    Returns the number of dropped modules. Kept as a public utility for
    external callers; the watch loop itself uses a snapshot-diff strategy
    (see `_modules_under_watch_roots`) which is O(N_dag_modules) instead
    of O(all sys.modules).
    """
    targets: list[Path] = []
    for root in roots:
        try:
            resolved = root.resolve()
        except OSError:
            continue
        if resolved.is_dir():
            targets.append(resolved)
        elif resolved.is_file():
            targets.append(resolved.parent)
    if not targets:
        return 0

    to_drop: list[str] = []
    for name, module in list(sys.modules.items()):
        if module is None:  # type: ignore[unreachable]
            continue  # type: ignore[unreachable]
        module_file = getattr(module, "__file__", None)
        if not module_file:
            continue
        try:
            module_path = Path(module_file).resolve()
        except OSError:
            continue
        for target in targets:
            try:
                module_path.relative_to(target)
            except ValueError:
                continue
            to_drop.append(name)
            break

    for name in to_drop:
        sys.modules.pop(name, None)
    return len(to_drop)


def watch_dag_file(
    dag_file: str,
    *,
    dag_id: str | None = None,
    watch_paths: Iterable[str] | None = None,
    poll_interval: float = 0.5,
    report_dir: str | Path | None = None,
    include_graph_in_report: bool = False,
    xcom_json_path: str | Path | None = None,
    graph_svg_path: str | Path | None = None,
    runner: Any = None,
    reporter: Any = None,
    stream: IO[str] | None = None,
    sleep: Any = None,
    max_iterations: int | None = None,
    **runner_kwargs: Any,
) -> RunResult:
    """Run the DAG file in a hot-reload loop until the user interrupts.

    Returns the most recent `RunResult`. After every iteration, the same
    artefact flags as `debug_dag_from_file` are honoured: `report_dir` writes
    `result.json` / `report.md` / `tasks.csv` etc., `xcom_json_path` writes a
    standalone XCom snapshot, `graph_svg_path` writes an explicit SVG path
    (defaults to `report_dir/graph.svg` when only `report_dir` is set).

    The optional `runner`, `reporter`, `sleep`, and `max_iterations`
    arguments exist primarily so tests can drive the loop deterministically
    without real I/O.
    """
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive.")

    if runner is None:
        from airflow_local_debug.runner import run_full_dag_from_file as runner
    if reporter is None:
        from airflow_local_debug.reporting.report import print_run_report as reporter
    if sleep is None:
        sleep = time.sleep

    from airflow_local_debug.execution.orchestrator import resolve_graph_svg_path, write_report_artifacts
    from airflow_local_debug.reporting.report import write_xcom_snapshot

    resolved_graph_svg_path = resolve_graph_svg_path(report_dir=report_dir, graph_svg_path=graph_svg_path)
    if resolved_graph_svg_path is not None:
        runner_kwargs.setdefault("graph_svg_path", resolved_graph_svg_path)
    if report_dir is not None or xcom_json_path is not None:
        runner_kwargs["collect_xcoms"] = True

    output = stream or sys.stdout
    roots = resolve_watch_roots(dag_file, list(watch_paths or []))
    last_failed_task: str | None = None
    last_result: RunResult | None = None
    dag_loaded_modules: set[str] = set()
    iterations = 0

    output.write(f"[watch] watching {len(roots)} path(s); save a file to retry, Ctrl+C to exit\n")
    output.flush()

    while True:
        if iterations and dag_loaded_modules:
            for name in dag_loaded_modules:
                sys.modules.pop(name, None)
            dag_loaded_modules = set()

        kwargs = dict(runner_kwargs)
        if last_failed_task is not None:
            output.write(f"[watch] re-running starting from failed task: {last_failed_task}\n")
            kwargs["start_task_ids"] = [last_failed_task]
            kwargs.pop("task_ids", None)
            kwargs.pop("task_group_ids", None)

        output.flush()
        modules_before = set(sys.modules)
        try:
            result: RunResult = runner(dag_file, dag_id=dag_id, **kwargs)
        except KeyboardInterrupt:
            output.write("\n[watch] interrupted\n")
            output.flush()
            return last_result or RunResult(dag_id=dag_id or "<unknown>")
        # Track only modules whose source file lives under a watch root, so we
        # do not blow away unrelated lazy-imported third-party modules between
        # iterations.
        dag_loaded_modules = _modules_under_watch_roots(set(sys.modules) - modules_before, roots)

        if xcom_json_path is not None:
            xcom_path = write_xcom_snapshot(result, xcom_json_path)
            result.notes.append(f"Wrote XCom snapshot to {xcom_path}")
        if report_dir is not None:
            write_report_artifacts(result, report_dir, include_graph=include_graph_in_report)

        reporter(result)
        last_result = result

        if result.ok:
            last_failed_task = None
            output.write("[watch] run succeeded. Waiting for file changes...\n")
        else:
            failed = first_failed_task_id(result)
            last_failed_task = failed
            if failed is not None:
                output.write(
                    f"[watch] run failed at task '{failed}'. Save a fix to retry from this task...\n"
                )
            else:
                output.write("[watch] run failed before any task started. Waiting for file changes...\n")
        output.flush()

        iterations += 1
        if max_iterations is not None and iterations >= max_iterations:
            return result

        try:
            # Tests inject a custom `sleep`; force polling in that case so
            # the test loop stays deterministic.
            using_default_sleep = sleep is time.sleep
            if using_default_sleep and _try_wait_via_watchdog(roots, stream=output):
                pass
            else:
                _wait_for_change(roots, poll_interval=poll_interval, sleep=sleep, stream=output)
        except KeyboardInterrupt:
            output.write("\n[watch] exiting\n")
            output.flush()
            return last_result or RunResult(dag_id=dag_id or "<unknown>")


def _try_wait_via_watchdog(roots: list[Path], *, stream: IO[str]) -> bool:
    """Block until any watched path changes, using `watchdog` if installed.

    Returns True when watchdog handled the wait; False when watchdog is
    unavailable (caller falls back to polling).
    """
    try:
        from watchdog.events import FileSystemEvent, FileSystemEventHandler
        from watchdog.observers import Observer
    except ImportError:
        return False

    import threading

    changed = threading.Event()

    class _Handler(FileSystemEventHandler):  # type: ignore[misc]
        def on_any_event(self, event: FileSystemEvent) -> None:
            if event.event_type in ("modified", "created", "deleted", "moved"):
                changed.set()

    handler = _Handler()
    observer = Observer()
    scheduled = 0
    seen_dirs: set[str] = set()
    for root in roots:
        try:
            target_dir = str(root if root.is_dir() else root.parent)
        except OSError:
            continue
        if target_dir in seen_dirs:
            continue
        seen_dirs.add(target_dir)
        observer.schedule(handler, target_dir, recursive=root.is_dir())
        scheduled += 1
    if not scheduled:
        return False

    observer.start()
    try:
        changed.wait()
    finally:
        observer.stop()
        observer.join()
    stream.write("[watch] detected change (watchdog); reloading...\n")
    stream.flush()
    return True


def _wait_for_change(
    roots: list[Path],
    *,
    poll_interval: float,
    sleep: Any,
    stream: IO[str],
) -> list[Path]:
    baseline = snapshot_mtimes(roots)
    while True:
        sleep(poll_interval)
        current = snapshot_mtimes(roots)
        changed = diff_snapshots(baseline, current)
        if changed:
            preview = ", ".join(str(path) for path in changed[:3])
            extra = f" (+{len(changed) - 3} more)" if len(changed) > 3 else ""
            stream.write(f"[watch] detected change in {preview}{extra}; reloading...\n")
            stream.flush()
            return changed
