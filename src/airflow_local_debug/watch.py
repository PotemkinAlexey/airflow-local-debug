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

from airflow_local_debug.models import RunResult

_WATCH_FILE_SUFFIXES = {".py", ".sql", ".yaml", ".yml", ".json"}
_FAILED_TASK_STATES = {"failed", "up_for_retry", "shutdown"}


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
        if token in _FAILED_TASK_STATES:
            return task.task_id
    return None


def purge_module_cache(roots: Iterable[Path]) -> int:
    """Drop sys.modules entries whose source file lives under any watch root.

    Returns the number of dropped modules. This forces a fresh re-import on
    the next runner call so helper modules picked up via the DAG file are
    reloaded too.
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
    runner: Any = None,
    reporter: Any = None,
    stream: IO[str] | None = None,
    sleep: Any = None,
    max_iterations: int | None = None,
    **runner_kwargs: Any,
) -> RunResult:
    """Run the DAG file in a hot-reload loop until the user interrupts.

    Returns the most recent `RunResult`. The optional `runner`, `reporter`,
    `sleep`, and `max_iterations` arguments exist primarily so tests can
    drive the loop deterministically without real I/O.
    """
    if poll_interval <= 0:
        raise ValueError("poll_interval must be positive.")

    if runner is None:
        from airflow_local_debug.runner import run_full_dag_from_file as runner
    if reporter is None:
        from airflow_local_debug.report import print_run_report as reporter
    if sleep is None:
        sleep = time.sleep

    output = stream or sys.stdout
    roots = resolve_watch_roots(dag_file, list(watch_paths or []))
    last_failed_task: str | None = None
    last_result: RunResult | None = None
    iterations = 0

    output.write(f"[watch] watching {len(roots)} path(s); save a file to retry, Ctrl+C to exit\n")
    output.flush()

    while True:
        if iterations:
            purge_module_cache(roots)

        kwargs = dict(runner_kwargs)
        if last_failed_task is not None:
            output.write(f"[watch] re-running starting from failed task: {last_failed_task}\n")
            kwargs["start_task_ids"] = [last_failed_task]
            kwargs.pop("task_ids", None)
            kwargs.pop("task_group_ids", None)

        output.flush()
        try:
            result = runner(dag_file, dag_id=dag_id, **kwargs)
        except KeyboardInterrupt:
            output.write("\n[watch] interrupted\n")
            output.flush()
            return last_result or RunResult(dag_id=dag_id or "<unknown>")

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
            return last_result

        try:
            _wait_for_change(roots, poll_interval=poll_interval, sleep=sleep, stream=output)
        except KeyboardInterrupt:
            output.write("\n[watch] exiting\n")
            output.flush()
            return last_result


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
