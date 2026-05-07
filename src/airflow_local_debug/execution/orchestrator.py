"""
End-to-end DAG execution orchestrator.

`execute_full_dag` wires the pieces together: partial-run selection,
graph rendering, plugin manager, env bootstrap, fail-fast retry policy,
task mocks, live trace, the strict / dag.test / dag.run backends, and
result-building. The public API in `airflow_local_debug.runner` is a
thin layer on top of this function.
"""

from __future__ import annotations

import logging
import traceback
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from airflow_local_debug.cli.loaders import coerce_logical_date
from airflow_local_debug.compat import (
    build_dag_test_kwargs,
    build_legacy_dag_run_kwargs,
    get_airflow_version,
    has_dag_test,
)
from airflow_local_debug.config.env import bootstrap_airflow_env
from airflow_local_debug.execution.deferrables import detect_deferrable_tasks, format_deferrable_note
from airflow_local_debug.execution.mocks import TaskMockRegistry, TaskMockRule, local_task_mocks
from airflow_local_debug.execution.partial_runs import (
    build_partial_selection_note,
    detect_external_upstreams,
    format_external_upstream_note,
    partial_dag_for_selected_tasks,
    resolve_partial_task_ids,
)
from airflow_local_debug.execution.result import (
    best_effort_last_dagrun,
    failed_task_label,
    result_from_dagrun,
)
from airflow_local_debug.execution.state import serialize_datetime
from airflow_local_debug.execution.strict_loop import strict_dag_test
from airflow_local_debug.models import LocalConfig, RunResult
from airflow_local_debug.plugins import (
    AirflowDebugPlugin,
    ConsoleTracePlugin,
    DebugPluginManager,
    ProblemLogPlugin,
    TaskContextPlugin,
)
from airflow_local_debug.reporting.console import print_run_preamble
from airflow_local_debug.reporting.graph import format_dag_graph
from airflow_local_debug.reporting.live_trace import live_task_trace
from airflow_local_debug.reporting.traceback_utils import format_pretty_exception

_log = logging.getLogger(__name__)


def write_report_artifacts(result: RunResult, report_dir: str | Path, *, include_graph: bool) -> None:
    """Persist `result.json`, `report.md`, `tasks.csv`, etc. and add a note."""
    from airflow_local_debug.reporting.report import write_run_artifacts

    resolved_report_dir = Path(report_dir).expanduser()
    resolved_report_dir.mkdir(parents=True, exist_ok=True)
    resolved_report_dir = resolved_report_dir.resolve()
    result.notes.append(f"Wrote run artifacts to {resolved_report_dir}")
    write_run_artifacts(result, resolved_report_dir, include_graph=include_graph)


def resolve_graph_svg_path(
    *,
    report_dir: str | Path | None,
    graph_svg_path: str | Path | None,
) -> str | Path | None:
    """Pick the SVG output path: explicit flag wins, else default to report_dir/graph.svg."""
    if graph_svg_path is not None:
        return graph_svg_path
    if report_dir is None:
        return None
    return Path(report_dir).expanduser() / "graph.svg"


def attach_graph_svg(dag: Any, result: RunResult, graph_svg_path: str | Path | None) -> None:
    """Render and write the DAG SVG, recording the path on the result."""
    if graph_svg_path is None:
        return

    from airflow_local_debug.reporting.graph import write_dag_svg

    try:
        result.graph_svg_path = write_dag_svg(dag, str(graph_svg_path))
    except Exception as exc:
        result.notes.append(f"Could not write DAG graph SVG to {Path(graph_svg_path).expanduser()}: {exc}")


def build_graph_ascii(dag: Any, notes: list[str]) -> str | None:
    """Render the ASCII graph, recording any failure as a note instead of raising."""
    try:
        return format_dag_graph(dag)
    except Exception as exc:
        notes.append(f"Graph rendering skipped: {exc}")
        return None


def backend_hint(dag: Any, *, fail_fast: bool = False) -> str:
    """Decide which Airflow execution backend will be used for this dag."""
    if has_dag_test(dag):
        return "dag.test.strict" if fail_fast else "dag.test"
    if hasattr(dag, "run") and callable(getattr(dag, "run", None)):
        return "dag.run"
    return "unsupported"


def bootstrap_pools(local_config: LocalConfig, notes: list[str]) -> None:
    """Insert / update Airflow pools from the local config."""
    if not local_config.pools:
        return

    try:
        from airflow.models.pool import Pool
        from airflow.utils.session import create_session
    except Exception as exc:
        notes.append(f"Skipping pool bootstrap: {exc}")
        return

    try:
        with create_session() as session:
            for pool_name, pool_payload in local_config.pools.items():
                slots = int(pool_payload.get("slots", 1))
                description = pool_payload.get("description") or "Loaded by airflow_debug"
                include_deferred = pool_payload.get("include_deferred")

                pool = session.query(Pool).filter(Pool.pool == pool_name).one_or_none()
                if pool is None:
                    pool = Pool(pool=pool_name, slots=slots, description=description)
                    session.add(pool)
                else:
                    pool.slots = slots
                    pool.description = description

                if include_deferred is not None and hasattr(pool, "include_deferred"):
                    pool.include_deferred = include_deferred

            session.commit()
        notes.append(f"Bootstrapped {len(local_config.pools)} pool(s) from local config.")
    except Exception as exc:
        notes.append(f"Pool bootstrap failed: {exc}")


@contextmanager
def local_task_policy(
    dag: Any,
    *,
    fail_fast: bool,
    notes: list[str],
) -> Iterator[None]:
    """Temporarily zero out task retries for the duration of a fail-fast run."""
    if not fail_fast:
        yield
        return

    originals: dict[int, dict[str, Any]] = {}
    patched_count = 0
    for task in list(getattr(dag, "task_dict", {}).values()):
        original: dict[str, Any] = {}
        for attr_name in ("retries", "retry_delay", "retry_exponential_backoff", "max_retry_delay"):
            if hasattr(task, attr_name):
                original[attr_name] = getattr(task, attr_name)
        if not original:
            continue
        originals[id(task)] = original
        patched_count += 1
        if "retries" in original:
            task.retries = 0
        if "retry_delay" in original:
            task.retry_delay = timedelta(0)
        if "retry_exponential_backoff" in original:
            task.retry_exponential_backoff = False
        if "max_retry_delay" in original:
            task.max_retry_delay = timedelta(0)

    if patched_count:
        notes.append(f"Fail-fast mode enabled for local debug; disabled retries on {patched_count} task(s).")
    try:
        yield
    finally:
        for task in list(getattr(dag, "task_dict", {}).values()):
            saved_original = originals.get(id(task))
            if not saved_original:
                continue
            for attr_name, value in saved_original.items():
                setattr(task, attr_name, value)


def build_plugin_manager(
    *,
    trace: bool,
    plugins: Iterable[AirflowDebugPlugin] | None,
    notes: list[str],
) -> DebugPluginManager:
    """Construct the per-run plugin dispatcher with default plugins prepended."""
    user_plugins = list(plugins or [])
    user_types = {type(plugin) for plugin in user_plugins}

    active_plugins: list[AirflowDebugPlugin] = []
    if TaskContextPlugin not in user_types:
        active_plugins.append(TaskContextPlugin())
    if ProblemLogPlugin not in user_types:
        active_plugins.append(ProblemLogPlugin())
    if trace and ConsoleTracePlugin not in user_types:
        active_plugins.append(ConsoleTracePlugin())
    active_plugins.extend(user_plugins)
    return DebugPluginManager(active_plugins, notes=notes)


def _error_result(
    *,
    dag: Any,
    dagrun: Any,
    config_path: str | None,
    notes: list[str],
    graph_ascii: str | None,
    backend: str | None,
    exc: BaseException,
    error_raw: str,
    task_mock_registry: TaskMockRegistry | None = None,
    collect_xcoms: bool = False,
    deferrables: list[Any] | None = None,
    selected_tasks: list[str] | None = None,
) -> RunResult:
    return result_from_dagrun(
        dag,
        dagrun,
        config_path=config_path,
        notes=notes,
        graph_ascii=graph_ascii,
        backend=backend,
        exception=format_pretty_exception(
            exc,
            task_id=failed_task_label(dagrun) or getattr(dag, "dag_id", None),
        ),
        exception_raw=error_raw,
        task_mock_registry=task_mock_registry,
        collect_xcoms=collect_xcoms,
        deferrables=deferrables,
        selected_tasks=selected_tasks,
    )


def execute_full_dag(
    dag: Any,
    *,
    local_config: LocalConfig,
    config_path: str | None,
    logical_date: str | date | datetime | None,
    conf: dict[str, Any] | None,
    extra_env: dict[str, str] | None,
    trace: bool,
    fail_fast: bool,
    plugins: Iterable[AirflowDebugPlugin] | None,
    task_mocks: Iterable[TaskMockRule] | None,
    collect_xcoms: bool,
    notes: list[str],
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
    graph_svg_path: str | Path | None = None,
) -> RunResult:
    """Run a DAG end-to-end and return a `RunResult`. The single orchestrator entry point."""
    backend: str | None = None
    dagrun = None
    result: RunResult | None = None
    pending_base_exception: BaseException | None = None
    task_mock_registry: TaskMockRegistry | None = None
    selected_task_ids: list[str] = []
    run_logical_date = coerce_logical_date(logical_date)
    task_mock_rules = list(task_mocks or [])
    graph_ascii: str | None = None
    deferrables: list[Any] = []
    run_context: dict[str, Any] = {}
    plugin_manager: DebugPluginManager | None = None
    try:
        selected = resolve_partial_task_ids(
            dag,
            task_ids=task_ids,
            start_task_ids=start_task_ids,
            task_group_ids=task_group_ids,
        )
        if selected is not None:
            selected_task_ids = selected
            notes.append(build_partial_selection_note(dag, selected_task_ids))
            external_upstreams = detect_external_upstreams(dag, selected_task_ids)
            mocked_external = {
                task_id: ups
                for task_id, ups in external_upstreams.items()
                if any(rule.task_id in ups for rule in task_mock_rules)
            }
            unmocked_external = {
                task_id: ups
                for task_id, ups in external_upstreams.items()
                if not any(rule.task_id in ups for rule in task_mock_rules)
            }
            if unmocked_external:
                notes.append(format_external_upstream_note(unmocked_external))
            if mocked_external:
                covered = sorted({up for ups in mocked_external.values() for up in ups})
                notes.append(
                    f"Partial run upstream XCom mocks active for: {', '.join(covered)}."
                )
            dag = partial_dag_for_selected_tasks(dag, selected_task_ids)

        graph_ascii = build_graph_ascii(dag, notes)
        hint = backend_hint(dag, fail_fast=fail_fast)
        deferrables = detect_deferrable_tasks(dag, backend_hint=hint)
        deferrable_note = format_deferrable_note(deferrables)
        if deferrable_note:
            notes.append(deferrable_note)
        run_context = {
            "config_path": config_path,
            "logical_date": serialize_datetime(run_logical_date),
            "conf": dict(conf or {}),
            "extra_env": dict(extra_env or {}),
            "backend_hint": hint,
            "graph_ascii": graph_ascii,
            "task_mocks": [rule.describe() for rule in task_mock_rules],
            "selected_tasks": list(selected_task_ids),
            "deferrables": [info.task_id for info in deferrables],
            "notes": notes,
        }
        plugin_manager = build_plugin_manager(trace=trace, plugins=plugins, notes=notes)
        print_run_preamble(
            dag,
            backend_hint=run_context["backend_hint"],
            config_path=config_path,
            logical_date=run_context["logical_date"],
            graph_text=graph_ascii,
        )
        plugin_manager.before_run(dag, run_context)
        with bootstrap_airflow_env(config=local_config, extra_env=extra_env), local_task_policy(
            dag,
            fail_fast=fail_fast,
            notes=notes,
        ), local_task_mocks(
            dag,
            task_mock_rules,
            notes=notes,
        ) as task_mock_registry, live_task_trace(
            dag,
            plugin_manager=plugin_manager,
            wrap_task_methods=not fail_fast,
        ) as trace_session:
            bootstrap_pools(local_config, notes)
            if has_dag_test(dag):
                kwargs = build_dag_test_kwargs(dag, run_logical_date, conf)
                if fail_fast:
                    backend = "dag.test.strict"
                    notes.append("Using strict local dag.test loop for deterministic fail-fast execution.")
                    dagrun = strict_dag_test(
                        dag,
                        execution_date=kwargs.get("logical_date") or kwargs.get("execution_date"),
                        run_conf=kwargs.get("run_conf") or kwargs.get("conf"),
                        trace_session=trace_session,
                    )
                else:
                    backend = "dag.test"
                    dagrun = dag.test(**kwargs)
                result = result_from_dagrun(
                    dag,
                    dagrun,
                    config_path=config_path,
                    notes=notes,
                    graph_ascii=graph_ascii,
                    backend=backend,
                    task_mock_registry=task_mock_registry,
                    collect_xcoms=collect_xcoms,
                    deferrables=deferrables,
                    selected_tasks=selected_task_ids,
                )
                return result

            if hasattr(dag, "run") and callable(getattr(dag, "run", None)):
                backend = "dag.run"
                notes.append(
                    "Falling back to legacy dag.run(); behavior may differ slightly from dag.test()."
                )

                if hasattr(dag, "clear") and callable(getattr(dag, "clear", None)) and run_logical_date is not None:
                    dag.clear(start_date=run_logical_date, end_date=run_logical_date)

                kwargs = build_legacy_dag_run_kwargs(dag, run_logical_date, conf)
                dag.run(**kwargs)
                dagrun = best_effort_last_dagrun(dag)
                result = result_from_dagrun(
                    dag,
                    dagrun,
                    config_path=config_path,
                    notes=notes,
                    graph_ascii=graph_ascii,
                    backend=backend,
                    task_mock_registry=task_mock_registry,
                    collect_xcoms=collect_xcoms,
                    deferrables=deferrables,
                    selected_tasks=selected_task_ids,
                )
                return result

            result = RunResult(
                dag_id=getattr(dag, "dag_id", "<unknown>"),
                backend="unsupported",
                airflow_version=get_airflow_version(),
                config_path=config_path,
                graph_ascii=graph_ascii,
                selected_tasks=selected_task_ids,
                deferrables=deferrables,
                notes=notes,
                exception="This Airflow runtime exposes neither dag.test() nor dag.run().",
                exception_raw="This Airflow runtime exposes neither dag.test() nor dag.run().",
            )
            return result
    except Exception as exc:
        error_raw = traceback.format_exc()
        dagrun = dagrun or best_effort_last_dagrun(dag)
        result = _error_result(
            dag=dag,
            dagrun=dagrun,
            config_path=config_path,
            notes=notes,
            graph_ascii=graph_ascii,
            backend=backend,
            exc=exc,
            error_raw=error_raw,
            task_mock_registry=task_mock_registry,
            collect_xcoms=collect_xcoms,
            deferrables=deferrables,
            selected_tasks=selected_task_ids,
        )
    except BaseException as exc:
        # SystemExit(0) is a clean shutdown, not a DAG failure — re-raise without
        # constructing an error result so the user-visible state stays neutral.
        if isinstance(exc, SystemExit) and (exc.code in (None, 0)):
            pending_base_exception = exc
        else:
            error_raw = traceback.format_exc()
            dagrun = dagrun or best_effort_last_dagrun(dag)
            result = _error_result(
                dag=dag,
                dagrun=dagrun,
                config_path=config_path,
                notes=notes,
                graph_ascii=graph_ascii,
                backend=backend,
                exc=exc,
                error_raw=error_raw,
                task_mock_registry=task_mock_registry,
                collect_xcoms=collect_xcoms,
                deferrables=deferrables,
                selected_tasks=selected_task_ids,
            )
            pending_base_exception = exc
    finally:
        if result is None:
            result = RunResult(
                dag_id=getattr(dag, "dag_id", "<unknown>"),
                backend=backend,
                airflow_version=get_airflow_version(),
                config_path=config_path,
                graph_ascii=graph_ascii,
                selected_tasks=selected_task_ids,
                notes=notes,
                exception="Local DAG run did not produce a result.",
                exception_raw="Local DAG run did not produce a result.",
            )
        attach_graph_svg(dag, result, graph_svg_path)
        if plugin_manager is not None:
            plugin_manager.after_run(dag, run_context, result)

    if pending_base_exception is not None:
        raise pending_base_exception
    return result
