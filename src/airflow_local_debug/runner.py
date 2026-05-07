"""
Core entrypoints for local Airflow DAG execution.

This module keeps execution as close to native Airflow as possible:
- bootstrap local env from config file
- render the DAG graph before execution
- run the whole DAG via `dag.test()` when available
- fall back to legacy `dag.run()` for older Airflow versions
- return a structured `RunResult`

Use `debug_dag(...)` when you want a single call that both runs the DAG and
prints the final report. Use `run_full_dag(...)` when you want the raw result
object and will decide yourself how to render it.
"""

from __future__ import annotations

import argparse
import logging
import traceback
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from airflow_local_debug.cli.args import add_common_run_args, add_watch_args
from airflow_local_debug.cli.loaders import (
    coerce_logical_date,
    load_cli_conf,
    load_cli_env_files,
    load_cli_extra_env,
    load_cli_selector_values,
    load_cli_task_mocks,
)
from airflow_local_debug.compat import (
    build_dag_test_kwargs,
    build_legacy_dag_run_kwargs,
    get_airflow_version,
    has_dag_test,
)
from airflow_local_debug.config.env import bootstrap_airflow_env
from airflow_local_debug.config.loader import get_default_config_path, load_local_config
from airflow_local_debug.execution.dag_loader import (
    dag_candidates_from_module,
    dag_file_info,
    format_dag_list,
    load_module_from_file,
    resolve_dag_from_module,
)
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
from airflow_local_debug.execution.state import (
    serialize_datetime,
)
from airflow_local_debug.execution.strict_loop import strict_dag_test as _strict_dag_test
from airflow_local_debug.models import DagFileInfo, LocalConfig, RunResult
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



def _write_report_artifacts(result: RunResult, report_dir: str | Path, *, include_graph: bool) -> None:
    from airflow_local_debug.reporting.report import write_run_artifacts

    resolved_report_dir = Path(report_dir).expanduser()
    resolved_report_dir.mkdir(parents=True, exist_ok=True)
    resolved_report_dir = resolved_report_dir.resolve()
    result.notes.append(f"Wrote run artifacts to {resolved_report_dir}")
    write_run_artifacts(result, resolved_report_dir, include_graph=include_graph)


def _resolve_graph_svg_path(
    *,
    report_dir: str | Path | None,
    graph_svg_path: str | Path | None,
) -> str | Path | None:
    if graph_svg_path is not None:
        return graph_svg_path
    if report_dir is None:
        return None
    return Path(report_dir).expanduser() / "graph.svg"


def _attach_graph_svg(dag: Any, result: RunResult, graph_svg_path: str | Path | None) -> None:
    if graph_svg_path is None:
        return

    from airflow_local_debug.reporting.graph import write_dag_svg

    try:
        result.graph_svg_path = write_dag_svg(dag, str(graph_svg_path))
    except Exception as exc:
        result.notes.append(f"Could not write DAG graph SVG to {Path(graph_svg_path).expanduser()}: {exc}")



def _build_graph_ascii(dag: Any, notes: list[str]) -> str | None:
    try:
        return format_dag_graph(dag)
    except Exception as exc:
        notes.append(f"Graph rendering skipped: {exc}")
        return None


def _backend_hint(dag: Any, *, fail_fast: bool = False) -> str:
    if has_dag_test(dag):
        return "dag.test.strict" if fail_fast else "dag.test"
    if hasattr(dag, "run") and callable(getattr(dag, "run", None)):
        return "dag.run"
    return "unsupported"


def _bootstrap_pools(local_config: LocalConfig, notes: list[str]) -> None:
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
def _local_task_policy(
    dag: Any,
    *,
    fail_fast: bool,
    notes: list[str],
) -> Iterator[None]:
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


def _build_plugin_manager(
    *,
    trace: bool,
    plugins: Iterable[AirflowDebugPlugin] | None,
    notes: list[str],
) -> DebugPluginManager:
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


def _execute_full_dag(
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

        graph_ascii = _build_graph_ascii(dag, notes)
        backend_hint = _backend_hint(dag, fail_fast=fail_fast)
        deferrables = detect_deferrable_tasks(dag, backend_hint=backend_hint)
        deferrable_note = format_deferrable_note(deferrables)
        if deferrable_note:
            notes.append(deferrable_note)
        run_context = {
            "config_path": config_path,
            "logical_date": serialize_datetime(run_logical_date),
            "conf": dict(conf or {}),
            "extra_env": dict(extra_env or {}),
            "backend_hint": backend_hint,
            "graph_ascii": graph_ascii,
            "task_mocks": [rule.describe() for rule in task_mock_rules],
            "selected_tasks": list(selected_task_ids),
            "deferrables": [info.task_id for info in deferrables],
            "notes": notes,
        }
        plugin_manager = _build_plugin_manager(trace=trace, plugins=plugins, notes=notes)
        print_run_preamble(
            dag,
            backend_hint=run_context["backend_hint"],
            config_path=config_path,
            logical_date=run_context["logical_date"],
            graph_text=graph_ascii,
        )
        plugin_manager.before_run(dag, run_context)
        with bootstrap_airflow_env(config=local_config, extra_env=extra_env), _local_task_policy(
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
            _bootstrap_pools(local_config, notes)
            if has_dag_test(dag):
                kwargs = build_dag_test_kwargs(dag, run_logical_date, conf)
                if fail_fast:
                    backend = "dag.test.strict"
                    notes.append("Using strict local dag.test loop for deterministic fail-fast execution.")
                    dagrun = _strict_dag_test(
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
        _attach_graph_svg(dag, result, graph_svg_path)
        if plugin_manager is not None:
            plugin_manager.after_run(dag, run_context, result)

    if pending_base_exception is not None:
        raise pending_base_exception
    return result



def list_dags_from_file(
    dag_file: str,
    *,
    config_path: str | None = None,
    extra_env: dict[str, str] | None = None,
) -> list[DagFileInfo]:
    selected_config_path = config_path if config_path is not None else get_default_config_path(required=False)
    with bootstrap_airflow_env(config_path=selected_config_path, extra_env=extra_env):
        module = load_module_from_file(dag_file)
        return [dag_file_info(dag) for dag in dag_candidates_from_module(module)]


def run_full_dag(
    dag: Any,
    *,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    graph_svg_path: str | Path | None = None,
    trace: bool = True,
    fail_fast: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
    task_mocks: Iterable[TaskMockRule] | None = None,
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
    collect_xcoms: bool = False,
) -> RunResult:
    """
    Run an ordinary Airflow DAG end-to-end in the current Python process.

    This is the low-level API:
    - it executes the DAG
    - it returns `RunResult`
    - it does not print the final summary for you

    If you want a single-call dev entrypoint, prefer `debug_dag(...)`.
    """
    selected_config_path = config_path if config_path is not None else get_default_config_path(required=False)
    notes: list[str] = []

    try:
        if selected_config_path:
            notes.append(f"Loaded local config from {selected_config_path}")
            local_config = load_local_config(selected_config_path)
        else:
            notes.append("No local config file provided; using current Airflow environment.")
            local_config = LocalConfig()
    except Exception as exc:
        error_raw = traceback.format_exc()
        result = RunResult(
            dag_id=getattr(dag, "dag_id", "<unknown>"),
            backend=_backend_hint(dag, fail_fast=fail_fast),
            airflow_version=get_airflow_version(),
            config_path=selected_config_path,
            notes=notes,
            exception=format_pretty_exception(exc, task_id=getattr(dag, "dag_id", "<dag>")),
            exception_raw=error_raw,
        )
        _attach_graph_svg(dag, result, graph_svg_path)
        return result
    return _execute_full_dag(
        dag,
        local_config=local_config,
        config_path=selected_config_path,
        logical_date=logical_date,
        conf=conf,
        extra_env=extra_env,
        trace=trace,
        fail_fast=fail_fast,
        plugins=plugins,
        task_mocks=task_mocks,
        collect_xcoms=collect_xcoms,
        notes=notes,
        task_ids=task_ids,
        start_task_ids=start_task_ids,
        task_group_ids=task_group_ids,
        graph_svg_path=graph_svg_path,
    )


def debug_dag(
    dag: Any,
    *,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    graph_svg_path: str | Path | None = None,
    trace: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
    include_graph_in_report: bool = False,
    report_dir: str | Path | None = None,
    xcom_json_path: str | Path | None = None,
    collect_xcoms: bool = False,
    raise_on_failure: bool = True,
    fail_fast: bool = True,
    task_mocks: Iterable[TaskMockRule] | None = None,
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
) -> RunResult:
    """
    Run a DAG locally and immediately print the standard final report.

    This is the convenience API intended for `if __name__ == "__main__":`
    blocks in normal DAG files.
    """
    from airflow_local_debug.reporting.report import print_run_report

    resolved_graph_svg_path = _resolve_graph_svg_path(report_dir=report_dir, graph_svg_path=graph_svg_path)
    result = run_full_dag(
        dag,
        config_path=config_path,
        logical_date=logical_date,
        conf=conf,
        extra_env=extra_env,
        graph_svg_path=resolved_graph_svg_path,
        trace=trace,
        fail_fast=fail_fast,
        plugins=plugins,
        task_mocks=task_mocks,
        task_ids=task_ids,
        start_task_ids=start_task_ids,
        task_group_ids=task_group_ids,
        collect_xcoms=collect_xcoms or xcom_json_path is not None,
    )
    if xcom_json_path is not None:
        from airflow_local_debug.reporting.report import write_xcom_snapshot

        xcom_path = write_xcom_snapshot(result, xcom_json_path)
        result.notes.append(f"Wrote XCom snapshot to {xcom_path}")
    if report_dir is not None:
        _write_report_artifacts(result, report_dir, include_graph=include_graph_in_report)
    print_run_report(result, include_graph=include_graph_in_report)
    if raise_on_failure and not result.ok:
        raise SystemExit(1)
    return result


def debug_dag_cli(
    dag: Any,
    *,
    argv: list[str] | None = None,
    require_config_path: bool = False,
    **kwargs: Any,
) -> RunResult:
    """
    Small CLI wrapper around `debug_dag(...)` for `python my_dag.py ...` usage.

    Example:
        python my_dag.py --config-path ~/airflow_defaults.py --logical-date 2026-04-07
    """
    parser = argparse.ArgumentParser(description=f"Local debug runner for DAG '{getattr(dag, 'dag_id', '<unknown>')}'")
    add_common_run_args(parser)
    args = parser.parse_args(argv)

    if require_config_path and not args.config_path:
        parser.error("--config-path is required for this DAG entrypoint.")

    try:
        conf = load_cli_conf(conf_json=args.conf_json, conf_file=args.conf_file)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        cli_extra_env = load_cli_extra_env(args.env)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        env_file_values = load_cli_env_files(
            args.env_file,
            auto_discover=not args.no_auto_env,
        )
    except ValueError as exc:
        parser.error(str(exc))
    try:
        task_mocks = load_cli_task_mocks(args.mock_file)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        cli_task_ids = load_cli_selector_values(args.task_ids, option_name="--task")
        cli_start_task_ids = load_cli_selector_values(args.start_task_ids, option_name="--start-task")
        cli_task_group_ids = load_cli_selector_values(args.task_group_ids, option_name="--task-group")
    except ValueError as exc:
        parser.error(str(exc))

    if conf is None:
        conf = kwargs.pop("conf", None)
    else:
        kwargs.pop("conf", None)
    programmatic_extra_env = kwargs.pop("extra_env", None)
    extra_env = dict(programmatic_extra_env or {})
    extra_env.update(env_file_values)
    extra_env.update(cli_extra_env)
    if task_mocks:
        kwargs.pop("task_mocks", None)
    else:
        task_mocks = kwargs.pop("task_mocks", None)
    programmatic_collect_xcoms = bool(kwargs.pop("collect_xcoms", False))
    programmatic_xcom_json_path = kwargs.pop("xcom_json_path", None)
    xcom_json_path = args.xcom_json_path or programmatic_xcom_json_path
    programmatic_task_ids = kwargs.pop("task_ids", None)
    programmatic_start_task_ids = kwargs.pop("start_task_ids", None)
    programmatic_task_group_ids = kwargs.pop("task_group_ids", None)
    task_ids = cli_task_ids or programmatic_task_ids
    start_task_ids = cli_start_task_ids or programmatic_start_task_ids
    task_group_ids = cli_task_group_ids or programmatic_task_group_ids

    return debug_dag(
        dag,
        config_path=args.config_path,
        logical_date=args.logical_date,
        conf=conf,
        extra_env=extra_env or None,
        trace=not args.no_trace,
        fail_fast=not args.no_fail_fast,
        include_graph_in_report=args.include_graph_in_report,
        report_dir=args.report_dir,
        graph_svg_path=args.graph_svg_path,
        task_mocks=task_mocks,
        task_ids=task_ids,
        start_task_ids=start_task_ids,
        task_group_ids=task_group_ids,
        collect_xcoms=programmatic_collect_xcoms or args.dump_xcom or xcom_json_path is not None,
        xcom_json_path=xcom_json_path,
        **kwargs,
    )


def debug_dag_file_cli(
    *,
    argv: list[str] | None = None,
) -> RunResult:
    """
    Generic CLI entrypoint for local debugging of any DAG file.

    Example:
        airflow-debug-run /abs/path/to/dag.py --dag-id my_dag --config-path ~/airflow_defaults.py
    """
    parser = argparse.ArgumentParser(description="Local debug runner for an Airflow DAG file")
    parser.add_argument("dag_file", help="Absolute path to the DAG Python file.")
    parser.add_argument(
        "--dag-id",
        dest="dag_id",
        help="Optional DAG id when the file defines multiple DAGs.",
    )
    parser.add_argument(
        "--list-dags",
        action="store_true",
        help="List DAG ids discovered in the file and exit without running a DAG.",
    )
    add_common_run_args(parser)
    add_watch_args(parser)
    args = parser.parse_args(argv)

    try:
        extra_env = load_cli_extra_env(args.env)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        env_file_values = load_cli_env_files(
            args.env_file,
            auto_discover=not args.no_auto_env,
        )
    except ValueError as exc:
        parser.error(str(exc))
    # --env wins over --env-file / auto .env
    merged_extra_env = dict(env_file_values)
    merged_extra_env.update(extra_env)
    extra_env = merged_extra_env
    try:
        task_mocks = load_cli_task_mocks(args.mock_file)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        task_ids = load_cli_selector_values(args.task_ids, option_name="--task")
        start_task_ids = load_cli_selector_values(args.start_task_ids, option_name="--start-task")
        task_group_ids = load_cli_selector_values(args.task_group_ids, option_name="--task-group")
    except ValueError as exc:
        parser.error(str(exc))

    if args.list_dags:
        try:
            infos = list_dags_from_file(
                args.dag_file,
                config_path=args.config_path,
                extra_env=extra_env or None,
            )
        except Exception as exc:
            parser.error(str(exc))
        print(format_dag_list(infos, source_path=args.dag_file))
        return RunResult(
            dag_id="<list-dags>",
            state="success",
            config_path=args.config_path,
            notes=[f"Listed {len(infos)} DAG(s) from {args.dag_file}."],
        )

    try:
        conf = load_cli_conf(conf_json=args.conf_json, conf_file=args.conf_file)
    except ValueError as exc:
        parser.error(str(exc))

    if args.watch:
        from airflow_local_debug.watch import watch_dag_file

        return watch_dag_file(
            args.dag_file,
            dag_id=args.dag_id,
            watch_paths=args.watch_path or [],
            poll_interval=args.watch_interval,
            config_path=args.config_path,
            logical_date=args.logical_date,
            conf=conf,
            extra_env=extra_env or None,
            trace=not args.no_trace,
            fail_fast=not args.no_fail_fast,
            task_mocks=task_mocks,
            task_ids=task_ids,
            start_task_ids=start_task_ids,
            task_group_ids=task_group_ids,
            collect_xcoms=args.dump_xcom or args.xcom_json_path is not None,
        )

    return debug_dag_from_file(
        args.dag_file,
        dag_id=args.dag_id,
        config_path=args.config_path,
        logical_date=args.logical_date,
        conf=conf,
        extra_env=extra_env or None,
        trace=not args.no_trace,
        fail_fast=not args.no_fail_fast,
        include_graph_in_report=args.include_graph_in_report,
        report_dir=args.report_dir,
        graph_svg_path=args.graph_svg_path,
        task_mocks=task_mocks,
        task_ids=task_ids,
        start_task_ids=start_task_ids,
        task_group_ids=task_group_ids,
        collect_xcoms=args.dump_xcom or args.xcom_json_path is not None,
        xcom_json_path=args.xcom_json_path,
    )


def run_full_dag_from_file(
    dag_file: str,
    *,
    dag_id: str | None = None,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    graph_svg_path: str | Path | None = None,
    trace: bool = True,
    fail_fast: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
    task_mocks: Iterable[TaskMockRule] | None = None,
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
    collect_xcoms: bool = False,
) -> RunResult:
    """
    Import a normal Airflow DAG file and run the entire DAG.

    This path is useful when local environment bootstrap must happen before
    the DAG module is imported.
    """
    selected_config_path = config_path if config_path is not None else get_default_config_path(required=False)
    notes: list[str] = []

    if selected_config_path:
        notes.append(f"Loaded local config from {selected_config_path}")
    else:
        notes.append("No local config file provided; using current Airflow environment.")

    try:
        with bootstrap_airflow_env(config_path=selected_config_path, extra_env=extra_env) as local_config:
            module = load_module_from_file(dag_file)
            dag = resolve_dag_from_module(module, dag_id=dag_id)
            # extra_env is already applied by the outer bootstrap above (needed for DAG import).
            # Pass extra_env=None to _execute_full_dag to avoid a second redundant apply.
            return _execute_full_dag(
                dag,
                local_config=local_config,
                config_path=selected_config_path,
                logical_date=logical_date,
                conf=conf,
                extra_env=None,
                trace=trace,
                fail_fast=fail_fast,
                plugins=plugins,
                task_mocks=task_mocks,
                collect_xcoms=collect_xcoms,
                notes=notes,
                task_ids=task_ids,
                start_task_ids=start_task_ids,
                task_group_ids=task_group_ids,
                graph_svg_path=graph_svg_path,
            )
    except Exception as exc:
        error_raw = traceback.format_exc()
        return RunResult(
            dag_id=dag_id or "<unknown>",
            config_path=selected_config_path,
            notes=notes,
            exception=format_pretty_exception(exc, task_id=dag_id or "<dag-file>"),
            exception_raw=error_raw,
        )


def debug_dag_from_file(
    dag_file: str,
    *,
    dag_id: str | None = None,
    config_path: str | None = None,
    logical_date: str | date | datetime | None = None,
    conf: dict[str, Any] | None = None,
    extra_env: dict[str, str] | None = None,
    graph_svg_path: str | Path | None = None,
    trace: bool = True,
    plugins: Iterable[AirflowDebugPlugin] | None = None,
    include_graph_in_report: bool = False,
    report_dir: str | Path | None = None,
    xcom_json_path: str | Path | None = None,
    collect_xcoms: bool = False,
    raise_on_failure: bool = True,
    fail_fast: bool = True,
    task_mocks: Iterable[TaskMockRule] | None = None,
    task_ids: Iterable[str] | None = None,
    start_task_ids: Iterable[str] | None = None,
    task_group_ids: Iterable[str] | None = None,
) -> RunResult:
    """
    Import a DAG file, run it locally, and immediately print the standard report.

    This is the file-based equivalent of `debug_dag(...)`.
    """
    from airflow_local_debug.reporting.report import print_run_report

    resolved_graph_svg_path = _resolve_graph_svg_path(report_dir=report_dir, graph_svg_path=graph_svg_path)
    result = run_full_dag_from_file(
        dag_file,
        dag_id=dag_id,
        config_path=config_path,
        logical_date=logical_date,
        conf=conf,
        extra_env=extra_env,
        graph_svg_path=resolved_graph_svg_path,
        trace=trace,
        fail_fast=fail_fast,
        plugins=plugins,
        task_mocks=task_mocks,
        task_ids=task_ids,
        start_task_ids=start_task_ids,
        task_group_ids=task_group_ids,
        collect_xcoms=collect_xcoms or xcom_json_path is not None,
    )
    if xcom_json_path is not None:
        from airflow_local_debug.reporting.report import write_xcom_snapshot

        xcom_path = write_xcom_snapshot(result, xcom_json_path)
        result.notes.append(f"Wrote XCom snapshot to {xcom_path}")
    if report_dir is not None:
        _write_report_artifacts(result, report_dir, include_graph=include_graph_in_report)
    print_run_report(result, include_graph=include_graph_in_report)
    if raise_on_failure and not result.ok:
        raise SystemExit(1)
    return result
