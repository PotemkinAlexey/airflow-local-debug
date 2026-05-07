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
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any

from airflow_local_debug.cli.args import add_common_run_args, add_watch_args
from airflow_local_debug.cli.loaders import (
    load_cli_conf,
    load_cli_env_files,
    load_cli_extra_env,
    load_cli_selector_values,
    load_cli_task_mocks,
)
from airflow_local_debug.compat import get_airflow_version
from airflow_local_debug.config.env import bootstrap_airflow_env
from airflow_local_debug.config.loader import get_default_config_path, load_local_config
from airflow_local_debug.execution.dag_loader import (
    dag_candidates_from_module,
    dag_file_info,
    format_dag_list,
    load_module_from_file,
    resolve_dag_from_module,
)
from airflow_local_debug.execution.mocks import TaskMockRule
from airflow_local_debug.execution.orchestrator import (
    attach_graph_svg,
    backend_hint,
    execute_full_dag,
    resolve_graph_svg_path,
    write_report_artifacts,
)
from airflow_local_debug.models import DagFileInfo, LocalConfig, RunResult
from airflow_local_debug.plugins import AirflowDebugPlugin
from airflow_local_debug.reporting.traceback_utils import format_pretty_exception

_log = logging.getLogger(__name__)




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
            backend=backend_hint(dag, fail_fast=fail_fast),
            airflow_version=get_airflow_version(),
            config_path=selected_config_path,
            notes=notes,
            exception=format_pretty_exception(exc, task_id=getattr(dag, "dag_id", "<dag>")),
            exception_raw=error_raw,
        )
        attach_graph_svg(dag, result, graph_svg_path)
        return result
    return execute_full_dag(
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

    resolved_graph_svg_path = resolve_graph_svg_path(report_dir=report_dir, graph_svg_path=graph_svg_path)
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
        write_report_artifacts(result, report_dir, include_graph=include_graph_in_report)
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
            report_dir=args.report_dir,
            include_graph_in_report=args.include_graph_in_report,
            xcom_json_path=args.xcom_json_path,
            graph_svg_path=args.graph_svg_path,
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
            # Pass extra_env=None to execute_full_dag to avoid a second redundant apply.
            return execute_full_dag(
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

    resolved_graph_svg_path = resolve_graph_svg_path(report_dir=report_dir, graph_svg_path=graph_svg_path)
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
        write_report_artifacts(result, report_dir, include_graph=include_graph_in_report)
    print_run_report(result, include_graph=include_graph_in_report)
    if raise_on_failure and not result.ok:
        raise SystemExit(1)
    return result
