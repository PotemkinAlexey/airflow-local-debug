"""
Local debug toolkit for ordinary Airflow DAGs.

The package is intentionally small:
- `debug_dag(...)` / `debug_dag_from_file(...)` are the one-call entrypoints
- `debug_dag_cli(...)` is the reusable `python my_dag.py --config-path ...` entrypoint
- `debug_dag_file_cli(...)` is the reusable file-based entrypoint for any DAG file
- `run_full_dag(...)` / `run_full_dag_from_file(...)` return `RunResult`
- `AirflowDebugPlugin` lets callers hook into per-task lifecycle events

Typical usage:

    from airflow_local_debug import debug_dag_cli

    if __name__ == "__main__":
        debug_dag_cli(dag, require_config_path=True)
"""

from airflow_local_debug.compat import get_airflow_version
from airflow_local_debug.config_loader import get_default_config_path, load_local_config
from airflow_local_debug.env_bootstrap import bootstrap_airflow_env
from airflow_local_debug.graph import format_dag_graph, print_dag_graph, render_dag_svg, write_dag_svg
from airflow_local_debug.live_trace import live_task_trace
from airflow_local_debug.plugins import (
    AirflowDebugPlugin,
    ConsoleTracePlugin,
    DebugPluginManager,
    ProblemLogPlugin,
    TaskContextPlugin,
)
from airflow_local_debug.report import format_run_report, print_run_report
from airflow_local_debug.runner import (
    debug_dag,
    debug_dag_cli,
    debug_dag_file_cli,
    debug_dag_from_file,
    run_full_dag,
    run_full_dag_from_file,
)
from airflow_local_debug.traceback_utils import StepTracer, StepTracerOptions, format_pretty_exception, safe_repr, shrink

__all__ = [
    "AirflowDebugPlugin",
    "bootstrap_airflow_env",
    "ConsoleTracePlugin",
    "debug_dag",
    "debug_dag_cli",
    "debug_dag_file_cli",
    "debug_dag_from_file",
    "DebugPluginManager",
    "ProblemLogPlugin",
    "TaskContextPlugin",
    "format_dag_graph",
    "format_pretty_exception",
    "format_run_report",
    "get_airflow_version",
    "get_default_config_path",
    "live_task_trace",
    "load_local_config",
    "print_dag_graph",
    "print_run_report",
    "render_dag_svg",
    "run_full_dag",
    "run_full_dag_from_file",
    "safe_repr",
    "shrink",
    "StepTracer",
    "StepTracerOptions",
    "write_dag_svg",
]
