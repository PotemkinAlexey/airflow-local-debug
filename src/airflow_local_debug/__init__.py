"""
Local debug toolkit for ordinary Airflow DAGs.

Entrypoints
-----------
- `debug_dag(dag, ...)` — run + print report (typical for inline use)
- `debug_dag_from_file(path, ...)` — same, but loads the DAG from a file
- `debug_dag_cli(dag)` — argparse wrapper for `python my_dag.py ...`
- `debug_dag_file_cli()` — argparse wrapper for the `airflow-debug-run` script
- `list_dags_from_file(path, ...)` — inspect DAG ids without running them
- `run_doctor(...)` — validate local Airflow/debug prerequisites
- `run_full_dag(dag, ...)` / `run_full_dag_from_file(...)` — return raw `RunResult`

Result object
-------------
`RunResult` exposes:
- `.ok` — True only if state == "success" and no exception
- `.state`, `.exception`, `.tasks` (list of `TaskRunInfo`), `.notes`, `.graph_ascii`
- `.backend` — one of "dag.test", "dag.test.strict", "dag.run", "unsupported"

Plugins
-------
Subclass `AirflowDebugPlugin` and pass via `plugins=[...]`. Hooks include
`before_run`, `before_task`, `after_task`, `on_task_error`, `after_run`.
The runner ships three default plugins (`TaskContextPlugin`, `ProblemLogPlugin`,
`ConsoleTracePlugin`); `_build_plugin_manager` skips defaults whose type is
already provided by the caller.

Fail-fast mode
--------------
`fail_fast=True` (default) disables retries and uses the strict local dag.test
loop, so the first failed task aborts the run with a deterministic state.

Graph rendering
---------------
- `format_dag_graph(dag)` / `print_dag_graph(dag)` — ASCII tree (capped at 500 tasks)
- `render_dag_svg(dag)` / `write_dag_svg(dag)` — standalone SVG (capped at 200 tasks)

Typical usage
-------------

    from airflow_local_debug import debug_dag_cli

    if __name__ == "__main__":
        debug_dag_cli(dag, require_config_path=True)

Library usage (no global state mutation)
----------------------------------------

    from airflow_local_debug import (
        run_full_dag,
        silenced_airflow_bootstrap_warnings,
    )

    with silenced_airflow_bootstrap_warnings():
        result = run_full_dag(dag, config_path="/path/to/airflow_defaults.py")
        if not result.ok:
            ...
"""

from importlib import import_module

from airflow_local_debug.bootstrap import (
    ensure_quiet_airflow_bootstrap,
    silence_airflow_bootstrap_warnings,
    silenced_airflow_bootstrap_warnings,
)
from airflow_local_debug.compat import get_airflow_version
from airflow_local_debug.config_loader import get_default_config_path, load_local_config
from airflow_local_debug.env_bootstrap import bootstrap_airflow_env
from airflow_local_debug.graph import format_dag_graph, print_dag_graph, render_dag_svg, write_dag_svg
from airflow_local_debug.live_trace import live_task_trace
from airflow_local_debug.models import DagFileInfo, LocalConfig, RunResult, TaskRunInfo
from airflow_local_debug.plugins import (
    AirflowDebugPlugin,
    ConsoleTracePlugin,
    DebugPluginManager,
    ProblemLogPlugin,
    RepeatedProblemWarningError,
    TaskContextPlugin,
)
from airflow_local_debug.report import format_run_report, print_run_report, write_run_artifacts
from airflow_local_debug.runner import (
    debug_dag,
    debug_dag_cli,
    debug_dag_file_cli,
    debug_dag_from_file,
    format_dag_list,
    list_dags_from_file,
    run_full_dag,
    run_full_dag_from_file,
)
from airflow_local_debug.traceback_utils import StepTracer, StepTracerOptions, format_pretty_exception, safe_repr, shrink

_DOCTOR_EXPORTS = {
    "DoctorCheck",
    "DoctorResult",
    "check_airflow_import",
    "check_dag_file",
    "check_local_config",
    "check_metadata_db",
    "doctor_result_to_dict",
    "format_doctor_json",
    "format_doctor_report",
    "is_supported_airflow_version",
    "run_doctor",
}


def __getattr__(name: str):
    if name in _DOCTOR_EXPORTS:
        module = import_module("airflow_local_debug.doctor")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "AirflowDebugPlugin",
    "bootstrap_airflow_env",
    "ConsoleTracePlugin",
    "debug_dag",
    "debug_dag_cli",
    "debug_dag_file_cli",
    "debug_dag_from_file",
    "DebugPluginManager",
    "DagFileInfo",
    "DoctorCheck",
    "DoctorResult",
    "doctor_result_to_dict",
    "ensure_quiet_airflow_bootstrap",
    "check_airflow_import",
    "check_dag_file",
    "check_local_config",
    "check_metadata_db",
    "format_dag_graph",
    "format_dag_list",
    "format_doctor_json",
    "format_doctor_report",
    "format_pretty_exception",
    "format_run_report",
    "get_airflow_version",
    "get_default_config_path",
    "live_task_trace",
    "list_dags_from_file",
    "load_local_config",
    "LocalConfig",
    "print_dag_graph",
    "print_run_report",
    "ProblemLogPlugin",
    "render_dag_svg",
    "RepeatedProblemWarningError",
    "is_supported_airflow_version",
    "run_doctor",
    "run_full_dag",
    "run_full_dag_from_file",
    "RunResult",
    "safe_repr",
    "shrink",
    "silence_airflow_bootstrap_warnings",
    "silenced_airflow_bootstrap_warnings",
    "StepTracer",
    "StepTracerOptions",
    "TaskContextPlugin",
    "TaskRunInfo",
    "write_dag_svg",
    "write_run_artifacts",
]
