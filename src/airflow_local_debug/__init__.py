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
- `.selected_tasks` — effective task ids when a partial run was requested
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
from typing import Any

from airflow_local_debug.compat import get_airflow_version
from airflow_local_debug.config.bootstrap import (
    ensure_quiet_airflow_bootstrap,
    silence_airflow_bootstrap_warnings,
    silenced_airflow_bootstrap_warnings,
)
from airflow_local_debug.config.dotenv import discover_dotenv_path, parse_dotenv_file, parse_dotenv_text
from airflow_local_debug.config.env import bootstrap_airflow_env
from airflow_local_debug.config.loader import get_default_config_path, load_local_config
from airflow_local_debug.execution.dag_loader import format_dag_list
from airflow_local_debug.execution.deferrables import detect_deferrable_tasks
from airflow_local_debug.execution.mocks import TaskMockRule, load_task_mock_rules, task_mock_rules_from_payload
from airflow_local_debug.models import DagFileInfo, DeferrableTaskInfo, LocalConfig, RunResult, TaskMockInfo, TaskRunInfo
from airflow_local_debug.plugins import (
    AirflowDebugPlugin,
    ConsoleTracePlugin,  # noqa: F401  (back-compat re-export; not in __all__)
    DebugPluginManager,  # noqa: F401  (back-compat re-export; not in __all__)
    ProblemLogPlugin,
    RepeatedProblemWarningError,
    TaskContextPlugin,  # noqa: F401  (back-compat re-export; not in __all__)
)
from airflow_local_debug.pytest_plugin import AirflowLocalRunner
from airflow_local_debug.reporting.graph import format_dag_graph, print_dag_graph, render_dag_svg, write_dag_svg
from airflow_local_debug.reporting.live_trace import live_task_trace  # noqa: F401  (back-compat re-export)
from airflow_local_debug.reporting.report import (
    format_run_gantt,
    format_run_report,
    print_run_report,
    write_run_artifacts,
    write_xcom_snapshot,
)
from airflow_local_debug.reporting.traceback_utils import (  # noqa: F401  (back-compat re-exports; not in __all__)
    StepTracer,
    StepTracerOptions,
    format_pretty_exception,
    safe_repr,
    shrink,
)
from airflow_local_debug.runner import (
    debug_dag,
    debug_dag_cli,
    debug_dag_file_cli,
    debug_dag_from_file,
    list_dags_from_file,
    run_full_dag,
    run_full_dag_from_file,
)
from airflow_local_debug.watch import watch_dag_file

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


def __getattr__(name: str) -> Any:
    if name in _DOCTOR_EXPORTS:
        module = import_module("airflow_local_debug.doctor")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# Stable public API. Everything else is treated as internal — symbols that
# used to live here remain importable from `airflow_local_debug.<submodule>`,
# but they are no longer part of the stability promise.
#
# Internal symbols not listed below (still importable, no compat guarantee):
#   StepTracer, StepTracerOptions, safe_repr, shrink — reporting.traceback_utils
#   format_pretty_exception                          — reporting.traceback_utils
#   live_task_trace                                  — reporting.live_trace
#   ConsoleTracePlugin, TaskContextPlugin            — built-in trace plugins
#   DebugPluginManager                               — plugin dispatcher
#   is_supported_airflow_version, check_*            — doctor internals
#   doctor_result_to_dict                            — doctor JSON helper
__all__ = [
    # Run entrypoints
    "debug_dag",
    "debug_dag_cli",
    "debug_dag_file_cli",
    "debug_dag_from_file",
    "run_full_dag",
    "run_full_dag_from_file",
    "list_dags_from_file",
    "watch_dag_file",
    # Result types
    "RunResult",
    "TaskRunInfo",
    "TaskMockInfo",
    "DeferrableTaskInfo",
    "DagFileInfo",
    "LocalConfig",
    # Mocks
    "TaskMockRule",
    "load_task_mock_rules",
    "task_mock_rules_from_payload",
    # Plugin system (subclass these from user code)
    "AirflowDebugPlugin",
    "ProblemLogPlugin",
    "RepeatedProblemWarningError",
    "AirflowLocalRunner",
    # Doctor
    "run_doctor",
    "DoctorCheck",
    "DoctorResult",
    "format_doctor_report",
    "format_doctor_json",
    # Reports
    "format_run_report",
    "print_run_report",
    "format_run_gantt",
    "write_run_artifacts",
    "write_xcom_snapshot",
    "format_dag_list",
    # Graph rendering
    "format_dag_graph",
    "print_dag_graph",
    "render_dag_svg",
    "write_dag_svg",
    # Bootstrap / config
    "bootstrap_airflow_env",
    "silenced_airflow_bootstrap_warnings",
    "silence_airflow_bootstrap_warnings",
    "ensure_quiet_airflow_bootstrap",
    "load_local_config",
    "get_default_config_path",
    "parse_dotenv_file",
    "parse_dotenv_text",
    "discover_dotenv_path",
    # Detection / version
    "detect_deferrable_tasks",
    "get_airflow_version",
]
