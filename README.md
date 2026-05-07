# airflow-local-debug

Single-process local debug toolkit for ordinary Apache Airflow DAGs.

Supported Airflow range: `apache-airflow>=2.10,!=3.0.*,<4`.
Validated on Airflow 2.10+, 3.1.3, 3.2.1. Airflow 3.0.x is intentionally
skipped: smoke runs against 3.0.6 fail inside Airflow's own local execution
API with HTTP 422 even for `EmptyOperator`, so `dag.test()`-style execution
cannot complete. This is a 3.0 limitation upstream, not a bug in this
package; the exclusion is enforced by the package metadata.

## What it does

- runs a full DAG locally without scheduler/webserver
- keeps native Airflow task and XCom behavior
- can mock selected heavy connector tasks for local runs
- can dump final XComs as JSON fixtures
- detects deferrable operators and reports the local trigger handling mode
- adds deterministic fail-fast execution for debug
- prints a console DAG graph before execution
- provides live per-task tracing and structured problem logging
- exposes a small plugin system for custom hooks

## Documentation

- [Documentation index](docs/index.md)
- [CLI usage](docs/cli.md)
- [Library API](docs/library-api.md)
- [Local config](docs/local-config.md)
- [Reports and artifacts](docs/reports.md)
- [Plugins](docs/plugins.md)
- [Airflow compatibility](docs/airflow-compatibility.md)

## Installation

```bash
pip install -e .
```

Or, when consumed as a Git dependency:

```bash
pip install "airflow-local-debug @ git+https://github.com/PotemkinAlexey/airflow-local-debug@main"
```

## Quick start

### Direct DAG entrypoint

```python
from airflow_local_debug import debug_dag_cli

if __name__ == "__main__":
    debug_dag_cli(dag, require_config_path=True)
```

Run:

```bash
python my_dag.py --config-path /absolute/path/to/airflow_defaults.py
```

### File-based runner

```bash
airflow-debug-run /absolute/path/to/my_dag.py \
  --dag-id my_dag \
  --config-path /absolute/path/to/airflow_defaults.py
```

Inspect DAGs in a file without running them:

```bash
airflow-debug-run /absolute/path/to/my_dag.py --list-dags
```

CLI flags (both entrypoints):

| Flag | Meaning |
|---|---|
| `--config-path` | Local Airflow config file (CONNECTIONS / VARIABLES / POOLS) |
| `--logical-date` | Logical date / execution date for the run |
| `--conf-json` | JSON object to pass as `dag_run.conf` |
| `--conf-file` | Path to a JSON object file to pass as `dag_run.conf` |
| `--env` | Extra `KEY=VALUE` environment variable for this run; repeatable |
| `--mock-file` | JSON/YAML task mock file; repeatable |
| `--dump-xcom` | Collect final XComs into `result.json` and `xcom.json` artifacts |
| `--xcom-json-path` | Write final XCom snapshot to an explicit JSON path |
| `--no-trace` | Disable live per-task console tracing |
| `--no-fail-fast` | Keep original retries (default disables them) |
| `--include-graph-in-report` | Include the DAG graph in the final report |
| `--report-dir` | Write `report.md`, `result.json`, `tasks.csv`, `junit.xml`, `graph.svg`, and optional `graph.txt` / `exception.txt` |
| `--graph-svg-path` | Write the rendered DAG graph SVG (defaults to `graph.svg` inside `--report-dir`) |

File-based runner only:

| Flag | Meaning |
|---|---|
| `--dag-id` | Select one DAG when the file defines multiple DAGs |
| `--list-dags` | Print discovered DAG ids and task counts, then exit without running |

## Doctor

Validate local prerequisites before running a DAG:

```bash
airflow-debug-doctor \
  --config-path /absolute/path/to/airflow_defaults.py \
  --dag-file /absolute/path/to/my_dag.py \
  --dag-id my_dag
```

The doctor checks Airflow import/version support, metadata DB readiness, local
config shape, and optional DAG import / Airflow 3 serialization.
Pass `--json` when CI or another tool needs a machine-readable result.

## Local config

The library can load a Python config module with optional globals:

- `CONNECTIONS`
- `VARIABLES`
- `POOLS`

Config path resolution order:

1. explicit `--config-path`
2. `AIRFLOW_DEBUG_LOCAL_CONFIG`
3. `RUNBOOK_LOCAL_CONFIG`

Example `airflow_defaults.py`:

```python
CONNECTIONS = {
    "demo_http": {
        "conn_type": "http",
        "host": "example.com",
        "extra": {"timeout": 30, "verify": False},
    },
}

VARIABLES = {
    "ENV": "local",
    "FEATURES": {"experimental_pipeline": True},
}

POOLS = {
    "default_pool": {"slots": 4},
}
```

## Run modes (backends)

| Backend | When used |
|---|---|
| `dag.test.strict` | `fail_fast=True` (default) — strict local loop, deterministic abort on first failure |
| `dag.test` | `fail_fast=False` and Airflow exposes `dag.test()` |
| `dag.run` | Older Airflow versions without `dag.test()` |
| `unsupported` | Neither method available |

`fail_fast=True` also disables retries on every task for the duration of the
local run, then restores the original values.

Deferrable tasks are detected before execution. In the default
`dag.test.strict` backend, the local loop uses inline trigger handling when the
installed Airflow runtime exposes it. In native `dag.test` / legacy `dag.run`
mode, the report calls out that provider behavior may still depend on Airflow's
trigger handling.

## Task mocks and XCom fixtures

Use task mocks to keep local runs away from real warehouses, notebooks, or
expensive APIs while still exercising graph flow and XCom contracts.

```json
{
  "mocks": [
    {
      "task_id": "load_to_snowflake",
      "xcom": {
        "return_value": {
          "rows_loaded": 120,
          "table": "analytics.events"
        }
      }
    }
  ]
}
```

```bash
airflow-debug-run /absolute/path/to/my_dag.py \
  --dag-id my_dag \
  --mock-file ./local.mocks.json \
  --dump-xcom \
  --report-dir ./airflow-debug-report
```

Rules can match by `task_id`, `task_id_glob`, `operator`, or `operator_glob`.
Mocked tasks are called out in the report and get `tasks[].mocked=true` in
`result.json`. `--dump-xcom` writes collected XComs to `RunResult.xcoms` and
`xcom.json` when `--report-dir` is used.

## Result object

`run_full_dag(...)` and `run_full_dag_from_file(...)` return a `RunResult`:

```python
@dataclass
class RunResult:
    dag_id: str
    run_id: str | None
    state: str | None              # normalized: lowercase, no enum prefix
    logical_date: str | None
    backend: str | None            # one of dag.test / dag.test.strict / dag.run / unsupported
    airflow_version: str | None
    config_path: str | None
    graph_ascii: str | None
    graph_svg_path: str | None
    tasks: list[TaskRunInfo]
    mocks: list[TaskMockInfo]
    deferrables: list[DeferrableTaskInfo]
    xcoms: dict[str, dict[str, Any]]
    notes: list[str]               # informational messages from bootstrap / plugins
    exception: str | None          # pretty-formatted exception block
    exception_raw: str | None
    exception_was_logged: bool

    @property
    def ok(self) -> bool: ...      # True only when state == "success" and no exception
```

Each `TaskRunInfo` also includes `start_date`, `end_date`, and
`duration_seconds` when Airflow exposes timing data for the task instance.

For a persisted snapshot from the convenience CLI/API, pass
`--report-dir ./airflow-debug-report` or `report_dir="..."`.

## Plugins

Subclass `AirflowDebugPlugin` and pass via `plugins=[...]`:

```python
from airflow_local_debug import AirflowDebugPlugin, debug_dag

class MyHook(AirflowDebugPlugin):
    def before_task(self, task, context):
        print(f"START {task.task_id}")

    def on_task_error(self, task, context, error):
        print(f"FAIL  {task.task_id}: {error!r}")

debug_dag(dag, plugins=[MyHook()])
```

Available hook points:

- `before_run` / `after_run`
- `before_task` / `after_task` / `on_task_error`
- `before_task_callback` / `after_task_callback` / `on_task_callback_error`

The runner ships three default plugins (`TaskContextPlugin`, `ProblemLogPlugin`,
`ConsoleTracePlugin`). If you pass an instance of any of those types via
`plugins=[...]`, the corresponding default is skipped to avoid duplication.

Plugin errors are isolated: they are recorded in `RunResult.notes` instead of
breaking DAG execution.

## Graph rendering

ASCII (printed automatically before the run, capped at 500 tasks):

```python
from airflow_local_debug import format_dag_graph, print_dag_graph

print_dag_graph(dag)
```

SVG (capped at 200 tasks):

```python
from airflow_local_debug import render_dag_svg, write_dag_svg

svg_path = write_dag_svg(dag)              # /tmp/airflow_debug_graphs/<dag>_<ts>.svg
svg_text = render_dag_svg(dag)             # raw SVG string
```

When `--report-dir` is used, the CLI also writes `graph.svg` and includes its
path in the final report.

## Library-mode usage (no global state mutation)

```python
from airflow_local_debug import (
    run_full_dag,
    silenced_airflow_bootstrap_warnings,
)

with silenced_airflow_bootstrap_warnings():
    result = run_full_dag(dag, config_path="/path/to/airflow_defaults.py")
    if not result.ok:
        raise RuntimeError(result.exception)
```

`ensure_quiet_airflow_bootstrap()` (used by the CLI) re-execs the process to
suppress import-time Airflow warnings; it permanently mutates global warning
state and is intended for one-shot CLI use only.

## Development

```bash
make install-dev
make test
make build
make check   # tests + build
make smoke   # Airflow runtime smoke in a temporary AIRFLOW_HOME
```

CI runs the test suite on Python 3.10, 3.11, 3.12, validates Airflow 2.10+ and
Airflow 3.1+/3.2 runtime smoke paths, and builds the wheel/sdist.
