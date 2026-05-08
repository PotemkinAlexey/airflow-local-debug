# Feature Guide with Examples

This guide shows each main `airflow-local-debug` capability as a practical
workflow. Use it as a cookbook: pick the feature you need, copy the closest
example, then adjust paths and task ids for your DAG.

For complete end-to-end workflows using runnable DAG files from this
repository, start with [Recipes](recipes.md) and [Example DAGs](../examples/README.md).

## Run a DAG from a file

Use `airflow-debug-run` when you have a normal Airflow DAG file and want to
debug it without adding code to the DAG.

Basic run:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --config-path /absolute/path/to/airflow_defaults.py
```

Run the same DAG for a specific schedule date:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --logical-date 2026-05-07T00:00:00+00:00
```

Run with artifacts for sharing or CI upload:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --report-dir ./airflow-debug-report \
  --include-graph-in-report
```

## Add an inline DAG entrypoint

Use `debug_dag_cli` when a DAG owner wants `python my_dag.py ...` to be the
normal local debug command.

Minimal entrypoint:

```python
from airflow_local_debug import debug_dag_cli

if __name__ == "__main__":
    debug_dag_cli(dag)
```

Require a local config file for safer connector behavior:

```python
from airflow_local_debug import debug_dag_cli

if __name__ == "__main__":
    debug_dag_cli(dag, require_config_path=True)
```

Provide defaults while still letting CLI flags override them:

```python
debug_dag_cli(
    dag,
    require_config_path=True,
    conf={"source": "local"},
    extra_env={"FEATURE_FLAG": "local"},
)
```

Run it:

```bash
python /absolute/path/to/dags/orders.py \
  --config-path /absolute/path/to/airflow_defaults.py \
  --report-dir ./airflow-debug-report
```

## Use the Python API

Use the Python API from custom local tools, notebooks, or tests that want a
`RunResult` object instead of only console output.

Run and inspect the result:

```python
from airflow_local_debug import run_full_dag

result = run_full_dag(
    dag,
    config_path="/absolute/path/to/airflow_defaults.py",
    logical_date="2026-05-07",
    conf={"dataset": "daily"},
)

assert result.ok, result.exception
```

Run a DAG file after applying local environment bootstrap:

```python
from airflow_local_debug import run_full_dag_from_file

result = run_full_dag_from_file(
    "/absolute/path/to/dags/orders.py",
    dag_id="orders_daily",
    config_path="/absolute/path/to/airflow_defaults.py",
)
```

Print the standard report and keep control of failures:

```python
from airflow_local_debug import debug_dag

result = debug_dag(
    dag,
    raise_on_failure=False,
    report_dir="./airflow-debug-report",
)

if not result.ok:
    print(result.exception)
```

## List DAGs before running

Use DAG discovery when a file contains multiple DAGs or when a script needs to
show the available choices.

CLI:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py --list-dags
```

Python:

```python
from airflow_local_debug import format_dag_list, list_dags_from_file

infos = list_dags_from_file("/absolute/path/to/dags/orders.py")
print(format_dag_list(infos))
```

Use a config during discovery when imports depend on Airflow variables or
environment:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --config-path /absolute/path/to/airflow_defaults.py \
  --list-dags
```

## Local config for connections, variables, and pools

Use a local config file to point hooks at safe local services and fixture
values without changing the deployed Airflow environment.

Example `airflow_defaults.py`:

```python
CONNECTIONS = {
    "warehouse": {
        "conn_type": "postgres",
        "host": "localhost",
        "schema": "analytics",
        "login": "airflow",
        "password": "airflow",
        "port": 5432,
    },
}

VARIABLES = {
    "ENV": "local",
    "BATCH_SIZE": 100,
    "FEATURES": {"skip_quality_gate": False},
}

POOLS = {
    "warehouse_pool": {
        "slots": 2,
        "description": "Local warehouse concurrency",
    },
}
```

Pass it explicitly:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --config-path ./airflow_defaults.py
```

Use the environment fallback:

```bash
export AIRFLOW_DEBUG_LOCAL_CONFIG=/absolute/path/to/airflow_defaults.py
airflow-debug-run /absolute/path/to/dags/orders.py --dag-id orders_daily
```

Load and inspect it in Python:

```python
from airflow_local_debug import load_local_config

config = load_local_config("./airflow_defaults.py")
assert "warehouse" in config.connections
```

## Environment variables and `.env` files

Use `--env`, `--env-file`, or auto-discovered `./.env` for credentials and
feature flags that should not be hard-coded in `airflow_defaults.py`.

One-off CLI values:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --env API_BASE_URL=http://localhost:8080 \
  --env FEATURE_FLAG=local
```

Multiple env files, with later files winning:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --env-file ./shared.env \
  --env-file ./developer.env
```

Disable auto-loading `./.env` for a reproducible CI run:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --no-auto-env
```

Programmatic usage:

```python
from airflow_local_debug import parse_dotenv_file, run_full_dag

env = parse_dotenv_file("./developer.env")
result = run_full_dag(dag, extra_env=env)
```

## Pass `dag_run.conf`

Use `dag_run.conf` when the DAG reads runtime parameters from the run context.

Inline JSON:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --conf-json '{"dataset": "orders", "limit": 1000}'
```

JSON file:

```json
{
  "dataset": "orders",
  "limit": 1000,
  "dry_run": true
}
```

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --conf-file ./local-conf.json
```

Python:

```python
result = run_full_dag(
    dag,
    conf={"dataset": "orders", "limit": 1000, "dry_run": True},
)
```

## Mock heavy or unsafe tasks

Use task mocks to keep local runs away from warehouses, notebooks, billing
APIs, or production-side effects while still validating graph flow and XCom
contracts.

Mock by exact task id:

```json
{
  "mocks": [
    {
      "name": "warehouse load fixture",
      "task_id": "load_to_warehouse",
      "xcom": {
        "return_value": {
          "rows_loaded": 120,
          "table": "analytics.orders"
        }
      }
    }
  ]
}
```

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --mock-file ./local.mocks.json
```

Mock by operator class for every matching task:

```yaml
mocks:
  - name: local databricks run
    operator: DatabricksSubmitRunOperator
    return_value:
      run_id: local-run-001
```

Mock by glob for generated task ids:

```json
{
  "mocks": [
    {
      "task_id_glob": "extract_partner_*",
      "xcom": {
        "return_value": {"status": "ok"}
      }
    }
  ]
}
```

Programmatic mocks:

```python
from airflow_local_debug import TaskMockRule, run_full_dag

result = run_full_dag(
    dag,
    task_mocks=[
        TaskMockRule(
            task_id="load_to_warehouse",
            name="warehouse fixture",
            xcom={"return_value": {"rows_loaded": 120}},
        )
    ],
    collect_xcoms=True,
)
```

Rules are required by default. Set `"required": false` when a mock should be
allowed to match no task in some DAG variants.

## Run only part of a DAG

Use partial runs when the full DAG is slow or the current change affects only
one branch.

Run exactly one task:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --task transform_orders
```

Run a task and all downstream tasks:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --start-task transform_orders
```

Run a TaskGroup subtree:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --task-group warehouse
```

Combine selectors:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --task extract_orders,extract_customers \
  --start-task transform_orders
```

Python:

```python
result = run_full_dag(
    dag,
    start_task_ids=["transform_orders"],
)
print(result.selected_tasks)
```

If a selected task pulls XComs from an upstream task that was not selected,
the runner adds a note. Fix it by selecting the upstream chain or by mocking
the upstream task's XCom:

```json
{
  "mocks": [
    {
      "task_id": "extract_orders",
      "xcom": {"return_value": [{"order_id": 1}]}
    }
  ]
}
```

## Collect XCom fixtures

Use XCom dumps to turn a successful local run into a fixture for tests or
downstream debugging.

Write XComs inside the report directory:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --dump-xcom \
  --report-dir ./airflow-debug-report
```

Write only the XCom snapshot to a chosen path:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --xcom-json-path ./fixtures/orders-xcom.json
```

Collect and assert in Python:

```python
result = run_full_dag(dag, collect_xcoms=True)
assert result.xcoms["load_to_warehouse"]["return_value"]["rows_loaded"] > 0
```

Example `xcom.json` shape:

```json
{
  "extract_orders": {
    "return_value": [{"order_id": 1}]
  },
  "load_to_warehouse": {
    "return_value": {
      "rows_loaded": 1
    }
  }
}
```

## Reports, artifacts, and CI output

Use reports when a run needs to be shared, compared, or uploaded from CI.

Write all standard artifacts:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --report-dir ./airflow-debug-report
```

The directory contains:

- `report.md`
- `result.json`
- `tasks.csv`
- `junit.xml`
- `graph.svg`
- optional `graph.txt`
- optional `exception.txt`
- optional `xcom.json`

Use `tasks.csv` for quick timing comparison:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --report-dir ./before

airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --report-dir ./after
```

Use `junit.xml` in CI so each Airflow task appears as a test case:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --report-dir "$CI_ARTIFACTS/airflow-debug"
```

Format a report yourself:

```python
from airflow_local_debug import format_run_report, run_full_dag

result = run_full_dag(dag)
print(format_run_report(result, include_graph=True))
```

## Render DAG graphs

Use graph rendering to inspect topology before task execution or to attach a
visual DAG preview to a report.

Print the ASCII graph before running:

```python
from airflow_local_debug import print_dag_graph

print_dag_graph(dag)
```

Write only an SVG graph:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --graph-svg-path ./orders.svg
```

Render SVG from Python:

```python
from airflow_local_debug import write_dag_svg

path = write_dag_svg(dag, "./orders.svg")
print(path)
```

Include the ASCII graph in the final report:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --report-dir ./airflow-debug-report \
  --include-graph-in-report
```

## Watch files and hot-reload

Use watch mode for the edit-save-run loop. The first run is a normal run. If a
task fails, the next change retries from that failed task and its downstreams.

Basic watch loop:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --config-path ./airflow_defaults.py \
  --watch
```

Watch SQL and fixture directories outside the DAG folder:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --watch \
  --watch-path /absolute/path/to/sql \
  --watch-path /absolute/path/to/fixtures
```

Use polling interval when `watchdog` is not installed:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --watch \
  --watch-interval 1.0
```

Use mocks with watch mode to make retry-from-failed-task deterministic:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --watch \
  --mock-file ./upstream-fixtures.json
```

Python:

```python
from airflow_local_debug import watch_dag_file

watch_dag_file(
    "/absolute/path/to/dags/orders.py",
    dag_id="orders_daily",
    watch_paths=["/absolute/path/to/sql"],
    poll_interval=0.5,
)
```

## Fail-fast mode and execution backends

Use the default fail-fast mode for local debugging. It disables task retries
for the run, executes through the strict local loop when available, and stops
at the first hard failure.

Default local debug run:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily
```

Disable fail-fast when you need native Airflow retry behavior:

```bash
airflow-debug-run /absolute/path/to/dags/orders.py \
  --dag-id orders_daily \
  --no-fail-fast
```

Python:

```python
result = run_full_dag(dag, fail_fast=False)
print(result.backend)
```

Common backend values:

- `dag.test.strict`: default fail-fast path.
- `dag.test`: native `dag.test()` with `fail_fast=False`.
- `dag.run`: legacy fallback when `dag.test()` is unavailable.
- `unsupported`: no usable local execution method.

## Deferrable operators

Use deferrable detection to understand whether local execution may depend on
Airflow/provider trigger handling.

Inspect deferrables before running:

```python
from airflow_local_debug import detect_deferrable_tasks

for item in detect_deferrable_tasks(dag, backend_hint="dag.test.strict"):
    print(item.task_id, item.operator, item.local_mode, item.reason)
```

Run normally and inspect the recorded result:

```python
result = run_full_dag(dag)
for item in result.deferrables:
    print(item.task_id, item.local_mode)
```

Mock a deferrable operator when local trigger behavior is not useful:

```json
{
  "mocks": [
    {
      "operator": "DatabricksSubmitRunDeferrableOperator",
      "return_value": {"run_id": "local-deferrable-run"}
    }
  ]
}
```

## Doctor checks

Use doctor before debugging a new environment or before CI smoke tests.

Human-readable output:

```bash
airflow-debug-doctor \
  --config-path ./airflow_defaults.py \
  --dag-file /absolute/path/to/dags/orders.py \
  --dag-id orders_daily
```

Require a config file:

```bash
airflow-debug-doctor \
  --require-config \
  --config-path ./airflow_defaults.py
```

JSON output for CI:

```bash
airflow-debug-doctor \
  --json \
  --dag-file /absolute/path/to/dags/orders.py \
  --dag-id orders_daily
```

Python:

```python
from airflow_local_debug import format_doctor_report, run_doctor

result = run_doctor(
    config_path="./airflow_defaults.py",
    dag_path="/absolute/path/to/dags/orders.py",
    dag_id="orders_daily",
)
print(format_doctor_report(result))
raise SystemExit(result.exit_code)
```

## Pytest fixture

Use the built-in `airflow_local_runner` fixture for integration tests that
execute real DAG code.

Run a DAG file in a test:

```python
def test_orders_dag_succeeds(airflow_local_runner):
    result = airflow_local_runner.run_dag(
        "/absolute/path/to/dags/orders.py",
        dag_id="orders_daily",
        config_path="./airflow_defaults.py",
    )

    assert result.ok
```

Mock a connector and assert on XComs:

```python
from airflow_local_debug import TaskMockRule


def test_orders_load_contract(airflow_local_runner):
    result = airflow_local_runner.run_dag(
        "/absolute/path/to/dags/orders.py",
        dag_id="orders_daily",
        task_mocks=[
            TaskMockRule(
                task_id="load_to_warehouse",
                xcom={"return_value": {"rows_loaded": 25}},
            )
        ],
    )

    assert result.xcoms["load_to_warehouse"]["return_value"]["rows_loaded"] == 25
```

Run only the branch under test:

```python
def test_transform_branch(airflow_local_runner, dag_object):
    result = airflow_local_runner.run_dag(
        dag_object,
        start_task_ids=["transform_orders"],
    )

    assert result.ok
    assert "transform_orders" in result.selected_tasks
```

Trace is off by default in the fixture so test output stays clean. Set
`trace=True` for failure diagnosis.

## Plugins and instrumentation

Use plugins when you need local-only instrumentation without editing task code.

Log task starts and failures:

```python
from airflow_local_debug import AirflowDebugPlugin, debug_dag


class LoggingPlugin(AirflowDebugPlugin):
    def before_task(self, task, context):
        print(f"START {task.task_id}")

    def on_task_error(self, task, context, error):
        print(f"FAIL {task.task_id}: {error!r}")


debug_dag(dag, plugins=[LoggingPlugin()])
```

Collect task ids for assertions:

```python
from airflow_local_debug import AirflowDebugPlugin, run_full_dag


class SeenTasks(AirflowDebugPlugin):
    def __init__(self):
        self.started = []

    def before_task(self, task, context):
        self.started.append(task.task_id)


plugin = SeenTasks()
result = run_full_dag(dag, plugins=[plugin], trace=False)
assert result.ok
assert "extract_orders" in plugin.started
```

Add a run-level note for custom tooling:

```python
from airflow_local_debug import AirflowDebugPlugin, run_full_dag


class BuildMetadataPlugin(AirflowDebugPlugin):
    def before_run(self, dag, context):
        context["notes"].append("Local build id: dev-123")


result = run_full_dag(dag, plugins=[BuildMetadataPlugin()])
assert "Local build id: dev-123" in result.notes
```

Plugin exceptions are isolated and become `RunResult.notes` entries instead of
changing the DAG execution result.

## Warning suppression for library tools

Use warning suppression when a long-lived Python process imports Airflow and
you want quiet bootstrap behavior.

Context manager:

```python
from airflow_local_debug import run_full_dag, silenced_airflow_bootstrap_warnings

with silenced_airflow_bootstrap_warnings():
    result = run_full_dag(dag)
```

CLI entrypoints already call the process-level bootstrap helper:

```python
from airflow_local_debug import ensure_quiet_airflow_bootstrap, debug_dag_file_cli

ensure_quiet_airflow_bootstrap()
debug_dag_file_cli()
```

Prefer the context manager in reusable library code. `ensure_quiet_airflow_bootstrap()`
may re-exec the process and is intended for one-shot command-line entrypoints.
