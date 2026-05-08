# Recipes

These recipes show complete local-debug workflows. Use them when you already
know which problem you are solving and want the shortest working path.

All examples assume this repository is installed in editable mode:

```bash
pip install -e .
```

The files under `examples/` are intentionally small and safe to run locally.
They are not meant to model production DAG style; they demonstrate how the
debug runner is wired.

## Run a Simple DAG Locally

Use this when you want to verify task logic, XCom flow, and final reporting
without a scheduler or webserver.

```bash
airflow-debug-run examples/orders_success_dag.py \
  --dag-id orders_success \
  --config-path examples/airflow_defaults.py \
  --logical-date 2026-05-07 \
  --report-dir .tmp/orders-success-report \
  --include-graph-in-report \
  --dump-xcom
```

Expected artifacts:

- `.tmp/orders-success-report/report.md`
- `.tmp/orders-success-report/result.json`
- `.tmp/orders-success-report/tasks.csv`
- `.tmp/orders-success-report/junit.xml`
- `.tmp/orders-success-report/graph.svg`
- `.tmp/orders-success-report/graph.txt`
- `.tmp/orders-success-report/xcom.json`

The same DAG can be run directly because it includes an inline entrypoint:

```bash
python examples/orders_success_dag.py \
  --config-path examples/airflow_defaults.py \
  --report-dir .tmp/orders-success-report
```

## Debug a Failing Task

Use this when a task raises and you want a deterministic local failure report.
The default backend is strict fail-fast mode, so the first failing task aborts
the run and downstream tasks are reported as not run or upstream failed where
Airflow exposes that state.

```bash
airflow-debug-run examples/orders_failing_dag.py \
  --dag-id orders_failing \
  --config-path examples/airflow_defaults.py \
  --report-dir .tmp/orders-failing-report
```

Useful files after the failure:

- `report.md`: readable state summary and formatted exception
- `exception.txt`: raw traceback when available
- `tasks.csv`: task-by-task state table
- `junit.xml`: CI-compatible task failures

If you want to compare native Airflow retry behavior instead of fail-fast:

```bash
airflow-debug-run examples/orders_failing_dag.py \
  --dag-id orders_failing \
  --config-path examples/airflow_defaults.py \
  --no-fail-fast
```

## Mock an Unsafe External Task

Use this when the DAG graph should run locally but one task would call a real
warehouse, notebook job, API, queue, or billing-sensitive service.

The example DAG has a `load_to_warehouse` task that deliberately raises if it
is not mocked.

```bash
airflow-debug-run examples/orders_mocked_external_dag.py \
  --dag-id orders_mocked_external \
  --config-path examples/airflow_defaults.py \
  --mock-file examples/local.mocks.json \
  --dump-xcom \
  --report-dir .tmp/orders-mocked-report
```

The mock rule replaces the task with a successful local stub and pushes a
`return_value` XCom. Inspect it:

```bash
cat .tmp/orders-mocked-report/xcom.json
```

Use exact task ids for local fixtures that are part of the DAG contract:

```json
{
  "mocks": [
    {
      "name": "local warehouse load",
      "task_id": "load_to_warehouse",
      "xcom": {
        "return_value": {
          "rows_loaded": 2,
          "table": "analytics.orders"
        }
      }
    }
  ]
}
```

Use `operator` or `operator_glob` rules only when many equivalent generated
tasks should share the same local behavior.

## Run One Branch While Developing

Use partial runs when only one branch changed and the full DAG is slow.

Run only the transform task:

```bash
airflow-debug-run examples/orders_success_dag.py \
  --dag-id orders_success \
  --config-path examples/airflow_defaults.py \
  --task transform_orders
```

Run a task and everything downstream:

```bash
airflow-debug-run examples/orders_success_dag.py \
  --dag-id orders_success \
  --config-path examples/airflow_defaults.py \
  --start-task transform_orders \
  --report-dir .tmp/orders-partial-report
```

If a selected task expects upstream XComs from a task that was not selected,
either include the upstream task or mock the upstream XCom-producing task.
The runner adds a note to `RunResult.notes` when it detects selected tasks
with external upstream dependencies.

## Use the Pytest Fixture in CI

Use `airflow_local_runner` when a test should execute a DAG and inspect the
same `RunResult` shape used by the CLI.

```python
from pathlib import Path


def test_orders_dag_runs_locally(airflow_local_runner):
    result = airflow_local_runner.run_file(
        str(Path("examples/orders_success_dag.py")),
        dag_id="orders_success",
        config_path="examples/airflow_defaults.py",
        collect_xcoms=True,
    )

    assert result.ok, result.exception
    assert result.xcoms["load_to_warehouse"]["return_value"]["rows_loaded"] == 2
```

When CI needs artifacts, pass a report directory:

```python
result = airflow_local_runner.run_file(
    "examples/orders_success_dag.py",
    dag_id="orders_success",
    config_path="examples/airflow_defaults.py",
    report_dir="artifacts/airflow-debug/orders",
    include_graph_in_report=True,
)
```

## Preflight a Developer Machine

Use doctor before blaming DAG code. It checks Airflow import/version support,
metadata DB readiness, local config shape, DAG import, and optional Airflow 3
serialization.

```bash
airflow-debug-doctor \
  --config-path examples/airflow_defaults.py \
  --dag-file examples/orders_success_dag.py \
  --dag-id orders_success
```

Machine-readable output for CI:

```bash
airflow-debug-doctor \
  --config-path examples/airflow_defaults.py \
  --dag-file examples/orders_success_dag.py \
  --dag-id orders_success \
  --json
```

Pass the same `--env` and `--env-file` values that the real local run uses, so
config import checks and DAG import checks see the same environment.

## Watch the DAG While Editing

Use watch mode for the edit-save-run loop. After a failure, the next rerun
starts from the failed task and downstream tasks.

```bash
airflow-debug-run examples/orders_success_dag.py \
  --dag-id orders_success \
  --config-path examples/airflow_defaults.py \
  --watch \
  --watch-path examples
```

For deterministic watch loops, mock external tasks and keep local config small:

```bash
airflow-debug-run examples/orders_mocked_external_dag.py \
  --dag-id orders_mocked_external \
  --config-path examples/airflow_defaults.py \
  --mock-file examples/local.mocks.json \
  --watch
```

## Decide What to Upload From CI

Recommended CI artifacts:

- `report.md` for humans
- `result.json` for automation
- `tasks.csv` for timing comparison
- `junit.xml` for CI test UI
- `exception.txt` for failures
- `graph.svg` when topology review matters
- `xcom.json` when downstream tests use XCom fixtures

For a normal pull-request smoke check:

```bash
airflow-debug-run examples/orders_success_dag.py \
  --dag-id orders_success \
  --config-path examples/airflow_defaults.py \
  --report-dir "$CI_ARTIFACTS/airflow-debug/orders" \
  --dump-xcom
```
