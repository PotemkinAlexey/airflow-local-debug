# Library API

Use the library API when you want to run DAGs from Python code, tests, or custom local tools.

## Convenience Entrypoints

### `debug_dag`

`debug_dag` runs a DAG, prints the standard report, and exits with `SystemExit(1)` on failure by default.

```python
from airflow_local_debug import debug_dag

debug_dag(
    dag,
    config_path="/absolute/path/to/airflow_defaults.py",
    logical_date="2026-05-07",
    conf={"dataset": "daily"},
    extra_env={"FEATURE_FLAG": "local"},
    report_dir="./airflow-debug-report",
)
```

Set `raise_on_failure=False` if you want to handle the result yourself:

```python
result = debug_dag(dag, raise_on_failure=False)
if not result.ok:
    print(result.exception)
```

### `debug_dag_cli`

`debug_dag_cli` is intended for `if __name__ == "__main__"` blocks in DAG files:

```python
from airflow_local_debug import debug_dag_cli

if __name__ == "__main__":
    debug_dag_cli(dag, require_config_path=True)
```

Programmatic defaults can still be passed:

```python
debug_dag_cli(
    dag,
    require_config_path=True,
    conf={"dataset": "daily"},
    extra_env={"FEATURE_FLAG": "local"},
)
```

CLI-provided values override programmatic values for the same option.

### `debug_dag_from_file`

Use `debug_dag_from_file` when environment bootstrap must happen before the DAG module is imported:

```python
from airflow_local_debug import debug_dag_from_file

debug_dag_from_file(
    "/absolute/path/to/my_dag.py",
    dag_id="my_dag",
    config_path="/absolute/path/to/airflow_defaults.py",
)
```

## Low-Level Entrypoints

### `run_full_dag`

`run_full_dag` executes a DAG and returns `RunResult` without printing the final report:

```python
from airflow_local_debug import run_full_dag

result = run_full_dag(
    dag,
    config_path="/absolute/path/to/airflow_defaults.py",
    logical_date="2026-05-07T10:00:00+00:00",
    conf={"dataset": "daily"},
    trace=False,
)
assert result.ok, result.exception
```

Collect XComs for pytest fixtures:

```python
result = run_full_dag(dag, collect_xcoms=True)
assert result.xcoms["load_to_warehouse"]["return_value"]["rows_loaded"] == 120
```

### `run_full_dag_from_file`

`run_full_dag_from_file` imports a DAG file after applying local environment bootstrap:

```python
from airflow_local_debug import run_full_dag_from_file

result = run_full_dag_from_file(
    "/absolute/path/to/my_dag.py",
    dag_id="my_dag",
    config_path="/absolute/path/to/airflow_defaults.py",
)
```

## DAG Discovery

List DAGs from a Python file without running them:

```python
from airflow_local_debug import list_dags_from_file, format_dag_list

infos = list_dags_from_file("/absolute/path/to/my_dag.py")
print(format_dag_list(infos))
```

Each `DagFileInfo` contains:

- `dag_id`
- `task_count`
- `fileloc`

## Task Mocks

Use task mocks when local debug should validate orchestration and XCom contracts
without calling a heavy external connector.

```python
from airflow_local_debug import TaskMockRule, run_full_dag

result = run_full_dag(
    dag,
    task_mocks=[
        TaskMockRule(
            task_id="load_to_snowflake",
            name="warehouse fixture",
            xcom={"return_value": {"rows_loaded": 120}},
        )
    ],
    collect_xcoms=True,
)
```

Rules can match by exact `task_id`, `task_id_glob`, exact `operator`, or
`operator_glob`. `operator` matches the task class name, Airflow `task_type`,
or full module path. A required rule that matches no task fails the run before
execution; set `required=False` for optional local rules.

For file-driven configs:

```python
from airflow_local_debug import load_task_mock_rules, run_full_dag

rules = load_task_mock_rules("./local.mocks.json")
result = run_full_dag(dag, task_mocks=rules)
```

## Doctor API

```python
from airflow_local_debug import (
    format_doctor_json,
    format_doctor_report,
    run_doctor,
)

result = run_doctor(
    config_path="/absolute/path/to/airflow_defaults.py",
    dag_path="/absolute/path/to/my_dag.py",
    dag_id="my_dag",
)

print(format_doctor_report(result))
print(format_doctor_json(result))
raise SystemExit(result.exit_code)
```

## Result Types

`RunResult` fields:

- `dag_id`
- `run_id`
- `state`
- `logical_date`
- `backend`
- `airflow_version`
- `config_path`
- `graph_ascii`
- `graph_svg_path`
- `tasks`
- `mocks`
- `xcoms`
- `notes`
- `exception`
- `exception_raw`
- `exception_was_logged`

`TaskRunInfo` fields:

- `task_id`
- `state`
- `try_number`
- `map_index`
- `start_date`
- `end_date`
- `duration_seconds`
- `mocked`

`TaskMockInfo` fields:

- `task_id`
- `mode`
- `rule_name`
- `xcom_keys`

`RunResult.ok` is true only when the run state is `success` and no exception was recorded.

## Warning Suppression

For library-mode usage, prefer the context manager:

```python
from airflow_local_debug import run_full_dag, silenced_airflow_bootstrap_warnings

with silenced_airflow_bootstrap_warnings():
    result = run_full_dag(dag)
```

`ensure_quiet_airflow_bootstrap()` is used by the CLI and may re-exec the process. It is intended for one-shot command-line entrypoints, not reusable library code.
