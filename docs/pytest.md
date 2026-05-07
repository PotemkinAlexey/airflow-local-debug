# Pytest fixture

`airflow-local-debug` ships a built-in pytest plugin that exposes the local
runner as the `airflow_local_runner` fixture. The plugin is auto-registered
via the `pytest11` entry point, so installing the package is enough — no
`conftest.py` glue is required.

## Quick start

```python
def test_my_dag(airflow_local_runner):
    result = airflow_local_runner.run_dag(
        "/abs/path/to/my_dag.py",
        dag_id="my_dag",
        config_path="/abs/path/to/airflow_defaults.py",
    )
    assert result.ok
    assert {task.task_id for task in result.tasks} >= {"extract", "load"}
```

## Fixture

`airflow_local_runner` returns an `AirflowLocalRunner` instance with a single
method:

```python
runner.run_dag(
    target,                    # DAG object OR path to a DAG file (str / Path)
    *,
    dag_id=None,               # required when target is a file with multiple DAGs
    config_path=None,
    logical_date=None,
    conf=None,
    extra_env=None,
    trace=False,               # silenced by default for clean test output
    fail_fast=True,
    plugins=None,
    task_mocks=None,
    task_ids=None,             # partial run: exact task ids
    start_task_ids=None,       # partial run: roots + their downstreams
    task_group_ids=None,       # partial run: whole task groups
    collect_xcoms=True,        # XComs are collected by default for assertions
)
```

`run_dag` returns a regular `RunResult` with the same shape as the CLI
runners, so the same assertions you would write against `result.json` work
verbatim in tests.

## Integration test patterns

Mock heavy operators and assert on the full graph result:

```python
from airflow_local_debug import TaskMockRule

def test_pipeline_succeeds_with_mocks(airflow_local_runner):
    result = airflow_local_runner.run_dag(
        "/abs/path/to/pipeline.py",
        task_mocks=[
            TaskMockRule(task_id="load_to_snowflake", xcom={"return_value": {"rows": 100}}),
        ],
    )
    assert result.ok
    assert result.xcoms["load_to_snowflake"]["return_value"]["rows"] == 100
```

Run only a subgraph:

```python
def test_only_transforms(airflow_local_runner):
    result = airflow_local_runner.run_dag(
        dag_object,
        start_task_ids=["transform_orders"],
    )
    assert result.ok
    assert result.selected_tasks == ["transform_orders", "validate_orders", "publish_orders"]
```

Pass a DAG object directly (skipping module discovery):

```python
def test_in_process_dag(airflow_local_runner, my_dag):
    result = airflow_local_runner.run_dag(my_dag, fail_fast=True)
    assert result.ok
```

## Notes

- The fixture is function-scoped; each test gets a fresh runner.
- Each call still requires a working Airflow metadata DB. In CI, initialize
  it once with `airflow db migrate` against a temporary `AIRFLOW_HOME`.
- `trace` defaults to `False` so test output stays clean. Set `trace=True`
  when you want the live console tracer for failure diagnosis.
