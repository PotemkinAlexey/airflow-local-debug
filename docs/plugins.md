# Plugins

Plugins let callers instrument local DAG execution without modifying DAG task code.

## Contract

Subclass `AirflowDebugPlugin` and pass instances through `plugins=[...]` on
`debug_dag`, `debug_dag_from_file`, `run_full_dag`, `run_full_dag_from_file`,
or the pytest fixture.

Plugins are for instrumentation and local guardrails. They should observe,
log, collect metadata, or add notes. They should not mutate DAG topology,
replace Airflow task behavior, or make external side effects that change the
meaning of the DAG run. Use task mocks for replacing task execution.

Plugin errors are isolated. If a plugin hook raises, the runner records a note
and continues with the DAG result. This keeps local instrumentation from
changing whether the DAG itself succeeds or fails.

Hook ordering:

1. run-level `before_run`
2. task-level hooks while tasks execute
3. run-level `after_run`

When the default strict backend is used, task trace hooks are driven by the
strict local scheduling loop. In non-strict `dag.test` mode, live tracing wraps
task methods and callbacks best-effort.

## Basic Plugin

```python
from airflow_local_debug import AirflowDebugPlugin, debug_dag

class LoggingPlugin(AirflowDebugPlugin):
    def before_run(self, dag, context):
        print(f"RUN {dag.dag_id}")

    def before_task(self, task, context):
        print(f"START {task.task_id}")

    def after_task(self, task, context, result):
        print(f"OK {task.task_id}")

    def on_task_error(self, task, context, error):
        print(f"FAIL {task.task_id}: {error!r}")

debug_dag(dag, plugins=[LoggingPlugin()])
```

## Hook Points

Run-level hooks:

- `before_run(dag, context)`
- `after_run(dag, context, result)`

Task-level hooks:

- `before_task(task, context)`
- `after_task(task, context, result)`
- `on_task_error(task, context, error)`

Callback hooks:

- `before_task_callback(task, context, callback_name)`
- `after_task_callback(task, context, callback_name, result)`
- `on_task_callback_error(task, context, callback_name, error)`

## Context

The runner passes a best-effort context dictionary. Available fields vary by hook and Airflow version. Common run context fields include:

- `config_path`
- `logical_date`
- `conf`
- `extra_env`
- `backend_hint`
- `graph_ascii`
- `notes`

Task hooks receive Airflow task context when available.

Context is deliberately best-effort because Airflow exposes different context
shapes across versions and backends. Treat missing fields as normal. Common
task context fields include:

- `ti`
- `task_instance`
- `run_id`

Common callback context fields include the original Airflow callback context
when Airflow provides one.

## Passing Plugins

Inline DAG entrypoint:

```python
from airflow_local_debug import AirflowDebugPlugin, debug_dag_cli


class NotesPlugin(AirflowDebugPlugin):
    def before_run(self, dag, context):
        context.setdefault("notes", []).append("local plugin enabled")


if __name__ == "__main__":
    debug_dag_cli(dag, plugins=[NotesPlugin()])
```

Python API:

```python
from airflow_local_debug import debug_dag

result = debug_dag(
    dag,
    plugins=[LoggingPlugin()],
    raise_on_failure=False,
)
```

Pytest fixture:

```python
def test_dag_with_plugin(airflow_local_runner):
    result = airflow_local_runner.run(dag, plugins=[LoggingPlugin()])
    assert result.ok
```

## Default Plugins

The runner installs these default plugins:

- `TaskContextPlugin`
- `ProblemLogPlugin`
- `ConsoleTracePlugin`

If you pass an instance of one of those exact plugin types, the default instance of that type is skipped to avoid duplicate behavior.

## Error Isolation

Plugin exceptions do not break DAG execution. They are captured as notes in `RunResult.notes`.

This is intentional: local debug instrumentation should not change the DAG's execution result.

## Live Trace Behavior

For normal `dag.test` execution, live tracing wraps task methods and callbacks. In strict fail-fast mode, the runner executes tasks through its own loop and drives trace hooks directly.

Use `trace=False` or `--no-trace` to disable console tracing.

## When Not to Use a Plugin

Use another mechanism instead when the goal is:

- replace or skip a task: use task mocks
- provide connections, variables, pools, or environment: use local config and
  `--env` / `--env-file`
- change which tasks run: use `--task`, `--start-task`, or `--task-group`
- parse final output: read `result.json`, `tasks.csv`, `junit.xml`, or
  `RunResult`
