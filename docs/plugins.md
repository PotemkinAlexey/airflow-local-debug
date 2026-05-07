# Plugins

Plugins let callers instrument local DAG execution without modifying DAG task code.

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
