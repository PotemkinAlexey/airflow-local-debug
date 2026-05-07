# Watch / hot-reload mode

`airflow-debug-run --watch` runs the DAG, prints the report, and then waits
for any watched file to change. On a change it purges cached modules,
re-imports the DAG file, and re-runs:

- if the previous run failed at a task, the next run uses
  `--start-task=<failed_task>` so only the failed subgraph executes;
- if the previous run succeeded, the next run is a full re-run.

This mirrors the typical SQL/Python edit loop: change a file, save, see the
result without restarting your shell.

## Quick start

```bash
airflow-debug-run /abs/path/to/my_dag.py \
  --dag-id my_dag \
  --config-path /abs/path/to/airflow_defaults.py \
  --watch
```

Press `Ctrl+C` to exit the loop.

## Flags

| Flag | Default | Meaning |
|---|---|---|
| `--watch` | off | Enable the hot-reload loop. |
| `--watch-path PATH` | DAG file's parent dir | Extra file or directory to watch. May be passed multiple times. |
| `--watch-interval SECONDS` | `0.5` | Polling interval. Lower = faster reaction, higher CPU cost. |

## What gets watched

By default the watcher tracks the DAG file and its parent directory
recursively. The polling pass picks up files with these suffixes:

- `.py` (DAG and helper modules)
- `.sql` (templated SQL referenced from operators)
- `.yaml` / `.yml` (mock files, configs)
- `.json` (mock files, fixtures)

Use `--watch-path` to add extra roots, e.g. a separate SQL directory shared
across multiple DAGs.

## Module reload semantics

Between iterations the watcher drops every entry from `sys.modules` whose
source file lives under a watch root. The DAG file is re-imported from disk
on the next iteration via the same loader the file CLI uses, so helper
modules sitting next to the DAG are picked up correctly.

Modules outside the watch roots (Airflow itself, providers, third-party
libraries) are kept cached.

## Failure → fix → retry workflow

```
$ airflow-debug-run pipeline.py --watch
[watch] watching 2 path(s); save a file to retry, Ctrl+C to exit
... DAG runs ...
[watch] run failed at task 'transform'. Save a fix to retry from this task...
# you edit transform.sql, save the file
[watch] detected change in /abs/path/transform.sql; reloading...
[watch] re-running starting from failed task: transform
... DAG runs again, only `transform` and its downstreams ...
[watch] run succeeded. Waiting for file changes...
```

The retry from the failed task uses the existing partial-run engine
(`--start-task`), so only the failed task and its downstream subgraph are
recomputed. Upstream task XComs are not preserved between iterations because
each iteration is an independent `run_full_dag_from_file` call — for
debugging logic in isolation, combine `--watch` with `--mock-file` so
upstream output is deterministic.

## Library API

```python
from airflow_local_debug import watch_dag_file

watch_dag_file(
    "/abs/path/to/my_dag.py",
    dag_id="my_dag",
    config_path="/abs/path/to/airflow_defaults.py",
    watch_paths=["/abs/path/to/sql"],
    poll_interval=0.5,
)
```

The function loops until interrupted and returns the most recent `RunResult`.
