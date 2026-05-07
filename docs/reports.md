# Reports and Artifacts

The runner always prints a final text report. When `--report-dir` or `report_dir=...` is provided, it also writes structured artifacts.

## Console Report

The final report includes:

- DAG id
- final run state
- backend (`dag.test.strict`, `dag.test`, `dag.run`, or `unsupported`)
- Airflow version
- logical date
- config path
- graph SVG path when generated
- notes from bootstrap/plugins
- task state summary
- per-task state and duration
- formatted exception when it was not already logged live

Example:

```text
DAG: my_dag
State: failed
Backend: dag.test.strict
Airflow: 3.2.1
Task summary: failed=1, success=2
Tasks:
- extract: success (1.24s)
- transform: failed (411ms)
Exception:
...
```

## Artifact Directory

```bash
airflow-debug-run /absolute/path/to/my_dag.py \
  --dag-id my_dag \
  --report-dir ./airflow-debug-report
```

Artifacts:

| File | When written | Purpose |
|---|---|---|
| `report.md` | always | Human-readable run summary. |
| `result.json` | always | Complete structured `RunResult`. |
| `tasks.csv` | when tasks exist | Flat task table with state, try number, dates, and duration. |
| `junit.xml` | when tasks exist | CI test report, one testcase per Airflow task. |
| `graph.svg` | when `--report-dir` is used unless overridden | Visual DAG graph. |
| `graph.txt` | when ASCII graph exists | Console DAG graph. |
| `exception.txt` | when an exception exists | Raw traceback when available, otherwise formatted exception text. |

## `result.json`

`result.json` is the dataclass representation of `RunResult`, including nested `TaskRunInfo` rows. It is stable enough for CI parsing and post-processing.

Important fields:

- `ok` is not serialized as a field because it is a property. Use `state == "success"` and `exception is null`.
- `state` is normalized to lowercase.
- `tasks[].duration_seconds` is present when Airflow exposes `start_date` and `end_date`.

## `tasks.csv`

Columns:

- `task_id`
- `map_index`
- `state`
- `try_number`
- `start_date`
- `end_date`
- `duration_seconds`

This is intended for spreadsheets, CI artifacts, and quick local timing comparisons.

## `junit.xml`

The JUnit artifact maps Airflow task instances to test cases:

- `failed`, `up_for_retry`, `upstream_failed`, `shutdown` become failures.
- `skipped`, `not_run`, `removed` become skipped test cases.
- `duration_seconds` becomes testcase time.

This lets CI systems display DAG task failures without custom parsing.

## Graph Artifacts

The ASCII graph is capped at 500 tasks. The SVG graph is capped at 200 tasks. Larger DAGs produce a placeholder instead of trying to render an unreadable graph.

Use an explicit SVG path when the report directory is not desired:

```bash
airflow-debug-run /absolute/path/to/my_dag.py \
  --dag-id my_dag \
  --graph-svg-path /tmp/my_dag.svg
```
