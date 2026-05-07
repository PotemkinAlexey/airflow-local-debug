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
- mocked task rules when used
- deferrable task handling when detected
- task state summary
- per-task state and duration
- mini-Gantt chart of task timings when `start_date` data is available
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
Timing (total 1.65s):
  extract    [██████████████████████████████████              ] 1.24s
  transform  [                                  ██████████████]  411ms
Exception:
...
```

The Gantt chart is rendered automatically whenever Airflow reports
`start_date` and `duration_seconds` for at least one task. It uses absolute
offsets from the first task's start, so parallel branches and sequential
chains are both visible at a glance. Use it to spot slow tasks without
opening the full report directory.

The library API exposes `format_run_gantt(result, *, width=60)` for custom
rendering or width control.

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
| `xcom.json` | when XComs are collected and exist | Final XCom snapshot grouped by task label and key. |
| `graph.svg` | when `--report-dir` is used unless overridden | Visual DAG graph. |
| `graph.txt` | when ASCII graph exists | Console DAG graph. |
| `exception.txt` | when an exception exists | Raw traceback when available, otherwise formatted exception text. |

## `result.json`

`result.json` is the dataclass representation of `RunResult`, including nested `TaskRunInfo` rows. It is stable enough for CI parsing and post-processing.

Important fields:

- `ok` is not serialized as a field because it is a property. Use `state == "success"` and `exception is null`.
- `state` is normalized to lowercase.
- `selected_tasks` lists the effective partial-run task ids when `--task`,
  `--start-task`, `--task-group`, or the corresponding library arguments were used.
- `tasks[].duration_seconds` is present when Airflow exposes `start_date` and `end_date`.
- `tasks[].mocked` is true when a task was replaced by a local mock rule.
- `mocks[]` lists applied mock rules.
- `deferrables[]` lists detected deferrable tasks and the local handling mode.
- `xcoms` contains the collected XCom snapshot when `--dump-xcom`,
  `--xcom-json-path`, or `collect_xcoms=True` was used.

## `tasks.csv`

Columns:

- `task_id`
- `map_index`
- `state`
- `try_number`
- `start_date`
- `end_date`
- `duration_seconds`
- `mocked`

This is intended for spreadsheets, CI artifacts, and quick local timing comparisons.

## `xcom.json`

The XCom dump groups values by task label:

```json
{
  "load_to_snowflake": {
    "return_value": {
      "rows_loaded": 120
    }
  },
  "mapped_task[2]": {
    "return_value": "ok"
  }
}
```

Values are normalized to JSON-safe shapes. Datetime-like values are serialized
with `isoformat()`; unknown object types fall back to `str(value)`.

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
