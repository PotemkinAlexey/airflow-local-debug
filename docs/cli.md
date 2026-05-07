# CLI Usage

The package exposes two console scripts:

- `airflow-debug-run`: import a DAG file and run one DAG locally.
- `airflow-debug-doctor`: validate the local Airflow/debug environment.

## `airflow-debug-run`

Run a DAG from a Python file:

```bash
airflow-debug-run /absolute/path/to/my_dag.py \
  --dag-id my_dag \
  --config-path /absolute/path/to/airflow_defaults.py
```

List DAGs in a file without running them:

```bash
airflow-debug-run /absolute/path/to/my_dag.py --list-dags
```

Run with a logical date, DAG run conf, temporary environment variables, and artifacts:

```bash
airflow-debug-run /absolute/path/to/my_dag.py \
  --dag-id my_dag \
  --logical-date 2026-05-07T10:00:00+00:00 \
  --conf-json '{"dataset": "daily"}' \
  --env FEATURE_FLAG=local \
  --report-dir ./airflow-debug-report
```

Run with local task mocks and an XCom snapshot:

```bash
airflow-debug-run /absolute/path/to/my_dag.py \
  --dag-id my_dag \
  --mock-file ./local.mocks.json \
  --dump-xcom \
  --report-dir ./airflow-debug-report
```

Run only part of a DAG:

```bash
airflow-debug-run /absolute/path/to/my_dag.py \
  --dag-id my_dag \
  --start-task load_to_warehouse
```

### Run Flags

| Flag | Meaning |
|---|---|
| `--dag-id` | Select one DAG when the file defines multiple DAGs. |
| `--list-dags` | Print discovered DAG ids and task counts, then exit without running. |
| `--config-path` | Local Airflow config file with `CONNECTIONS`, `VARIABLES`, and `POOLS`. |
| `--logical-date` | Logical date / execution date for the local DAG run. |
| `--conf-json` | JSON object passed as `dag_run.conf`. |
| `--conf-file` | Path to a JSON object file passed as `dag_run.conf`. |
| `--env KEY=VALUE` | Extra environment variable for this run. May be repeated. |
| `--mock-file` | JSON/YAML task mock file. May be repeated. |
| `--task` | Run only the selected task id. May be repeated or comma-separated. |
| `--start-task` | Run the selected task id and all downstream tasks. May be repeated or comma-separated. |
| `--task-group` | Run tasks inside a TaskGroup id. May be repeated or comma-separated. |
| `--dump-xcom` | Collect final XComs into `result.json` and `xcom.json` artifacts. |
| `--xcom-json-path` | Write final XCom snapshot to an explicit JSON path. |
| `--no-trace` | Disable live per-task console tracing. |
| `--no-fail-fast` | Keep original retries instead of forcing fail-fast debug mode. |
| `--include-graph-in-report` | Include the ASCII DAG graph in the final report. |
| `--report-dir` | Write `report.md`, `result.json`, `tasks.csv`, `junit.xml`, `graph.svg`, and optional `graph.txt` / `exception.txt`. |
| `--graph-svg-path` | Write the rendered DAG graph SVG to an explicit path. Defaults to `graph.svg` inside `--report-dir`. |

`--conf-json` and `--conf-file` are mutually exclusive. The conf payload must be a JSON object.

### Task Mocks

`--mock-file` lets a local run replace selected task `execute()` calls with a
successful local stub. The runner still creates real task instances, marks them
through Airflow, and pushes configured XCom values. Mocked tasks are visible in
the console report, `result.json`, and `tasks.csv`.

Example `local.mocks.json`:

```json
{
  "mocks": [
    {
      "name": "snowflake load fixture",
      "task_id": "load_to_snowflake",
      "xcom": {
        "return_value": {
          "rows_loaded": 120,
          "table": "analytics.events"
        }
      }
    },
    {
      "operator": "DatabricksSubmitRunOperator",
      "return_value": {
        "run_id": "local-mock-run"
      }
    }
  ]
}
```

Supported selectors:

- `task_id`
- `task_id_glob`
- `operator` (class name, `task_type`, or full module path)
- `operator_glob`

Rules are required by default: if a rule matches no task, the run fails before
execution. Set `"required": false` for optional mocks.

### Partial Runs

Partial runs use Airflow's native `DAG.partial_subset(...)` before execution.
The rendered graph, executed task instances, report, and `result.json` are scoped
to the selected subset.

- `--task load`: run only `load`.
- `--start-task load`: run `load` and all downstream tasks.
- `--task-group warehouse`: run tasks in `warehouse` and nested task groups.

The final selected task ids are available as `RunResult.selected_tasks`.

#### Upstream XCom dependencies

When the selected subgraph has upstream tasks outside the selection, the
runner emits a note:

```
Partial run skips upstream task(s) that selected task(s) depend on:
transform <- extract. XCom pulls from these upstreams will return None.
Provide --mock-file to inject upstream XCom values, or include the upstream
chain via additional --task / --start-task selectors.
```

This catches the most common foot-gun: `--start-task transform` runs
`transform`, but `transform` calls `xcom_pull(task_ids="extract")` which
returns `None` because `extract` was not in the partial run. Two ways to
fix:

1. Provide a mock for the missing upstream:

   ```bash
   airflow-debug-run pipeline.py --start-task transform \
     --mock-file ./upstream-mocks.json
   ```

   ```json
   {"mocks": [{"task_id": "extract", "xcom": {"return_value": [1, 2, 3]}}]}
   ```

   When a `--mock-file` covers a missing upstream by exact `task_id`, the
   runner emits a different note (`Partial run upstream XCom mocks active
   for: extract.`) instead of the warning.

2. Extend the selection to include the upstream chain:

   ```bash
   airflow-debug-run pipeline.py --start-task extract
   ```

### XCom Dumps

Use `--dump-xcom` with `--report-dir` to write `xcom.json` when XComs exist.
Use `--xcom-json-path /tmp/xcom.json` when you want a standalone fixture file.
The same snapshot is also available as `RunResult.xcoms`.

### Exit Codes

- `0`: DAG run succeeded, or `--list-dags` completed successfully.
- `1`: DAG run failed or the environment check failed.
- `2`: argparse usage error.

## Inline DAG Entrypoint

Inside a DAG file:

```python
from airflow_local_debug import debug_dag_cli

if __name__ == "__main__":
    debug_dag_cli(dag, require_config_path=True)
```

Run:

```bash
python my_dag.py --config-path /absolute/path/to/airflow_defaults.py
```

Inline `debug_dag_cli` accepts the same run flags as `airflow-debug-run`, except file-selection flags such as `--dag-id` and `--list-dags`.

## `airflow-debug-doctor`

Validate Airflow import/version support, metadata DB readiness, local config shape, and optional DAG import/serialization:

```bash
airflow-debug-doctor \
  --config-path /absolute/path/to/airflow_defaults.py \
  --dag-file /absolute/path/to/my_dag.py \
  --dag-id my_dag
```

Use JSON output for CI or scripts:

```bash
airflow-debug-doctor --json --dag-file /absolute/path/to/my_dag.py
```

Doctor flags:

| Flag | Meaning |
|---|---|
| `--config-path` | Local config file to validate. |
| `--require-config` | Fail when no config file is explicitly configured. |
| `--dag-file` | Optional DAG file to import and validate. |
| `--dag-id` | Select one DAG when the file defines multiple DAGs. |
| `--json` | Print machine-readable JSON. |
