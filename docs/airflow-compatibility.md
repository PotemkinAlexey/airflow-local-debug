# Airflow Compatibility

Supported package range:

```text
apache-airflow>=2.10,!=3.0.*,<4
```

Validated versions:

- Airflow 2.10.x
- Airflow 2.11.x
- Airflow 3.1.3
- Airflow 3.2.1

## Why Airflow 3.0.x Is Excluded

Airflow 3.0.x is intentionally excluded. Smoke runs against Airflow 3.0.6 fail inside Airflow's local execution path with HTTP 422 even for a trivial `EmptyOperator` DAG.

That means `dag.test()`-style local execution cannot complete reliably on 3.0.x. The exclusion is a compatibility guard around an upstream limitation, not a workaround for a bug in this package.

Use Airflow 3.1+ for Airflow 3 local execution.

## Backends

The runner chooses one of these backends:

| Backend | When used |
|---|---|
| `dag.test.strict` | Default fail-fast mode when `dag.test()` exists. |
| `dag.test` | `fail_fast=False` and Airflow exposes `dag.test()`. |
| `dag.run` | Older Airflow versions without `dag.test()`. |
| `unsupported` | Neither local execution method is available. |

## Strict Fail-Fast Mode

`fail_fast=True` is the default. The runner:

1. disables task retries for the duration of the run,
2. executes schedulable task instances in topological order,
3. aborts deterministically when a task hard-fails,
4. restores original retry settings afterward.

This behavior is optimized for local debugging, where repeated retry delays usually hide the first useful failure.

Disable it when you explicitly need native retry behavior:

```bash
airflow-debug-run /absolute/path/to/my_dag.py --no-fail-fast
```

or:

```python
run_full_dag(dag, fail_fast=False)
```

## Deferrable Operators

The runner detects tasks that look deferrable before execution. Detection uses
common Airflow/provider signals such as `deferrable=True`,
`start_from_trigger=True`, or `start_trigger_args`.

Detected tasks are included in `RunResult.deferrables`, `result.json`, and the
console report. The report also records the local handling mode:

| Backend | Deferrable handling |
|---|---|
| `dag.test.strict` | Uses strict local loop with inline trigger handling when Airflow exposes it. |
| `dag.test` | Uses native `dag.test`; behavior may depend on Airflow/provider trigger behavior. |
| `dag.run` | Legacy path may leave tasks deferred; prefer default fail-fast mode or task mocks. |

If a task instance remains in `deferred` state after a local run, the final
report adds an actionable note suggesting default `fail_fast=True` or
`--mock-file` for that task.

## CI Matrix

The repository CI validates a compatibility matrix across Python and Airflow versions. It also runs a runtime smoke script that checks:

- doctor command behavior,
- successful strict DAG run,
- failed strict DAG run with downstream normalization,
- partial strict DAG run selection,
- report artifact output (`report.md`, `result.json`, `tasks.csv`,
  `junit.xml`, `graph.svg`, `graph.txt`, and `xcom.json`),
- wheel/sdist build.

Run the same local smoke shape with:

```bash
make smoke
```

`make smoke` creates a temporary `AIRFLOW_HOME`, runs `airflow db migrate`, and then executes `tests/smoke_airflow_runtime.py`.
