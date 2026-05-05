# airflow-local-debug

Single-process local debug toolkit for ordinary Apache Airflow DAGs.

## What it does

- runs a full DAG locally without scheduler/webserver
- keeps native Airflow task and XCom behavior
- adds deterministic fail-fast execution for debug
- prints a console DAG graph before execution
- provides live per-task tracing and structured problem logging

## Installation

```bash
pip install -e .
```

## Quick start

### Direct DAG entrypoint

```python
from airflow_local_debug import debug_dag_cli

if __name__ == "__main__":
    debug_dag_cli(dag, require_config_path=True)
```

Run:

```bash
python my_dag.py --config-path /absolute/path/to/airflow_defaults.py
```

### File-based runner

```bash
airflow-debug-run /absolute/path/to/my_dag.py \
  --dag-id my_dag \
  --config-path /absolute/path/to/airflow_defaults.py
```

## Local config

The library can load a Python config module with optional globals:

- `CONNECTIONS`
- `VARIABLES`
- `POOLS`

Config path resolution order:

1. explicit `--config-path`
2. `AIRFLOW_DEBUG_LOCAL_CONFIG`
3. `RUNBOOK_LOCAL_CONFIG`

The library does not assume any repository-specific fallback path.
