# airflow-local-debug Documentation

`airflow-local-debug` runs ordinary Apache Airflow DAGs in a single local
Python process without a scheduler or webserver. It is intended for fast DAG
development, reproducible local debugging, and CI smoke checks.

Start here:

- [Feature guide with examples](features.md): practical examples for each major capability.
- [CLI usage](cli.md): `airflow-debug-run`, `airflow-debug-doctor`, flags, and exit codes.
- [Library API](library-api.md): importable APIs for DAG code and Python test suites.
- [Pytest fixture](pytest.md): `airflow_local_runner` fixture for integration tests.
- [Watch mode](watch.md): `--watch` hot-reload loop with retry-from-failed-task.
- [Local config](local-config.md): local connections, variables, pools, and environment precedence.
- [Reports and artifacts](reports.md): console report, `result.json`, `tasks.csv`, `junit.xml`, graph files.
- [Plugins](plugins.md): hook points and custom runtime instrumentation.
- [Airflow compatibility](airflow-compatibility.md): supported Airflow versions and the Airflow 3.0 exclusion.

## Typical Workflow

1. Add an inline entrypoint to a DAG file:

   ```python
   from airflow_local_debug import debug_dag_cli

   if __name__ == "__main__":
       debug_dag_cli(dag, require_config_path=True)
   ```

2. Create a local config file for connections, variables, and pools.

3. Run the DAG locally:

   ```bash
   python my_dag.py \
     --config-path /absolute/path/to/airflow_defaults.py \
     --logical-date 2026-05-07 \
     --report-dir ./airflow-debug-report
   ```

4. Inspect the final report and artifacts:

   - `report.md`: human-readable run summary
   - `result.json`: structured `RunResult`
   - `tasks.csv`: task states and timings
   - `junit.xml`: CI-compatible test report
   - `graph.svg`: DAG graph preview

## Design Notes

- The runner executes the real DAG code and keeps native Airflow task/XCom behavior.
- `fail_fast=True` is the default for local debugging; retries are disabled during the run and restored afterward.
- Partial runs use Airflow's native DAG subset support, so reports and task instances are scoped to the selected subgraph.
- Airflow metadata DB must be initialized because Airflow task execution still needs normal metadata tables.
- The package intentionally excludes Airflow 3.0.x. Use Airflow 3.1+ for Airflow 3 local execution.
