# airflow-local-debug examples

These examples are small DAGs for local debugging workflows. They are safe to
run on a developer machine and pair with the recipes in `docs/recipes.md`.

## Files

- `orders_success_dag.py`: successful TaskFlow DAG with XCom flow.
- `orders_failing_dag.py`: deterministic failure for report and JUnit checks.
- `orders_mocked_external_dag.py`: DAG with an unsafe external task that should
  be replaced by `local.mocks.json`.
- `airflow_defaults.py`: local Connections, Variables, and Pools.
- `local.mocks.json`: task mock fixture for `orders_mocked_external_dag.py`.
- `pytest/test_orders_debug.py`: pytest fixture example.

## Run

```bash
airflow-debug-run examples/orders_success_dag.py \
  --dag-id orders_success \
  --config-path examples/airflow_defaults.py \
  --report-dir .tmp/orders-success-report \
  --dump-xcom
```

```bash
airflow-debug-run examples/orders_mocked_external_dag.py \
  --dag-id orders_mocked_external \
  --config-path examples/airflow_defaults.py \
  --mock-file examples/local.mocks.json \
  --report-dir .tmp/orders-mocked-report \
  --dump-xcom
```
