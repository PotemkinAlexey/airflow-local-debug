DAG: orders_success
State: success
Backend: dag.test.strict
Airflow: 3.2.1
Logical date: 2026-05-07T00:00:00+00:00
Config: /repo/examples/airflow_defaults.py
Selected tasks: extract_orders, transform_orders, load_to_warehouse
Graph SVG: /repo/.tmp/orders-success-report/graph.svg

orders_success
  extract_orders -> transform_orders
  transform_orders -> load_to_warehouse
Notes:
- Loaded local config from /repo/examples/airflow_defaults.py
Mocked tasks:
- load_to_warehouse: success via local warehouse load xcom=return_value
Task summary: success=3
Tasks:
- extract_orders: success (250ms)
- transform_orders: success (500ms)
- load_to_warehouse: success (750ms) [mocked]
