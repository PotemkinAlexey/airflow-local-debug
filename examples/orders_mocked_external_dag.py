from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

from airflow_local_debug import debug_dag_cli


def unsafe_warehouse_load() -> dict[str, object]:
    raise RuntimeError("This example task should be mocked for local runs")


@dag(
    dag_id="orders_mocked_external",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["local-debug", "example"],
)
def build_orders_mocked_external_dag():
    @task
    def extract_orders() -> list[dict[str, int]]:
        return [
            {"order_id": 1, "amount": 10},
            {"order_id": 2, "amount": 25},
        ]

    load_to_warehouse = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=unsafe_warehouse_load,
        pool="warehouse_pool",
    )

    extract_orders() >> load_to_warehouse


dag = build_orders_mocked_external_dag()


if __name__ == "__main__":
    debug_dag_cli(dag, require_config_path=False)
