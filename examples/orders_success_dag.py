from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from airflow_local_debug import debug_dag_cli


@dag(
    dag_id="orders_success",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["local-debug", "example"],
)
def build_orders_success_dag():
    @task
    def extract_orders() -> list[dict[str, int]]:
        return [
            {"order_id": 1, "amount": 10},
            {"order_id": 2, "amount": 25},
        ]

    @task
    def transform_orders(rows: list[dict[str, int]]) -> list[dict[str, int]]:
        return [{"order_id": row["order_id"], "amount_cents": row["amount"] * 100} for row in rows]

    @task
    def load_to_warehouse(rows: list[dict[str, int]]) -> dict[str, object]:
        return {"rows_loaded": len(rows), "table": "analytics.orders"}

    loaded = load_to_warehouse(transform_orders(extract_orders()))
    notify = EmptyOperator(task_id="notify")
    loaded >> notify


dag = build_orders_success_dag()


if __name__ == "__main__":
    debug_dag_cli(dag, require_config_path=False)
