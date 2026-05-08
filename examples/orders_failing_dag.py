from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from airflow_local_debug import debug_dag_cli


@dag(
    dag_id="orders_failing",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["local-debug", "example"],
)
def build_orders_failing_dag():
    @task
    def extract_orders() -> list[dict[str, int]]:
        return [{"order_id": 1, "amount": 10}]

    @task
    def transform_orders(rows: list[dict[str, int]]) -> list[dict[str, int]]:
        if rows:
            raise RuntimeError("Example transform failure")
        return rows

    transform_orders(extract_orders())


dag = build_orders_failing_dag()


if __name__ == "__main__":
    debug_dag_cli(dag, require_config_path=False)
