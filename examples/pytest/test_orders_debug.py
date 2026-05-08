from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_orders_success_dag_runs_locally(airflow_local_runner) -> None:
    result = airflow_local_runner.run_file(
        str(ROOT / "examples" / "orders_success_dag.py"),
        dag_id="orders_success",
        config_path=str(ROOT / "examples" / "airflow_defaults.py"),
        collect_xcoms=True,
    )

    assert result.ok, result.exception
    assert result.xcoms["load_to_warehouse"]["return_value"]["rows_loaded"] == 2


def test_orders_external_task_can_be_mocked(airflow_local_runner) -> None:
    result = airflow_local_runner.run_file(
        str(ROOT / "examples" / "orders_mocked_external_dag.py"),
        dag_id="orders_mocked_external",
        config_path=str(ROOT / "examples" / "airflow_defaults.py"),
        mock_files=[str(ROOT / "examples" / "local.mocks.json")],
        collect_xcoms=True,
    )

    assert result.ok, result.exception
    assert result.mocks[0].task_id == "load_to_warehouse"
    assert result.xcoms["load_to_warehouse"]["return_value"]["table"] == "analytics.orders"
