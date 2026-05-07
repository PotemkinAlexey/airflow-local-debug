from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path

import airflow

from airflow_local_debug import (
    is_supported_airflow_version,
    run_doctor,
    run_full_dag,
)

try:
    from airflow.sdk import DAG
except Exception:
    from airflow import DAG  # type: ignore[assignment]

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.python import PythonOperator
except Exception:
    from airflow.operators.empty import EmptyOperator  # type: ignore[assignment]
    from airflow.operators.python import PythonOperator  # type: ignore[assignment]


def _boom(**_context):
    raise RuntimeError("expected smoke failure")


def _write_doctor_files(tmp_path: Path) -> tuple[Path, Path]:
    config_path = tmp_path / "airflow_defaults.py"
    config_path.write_text(
        "\n".join(
            [
                "CONNECTIONS = {'demo_http': {'conn_type': 'http', 'host': 'example.com'}}",
                "VARIABLES = {'ENV': 'local'}",
                "POOLS = {'default_pool': {'slots': 2}}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    dag_path = tmp_path / "doctor_smoke_dag.py"
    dag_path.write_text(
        "\n".join(
            [
                "from datetime import datetime, timezone",
                "try:",
                "    from airflow.sdk import DAG",
                "except Exception:",
                "    from airflow import DAG",
                "try:",
                "    from airflow.providers.standard.operators.empty import EmptyOperator",
                "except Exception:",
                "    from airflow.operators.empty import EmptyOperator",
                "",
                "with DAG(",
                "    dag_id='doctor_smoke',",
                "    schedule=None,",
                "    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),",
                ") as dag:",
                "    EmptyOperator(task_id='hello')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return config_path, dag_path


def _run_doctor_smoke() -> None:
    with tempfile.TemporaryDirectory(prefix="ald-doctor-") as raw_tmp:
        config_path, dag_path = _write_doctor_files(Path(raw_tmp))
        result = run_doctor(
            config_path=str(config_path),
            require_config=True,
            dag_path=str(dag_path),
            dag_id="doctor_smoke",
        )
    assert result.ok, result.checks


def _run_success_smoke() -> None:
    with DAG(
        dag_id="ald_ci_success",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    ) as dag:
        EmptyOperator(task_id="hello")

    result = run_full_dag(
        dag,
        logical_date=datetime(2026, 1, 2, tzinfo=timezone.utc),
        trace=False,
        fail_fast=True,
    )
    assert result.backend == "dag.test.strict"
    assert result.ok, result.exception_raw
    assert [(task.task_id, task.state) for task in result.tasks] == [("hello", "success")]


def _run_fail_fast_smoke() -> None:
    with DAG(
        dag_id="ald_ci_fail_fast",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    ) as dag:
        first = PythonOperator(task_id="first", python_callable=_boom)
        second = EmptyOperator(task_id="second")
        first >> second

    result = run_full_dag(
        dag,
        logical_date=datetime(2026, 1, 3, tzinfo=timezone.utc),
        trace=False,
        fail_fast=True,
    )
    assert result.backend == "dag.test.strict"
    assert result.state == "failed"
    assert {task.task_id: task.state for task in result.tasks} == {
        "first": "failed",
        "second": "upstream_failed",
    }


def _run_partial_smoke() -> None:
    with DAG(
        dag_id="ald_ci_partial",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    ) as dag:
        first = EmptyOperator(task_id="first")
        middle = EmptyOperator(task_id="middle")
        last = EmptyOperator(task_id="last")
        first >> middle >> last

    result = run_full_dag(
        dag,
        logical_date=datetime(2026, 1, 4, tzinfo=timezone.utc),
        trace=False,
        fail_fast=True,
        start_task_ids=["middle"],
    )
    assert result.backend == "dag.test.strict"
    assert result.ok, result.exception_raw
    assert result.selected_tasks == ["middle", "last"]
    assert [(task.task_id, task.state) for task in result.tasks] == [("middle", "success"), ("last", "success")]


def main() -> None:
    version = getattr(airflow, "__version__", None)
    assert is_supported_airflow_version(version), f"Unsupported Airflow version in smoke: {version}"
    print(f"Running airflow-local-debug smoke against apache-airflow {version}")
    _run_doctor_smoke()
    _run_success_smoke()
    _run_fail_fast_smoke()
    _run_partial_smoke()


if __name__ == "__main__":
    main()
