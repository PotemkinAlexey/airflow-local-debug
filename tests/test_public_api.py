from __future__ import annotations

import airflow_local_debug


def test_public_api_exports_core_entrypoints() -> None:
    assert hasattr(airflow_local_debug, "debug_dag")
    assert hasattr(airflow_local_debug, "debug_dag_from_file")
    assert hasattr(airflow_local_debug, "debug_dag_cli")
    assert hasattr(airflow_local_debug, "run_full_dag")
    assert hasattr(airflow_local_debug, "list_dags_from_file")
    assert hasattr(airflow_local_debug, "format_dag_list")
    assert hasattr(airflow_local_debug, "run_doctor")
    assert hasattr(airflow_local_debug, "format_doctor_report")
    assert hasattr(airflow_local_debug, "format_doctor_json")
    assert hasattr(airflow_local_debug, "doctor_result_to_dict")
    assert hasattr(airflow_local_debug, "write_run_artifacts")
    assert hasattr(airflow_local_debug, "DagFileInfo")
    assert hasattr(airflow_local_debug, "AirflowDebugPlugin")
