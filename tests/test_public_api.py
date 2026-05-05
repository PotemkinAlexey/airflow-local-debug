from __future__ import annotations

import airflow_local_debug


def test_public_api_exports_core_entrypoints() -> None:
    assert hasattr(airflow_local_debug, "debug_dag")
    assert hasattr(airflow_local_debug, "debug_dag_from_file")
    assert hasattr(airflow_local_debug, "debug_dag_cli")
    assert hasattr(airflow_local_debug, "run_full_dag")
    assert hasattr(airflow_local_debug, "AirflowDebugPlugin")
