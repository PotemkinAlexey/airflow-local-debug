from __future__ import annotations

from typing import Any

from airflow_local_debug.reporting.console import print_run_preamble


class FakeDag:
    dag_id = "demo"


def test_print_run_preamble_includes_optional_context(capsys: Any) -> None:
    output = print_run_preamble(
        FakeDag(),
        backend_hint="dag.test.strict",
        config_path="/tmp/config.py",
        logical_date="2026-01-02T00:00:00+00:00",
        graph_text="demo\n  task",
    )

    expected = "\n".join(
        [
            "Starting local DAG run: demo",
            "Backend: dag.test.strict",
            "Logical date: 2026-01-02T00:00:00+00:00",
            "Config: /tmp/config.py",
            "",
            "demo\n  task",
        ]
    )
    assert output == expected
    assert capsys.readouterr().out == expected + "\n"


def test_print_run_preamble_handles_minimal_unknown_dag(capsys: Any) -> None:
    output = print_run_preamble(object())

    assert output == "Starting local DAG run: <unknown>"
    assert capsys.readouterr().out == "Starting local DAG run: <unknown>\n"
