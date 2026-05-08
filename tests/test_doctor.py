from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from airflow_local_debug.doctor import (
    DoctorCheck,
    DoctorResult,
    build_parser,
    check_dag_file,
    check_local_config,
    check_metadata_db,
    doctor_result_to_dict,
    format_doctor_json,
    format_doctor_report,
    is_supported_airflow_version,
    run_doctor,
)


def test_supported_airflow_version_range() -> None:
    assert is_supported_airflow_version("2.10.0") is True
    assert is_supported_airflow_version("2.11.2") is True
    assert is_supported_airflow_version("3.0.6") is False
    assert is_supported_airflow_version("3.1.3") is True
    assert is_supported_airflow_version("4.0.0") is False
    assert is_supported_airflow_version(None) is False


def test_check_metadata_db_skips_when_airflow_unavailable() -> None:
    check = check_metadata_db(airflow_available=False)
    assert check.status == "skip"


def test_check_local_config_warns_when_optional_config_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AIRFLOW_DEBUG_LOCAL_CONFIG", raising=False)
    monkeypatch.delenv("RUNBOOK_LOCAL_CONFIG", raising=False)

    check = check_local_config(require_config=False)

    assert check.status == "warn"
    assert "No local config" in check.message


def test_check_local_config_validates_runtime_payloads(tmp_path: Path) -> None:
    config_path = tmp_path / "airflow_defaults.py"
    config_path.write_text(
        "\n".join(
            [
                "CONNECTIONS = {'demo': {'conn_type': 'http', 'host': 'example.com'}}",
                "VARIABLES = {'ENV': 'local', 'FEATURES': {'x': True}}",
                "POOLS = {'default_pool': {'slots': '2', 'include_deferred': True}}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    check = check_local_config(str(config_path))

    assert check.status == "ok"
    assert "1 connection(s)" in check.message
    assert "2 variable(s)" in check.message
    assert "1 pool(s)" in check.message


def test_check_local_config_applies_extra_env_while_loading(tmp_path: Path) -> None:
    config_path = tmp_path / "airflow_defaults.py"
    config_path.write_text(
        "\n".join(
            [
                "import os",
                "VARIABLES = {'TOKEN': os.environ['DOCTOR_TOKEN']}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    check = check_local_config(str(config_path), extra_env={"DOCTOR_TOKEN": "secret"})

    assert check.status == "ok"
    assert "1 variable(s)" in check.message
    assert os.environ.get("DOCTOR_TOKEN") is None


def test_check_local_config_rejects_non_serializable_values(tmp_path: Path) -> None:
    config_path = tmp_path / "bad_config.py"
    config_path.write_text("VARIABLES = {'BAD': object()}\n", encoding="utf-8")

    check = check_local_config(str(config_path))

    assert check.status == "fail"
    assert "not JSON-serializable" in check.message


def test_check_dag_file_applies_extra_env_while_importing(tmp_path: Path) -> None:
    dag_path = tmp_path / "env_dag.py"
    dag_path.write_text(
        "\n".join(
            [
                "import os",
                "if os.environ['DOCTOR_DAG_READY'] != '1':",
                "    raise RuntimeError('wrong env')",
                "",
                "class FakeDag:",
                "    dag_id = 'demo'",
                "    task_dict = {'a': object()}",
                "",
                "dag = FakeDag()",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    check = check_dag_file(
        str(dag_path),
        airflow_version="2.11.2",
        airflow_available=True,
        extra_env={"DOCTOR_DAG_READY": "1"},
    )

    assert check.status == "ok"
    assert "dag_id: demo" in check.details
    assert os.environ.get("DOCTOR_DAG_READY") is None


def test_check_dag_file_loads_airflow_two_style_dag_object(tmp_path: Path) -> None:
    dag_path = tmp_path / "demo_dag.py"
    dag_path.write_text(
        "\n".join(
            [
                "class FakeDag:",
                "    dag_id = 'demo'",
                "    task_dict = {'a': object(), 'b': object()}",
                "",
                "dag = FakeDag()",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    check = check_dag_file(str(dag_path), airflow_version="2.11.2", airflow_available=True)

    assert check.status == "ok"
    assert check.message == "DAG imported successfully."
    assert "dag_id: demo" in check.details
    assert "task_count: 2" in check.details


def test_check_dag_file_skips_when_not_provided() -> None:
    check = check_dag_file(None)
    assert check.status == "skip"


def test_format_doctor_report_includes_verdict() -> None:
    result = DoctorResult(
        checks=[
            DoctorCheck(name="Airflow version", status="ok", message="supported"),
            DoctorCheck(name="Local config", status="fail", message="missing"),
        ]
    )

    report = format_doctor_report(result)

    assert "[OK] Airflow version: supported" in report
    assert "[FAIL] Local config: missing" in report
    assert report.endswith("Verdict: FAIL")


def test_format_doctor_json_is_machine_readable() -> None:
    result = DoctorResult(
        checks=[
            DoctorCheck(
                name="Airflow version",
                status="ok",
                message="supported",
                details=["apache-airflow 3.2.1"],
            ),
            DoctorCheck(name="Metadata DB", status="warn", message="not checked"),
        ]
    )

    payload = doctor_result_to_dict(result)
    rendered = format_doctor_json(result)

    assert payload["ok"] is True
    assert payload["exit_code"] == 0
    assert payload["checks"] == [
        {
            "details": ["apache-airflow 3.2.1"],
            "message": "supported",
            "name": "Airflow version",
            "status": "ok",
        },
        {
            "details": [],
            "message": "not checked",
            "name": "Metadata DB",
            "status": "warn",
        },
    ]
    assert json.loads(rendered) == payload


def test_doctor_parser_accepts_json_flag() -> None:
    args = build_parser().parse_args(["--json", "--env", "FOO=bar", "--env", "BAZ=qux"])
    assert args.json is True
    assert args.env == ["FOO=bar", "BAZ=qux"]


def test_run_doctor_passes_extra_env_to_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        "airflow_local_debug.doctor.check_airflow_import",
        lambda: (DoctorCheck(name="Airflow version", status="ok", message="supported"), "2.11.2", True),
    )
    monkeypatch.setattr(
        "airflow_local_debug.doctor.check_metadata_db",
        lambda *, airflow_available: DoctorCheck(name="Metadata DB", status="ok", message="reachable"),
    )

    def fake_check_local_config(config_path: str | None, **kwargs: object) -> DoctorCheck:
        seen["local"] = kwargs.get("extra_env")
        return DoctorCheck(name="Local config", status="ok", message="loaded")

    def fake_check_dag_file(dag_path: str | None, **kwargs: object) -> DoctorCheck:
        seen["dag"] = kwargs.get("extra_env")
        return DoctorCheck(name="DAG file", status="skip", message="skipped")

    monkeypatch.setattr("airflow_local_debug.doctor.check_local_config", fake_check_local_config)
    monkeypatch.setattr("airflow_local_debug.doctor.check_dag_file", fake_check_dag_file)

    result = run_doctor(extra_env={"FOO": "bar"})

    assert result.ok
    assert seen == {"local": {"FOO": "bar"}, "dag": {"FOO": "bar"}}
